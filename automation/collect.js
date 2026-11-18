#!/usr/bin/env node
'use strict';
/*
 * CMO War Room — D-minus 사전추적 수집기 (fail-soft / self-heal)
 * ------------------------------------------------------------------
 * 목적: 개봉 전 작품(예: "호프")의 D-day 카운트다운 구간에서 매일
 *       KOBIS(사전예매)·버즈(WOM) 두 소스를 수집해 시계열 "행"을 남긴다.
 *
 * 이 파일이 해결하는 것 — 2026-07-11 경보의 실패 모드 정면 대응:
 *   1) "KOBIS·버즈 모두 무응답"  → 두 소스를 완전 독립 수집(한쪽 실패가
 *                                  다른 쪽/행 전체를 죽이지 못함).
 *   2) "무응답(6회 시도)"         → AbortController 타임아웃 + 지수 백오프 +
 *                                  지터. 실패를 timeout/http/network/empty로
 *                                  분류해 알림에 원인을 남김(예전엔 알 수 없었음).
 *   3) "행 미기록"                → 성공한 값만이라도 status=partial|failed 로
 *                                  항상 upsert. 시계열에 구멍(gap)이 안 생김.
 *   4) "내일 회복 기대"           → 매 실행 시 누락/실패 행을 자동 감지(--heal 로
 *                                  즉시 백필). 회복을 사람이 기다릴 필요 없음.
 *
 * 무의존성(zero-dep). Node 18+ (global fetch/AbortController). 여기선 v22 확인됨.
 *
 * 사용:
 *   node automation/collect.js --run                 # 오늘(KST) 타깃 수집
 *   node automation/collect.js --date 2026-07-11     # 특정 날짜 수집(백필)
 *   node automation/collect.js --backfill 2026-07-11 # 위와 동일(가독성용 별칭)
 *   node automation/collect.js --heal                # 누락/실패 행 전부 재수집
 *   node automation/collect.js --run --heal          # 오늘 수집 + 구멍 메우기
 *   node automation/collect.js --dry                 # 텔레그램 전송 없이 확인
 *
 * 설정: automation/.env.example 참고. 필요한 환경변수를 셸/CI 시크릿으로 주입.
 *   KOBIS_KEY, KOBIS_BASE(선택), BUZZ_URL, BUZZ_KEY(선택),
 *   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, RETRIES(선택), TIMEOUT_MS(선택),
 *   TRACK_WINDOW_DAYS(선택, 기본 14)
 */

const fs = require('fs');
const path = require('path');

const HERE = __dirname;
const OUT_DIR = path.join(HERE, 'out');
const SUBJECTS_PATH = path.join(HERE, 'subjects.json');

const RETRIES = int(process.env.RETRIES, 6);
const TIMEOUT_MS = int(process.env.TIMEOUT_MS, 15000);
const TRACK_WINDOW_DAYS = int(process.env.TRACK_WINDOW_DAYS, 14);
const MAX_BACKOFF_MS = 30000;

// ---------- 작은 유틸 ----------
function int(v, d) { const n = parseInt(v, 10); return Number.isFinite(n) ? n : d; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function slug(s) { return String(s).trim().replace(/\s+/g, '_').replace(/[^\w가-힣_-]/g, ''); }
function log(...a) { console.log('[collect]', ...a); }
function warn(...a) { console.warn('[collect]', ...a); }

// 날짜: YYYY-MM-DD 를 UTC epoch(ms)로. TZ 흔들림 방지.
function parseDate(s) { const [y, m, d] = s.split('-').map(Number); return Date.UTC(y, m - 1, d); }
function fmtDate(ms) {
  const dt = new Date(ms);
  const p = n => String(n).padStart(2, '0');
  return `${dt.getUTCFullYear()}-${p(dt.getUTCMonth() + 1)}-${p(dt.getUTCDate())}`;
}
function daysBetween(a, b) { return Math.round((parseDate(a) - parseDate(b)) / 86400000); }
function dMinusLabel(openDt, targetDt) {
  const diff = daysBetween(openDt, targetDt);
  return diff > 0 ? `D-${diff}` : diff === 0 ? 'D-DAY' : `D+${-diff}`;
}
// "오늘"은 KST 기준(개봉일 경계가 한국 시간).
function todayKST() { return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10); }

// ---------- 재시도 fetch (실패 원인 분류가 핵심) ----------
// 반환: 성공 시 파싱된 값. 실패 시 { kind, message, status?, attempts } 를 던짐.
async function fetchJSON(url, { headers = {}, timeoutMs = TIMEOUT_MS, retries = RETRIES, label = '' } = {}) {
  let last;
  for (let attempt = 1; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(new Error('timeout')), timeoutMs);
    try {
      const res = await fetch(url, { signal: ctrl.signal, headers, redirect: 'follow' });
      clearTimeout(timer);
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw tag(new Error(`HTTP ${res.status}`), { kind: 'http', status: res.status, body: body.slice(0, 200) });
      }
      const text = await res.text();
      if (!text.trim()) throw tag(new Error('empty body'), { kind: 'empty' });
      try { return JSON.parse(text); }
      catch { throw tag(new Error('non-JSON body'), { kind: 'empty', body: text.slice(0, 200) }); }
    } catch (e) {
      clearTimeout(timer);
      const kind = e.kind
        || (e.name === 'AbortError' || /timeout/i.test(e.message || '') ? 'timeout' : 'network');
      last = tag(e, { kind });
      const willRetry = attempt < retries;
      warn(`${label} 시도 ${attempt}/${retries} 실패 [${kind}] ${e.message}${willRetry ? ' — 재시도' : ''}`);
      if (willRetry) {
        const backoff = Math.min(1000 * 2 ** (attempt - 1), MAX_BACKOFF_MS);
        const jitter = Math.floor(Math.random() * 500);
        await sleep(backoff + jitter);
      }
    }
  }
  throw tag(last || new Error('unknown'), { attempts: retries });
}
function tag(err, props) { return Object.assign(err, props); }

// ---------- 소스별 수집 (완전 독립) ----------
// 각 함수는 절대 throw 하지 않는다. { ok, data } 또는 { ok:false, kind, error, attempts }.
async function collectKobis(targetDt) {
  const key = process.env.KOBIS_KEY;
  if (!key) return { ok: false, kind: 'config', error: 'KOBIS_KEY 미설정' };
  const base = process.env.KOBIS_BASE
    || 'https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json';
  const url = `${base}?key=${encodeURIComponent(key)}&targetDt=${targetDt.replace(/-/g, '')}`;
  try {
    const j = await fetchJSON(url, { label: 'KOBIS' });
    // openAPI가 에러 객체를 200으로 돌려주는 경우까지 방어.
    if (j && j.faultInfo) throw tag(new Error(j.faultInfo.message || 'KOBIS faultInfo'), { kind: 'http' });
    const list = (j && j.boxOfficeResult && j.boxOfficeResult.dailyBoxOfficeList) || [];
    return { ok: true, data: { dailyBoxOfficeList: list } };
  } catch (e) {
    return { ok: false, kind: e.kind || 'network', error: e.message, attempts: e.attempts || RETRIES };
  }
}

async function collectBuzz(subject, targetDt) {
  const tmpl = process.env.BUZZ_URL;
  if (!tmpl) return { ok: false, kind: 'config', error: 'BUZZ_URL 미설정' };
  const url = tmpl
    .replace('{title}', encodeURIComponent(subject.title))
    .replace('{date}', targetDt);
  const headers = process.env.BUZZ_KEY ? { Authorization: `Bearer ${process.env.BUZZ_KEY}` } : {};
  try {
    const j = await fetchJSON(url, { label: 'BUZZ', headers });
    return { ok: true, data: j };
  } catch (e) {
    return { ok: false, kind: e.kind || 'network', error: e.message, attempts: e.attempts || RETRIES };
  }
}

// KOBIS 응답에서 이 작품의 사전예매/박스오피스 지표만 추린다(공백이면 null).
function summarizeKobis(subject, kobis) {
  const list = (kobis.data && kobis.data.dailyBoxOfficeList) || [];
  const hit = list.find(x => x.movieNm && x.movieNm.includes(subject.title));
  if (!hit) return { matched: false }; // 개봉 전이면 일일 박스오피스에 없음 — 정상적 공백
  return {
    matched: true,
    rank: num(hit.rank),
    audiAcc: num(hit.audiAcc),
    showCnt: num(hit.showCnt),
    scrnCnt: num(hit.scrnCnt),
    salesAcc: num(hit.salesAcc),
  };
}
function num(v) { const n = Number(String(v).replace(/,/g, '')); return Number.isFinite(n) ? n : null; }

// ---------- 행(row) 저장소 (JSON 시계열, 날짜별 upsert) ----------
function rowsPath(subject) { return path.join(OUT_DIR, `rows_${slug(subject.title)}.json`); }
function loadRows(subject) {
  try { return JSON.parse(fs.readFileSync(rowsPath(subject), 'utf8')); }
  catch { return { title: subject.title, openDt: subject.openDt, rows: [] }; }
}
function saveRows(subject, store) {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  store.title = subject.title; store.openDt = subject.openDt;
  store.rows.sort((a, b) => a.date < b.date ? -1 : 1);
  fs.writeFileSync(rowsPath(subject), JSON.stringify(store, null, 2));
}
function upsertRow(store, row) {
  const i = store.rows.findIndex(r => r.date === row.date);
  if (i >= 0) store.rows[i] = row; else store.rows.push(row);
}

// 추적 창(개봉 D-window ~ 개봉일) 안에서 오늘까지 중 누락/실패한 날짜를 반환.
function findGaps(subject, store, upTo) {
  const open = parseDate(subject.openDt);
  const start = open - TRACK_WINDOW_DAYS * 86400000;
  const end = Math.min(parseDate(upTo), open);
  const have = new Map(store.rows.map(r => [r.date, r]));
  const gaps = [];
  for (let ms = start; ms <= end; ms += 86400000) {
    const d = fmtDate(ms);
    const row = have.get(d);
    if (!row || row.status === 'failed') gaps.push(d);
  }
  return gaps;
}

// ---------- 텔레그램 알림 ----------
async function alert(text, dry) {
  const token = process.env.TELEGRAM_BOT_TOKEN, chat = process.env.TELEGRAM_CHAT_ID;
  log('ALERT:', text.replace(/\n/g, ' '));
  if (dry) return;
  if (!token || !chat) { warn('텔레그램 미설정 — 알림 콘솔로만 출력'); return; }
  try {
    const res = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, text, disable_web_page_preview: true }),
    });
    if (!res.ok) warn('텔레그램 전송 실패 HTTP', res.status);
  } catch (e) { warn('텔레그램 전송 예외:', e.message); }
}

// ---------- 한 작품 · 한 날짜 수집 ----------
async function collectOne(subject, targetDt, opts) {
  const dLabel = dMinusLabel(subject.openDt, targetDt);
  log(`▶ ${subject.title} ${dLabel} (${targetDt}) 수집`);

  // 두 소스를 병렬·독립으로. 한쪽이 6회 다 죽어도 다른 쪽은 살아남는다.
  const [kobis, buzz] = await Promise.all([
    collectKobis(targetDt),
    collectBuzz(subject, targetDt),
  ]);

  const okCount = [kobis.ok, buzz.ok].filter(Boolean).length;
  const status = okCount === 2 ? 'ok' : okCount === 1 ? 'partial' : 'failed';
  const errors = [];
  if (!kobis.ok) errors.push({ source: 'KOBIS', kind: kobis.kind, message: kobis.error, attempts: kobis.attempts });
  if (!buzz.ok) errors.push({ source: 'BUZZ', kind: buzz.kind, message: buzz.error, attempts: buzz.attempts });

  const row = {
    date: targetDt,
    dMinus: dLabel,
    status,                                   // ok | partial | failed — 행은 항상 남는다
    kobis: kobis.ok ? summarizeKobis(subject, kobis) : null,
    buzz: buzz.ok ? buzz.data : null,
    errors,
    collectedAt: new Date().toISOString(),
  };

  const store = loadRows(subject);
  upsertRow(store, row);
  saveRows(subject, store);
  log(`  status=${status} · 저장: ${path.relative(process.cwd(), rowsPath(subject))}`);

  // 알림은 문제 있을 때만. 원인(kind)과 시도 횟수를 반드시 실어 예전 "행 미기록" 애매함을 없앤다.
  if (status !== 'ok') {
    const icon = status === 'failed' ? '⛔' : '⚠️';
    const failed = errors.map(e => `${e.source}(${e.kind}${e.attempts ? `·${e.attempts}회` : ''})`).join(', ');
    const note = status === 'failed'
      ? '두 소스 모두 실패 — failed 행 기록됨(구멍 없음). 다음 실행에서 자동 백필 시도.'
      : '부분 성공 — partial 행 기록됨. 실패 소스만 다음 실행에서 자동 백필.';
    await alert(
      `${icon} ${subject.title} ${dLabel} (${targetDt}) 수집 ${status}\n실패 소스: ${failed}\n${note}`,
      opts.dry
    );
  }
  return row;
}

// ---------- 메인 ----------
async function main() {
  const args = process.argv.slice(2);
  const has = f => args.includes(f);
  const opt = k => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : null; };
  const explicitDate = args.find(a => /^\d{4}-\d{2}-\d{2}$/.test(a))
    || opt('--date') || opt('--backfill');
  const opts = { dry: has('--dry') };

  if (!has('--run') && !has('--heal') && !explicitDate) {
    console.log('사용법: node automation/collect.js [--run] [--date YYYY-MM-DD] [--backfill YYYY-MM-DD] [--heal] [--dry]');
    process.exit(2);
  }

  let subjects;
  try { subjects = JSON.parse(fs.readFileSync(SUBJECTS_PATH, 'utf8')); }
  catch (e) { console.error('subjects.json 로드 실패:', e.message); process.exit(1); }

  const today = todayKST();
  const targetDate = explicitDate || today;
  let hadFailure = false;

  for (const subject of subjects) {
    // 1) 지정 날짜(또는 오늘) 수집 — 단, --heal 만 준 경우는 건너뛰고 gap만 처리.
    if (has('--run') || explicitDate) {
      const row = await collectOne(subject, targetDate, opts);
      if (row.status === 'failed') hadFailure = true;
    }
    // 2) 자가복구: 누락/실패 행 백필.
    if (has('--heal')) {
      const store = loadRows(subject);
      const gaps = findGaps(subject, store, today).filter(d => d !== targetDate);
      if (gaps.length) {
        log(`↻ ${subject.title} 백필 대상 ${gaps.length}건: ${gaps.join(', ')}`);
        for (const d of gaps) {
          const row = await collectOne(subject, d, opts);
          if (row.status === 'failed') hadFailure = true;
        }
      } else {
        log(`✓ ${subject.title} 누락 행 없음`);
      }
    } else {
      // --heal 없이도 구멍 현황은 보고(사람이 인지하도록).
      const store = loadRows(subject);
      const gaps = findGaps(subject, store, today);
      if (gaps.length) warn(`${subject.title} 미해결 행 ${gaps.length}건(--heal 로 백필): ${gaps.join(', ')}`);
    }
  }

  // 종료 코드: failed 행이 남아 있으면 CI가 인지하도록 비0(부분성공은 0).
  process.exit(hadFailure ? 1 : 0);
}

main().catch(e => { console.error('치명적 예외:', e); process.exit(1); });
