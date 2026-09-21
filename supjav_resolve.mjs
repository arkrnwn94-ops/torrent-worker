#!/usr/bin/env node
// supjav_resolve.mjs — resolve data_links supjav (4 host) → URL media LANGSUNG
// (tanpa relay), ukur perkiraan ukuran tiap host, pilih yang TERBESAR.
//
// Dipakai worker codespace (/add_supjav). Beda dengan frontend sumber.ts:
//   - TIDAK membungkus urlRelay: di codespace kita yang mengunduh, IP sama
//     dengan yang meng-resolve, jadi token IP-bound tetap sah.
//   - Menambahkan probe ukuran: MP4 → content-length; HLS → varian
//     BANDWIDTH tertinggi, byte ~= bandwidth * durasi / 8.
//
// Pakai:
//   node supjav_resolve.mjs '<json data_links>' [--only ST,TV,...]
//   data_links = [{"host":"ST","link":"<hex>"}, ...]
// Output (stdout, satu baris JSON):
//   {"ok":true,"pilih":{server,tipe,url,bytes,tinggi},"kandidat":[...]}

const REFERER = 'https://supjav.com/';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const H = { Referer: REFERER, 'User-Agent': UA };

async function ambilHtml(url) {
  const r = await fetch(url, { headers: { ...H, Accept: 'text/html,*/*' }, redirect: 'follow' });
  return { html: await r.text(), url: r.url };
}

// ── pembuka p,a,c,k,e,d ──────────────────────────────────────────────────────
const DIGIT = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
function dariBase(s, a) { let n = 0; for (const c of s) { const d = DIGIT.indexOf(c); if (d < 0 || d >= a) return -1; n = n * a + d; } return n; }
function bukaPacked(js) {
  const m = js.match(/\}\('([\s\S]*)',(\d+),(\d+),'([\s\S]*?)'\.split\('\|'\)/);
  if (!m) return null;
  const a = Number(m[2]), c = Number(m[3]), kamus = m[4].split('|');
  const p = m[1].replace(/\\'/g, "'").replace(/\\\\/g, '\\');
  return p.replace(/\b\w+\b/g, (k) => { const i = dariBase(k, a); return i >= 0 && i < c && kamus[i] ? kamus[i] : k; });
}

// ── payload VOE ──────────────────────────────────────────────────────────────
const SAMPAH_VOE = ['@$', '^^', '~@', '%?', '*~', '!!', '#&'];
function rot13(s) { return s.replace(/[a-zA-Z]/g, (c) => { const b = c <= 'Z' ? 65 : 97; return String.fromCharCode(((c.charCodeAt(0) - b + 13) % 26) + b); }); }
function bacaPayloadVoe(html) {
  const m = html.match(/<script type="application\/json">\s*(\[[\s\S]*?\])\s*<\/script>/);
  if (!m) return null;
  let s;
  try { const luar = JSON.parse(m[1]); if (typeof luar[0] !== 'string') return null; s = luar[0]; } catch { return null; }
  try {
    s = rot13(s);
    for (const j of SAMPAH_VOE) s = s.split(j).join('');
    s = Buffer.from(s, 'base64').toString('latin1');
    s = Array.from(s, (c) => String.fromCharCode(c.charCodeAt(0) - 3)).join('');
    s = s.split('').reverse().join('');
    return JSON.parse(Buffer.from(s, 'base64').toString('utf8'));
  } catch { return null; }
}

// ── resolver per host → {tipe, url} URL LANGSUNG (tanpa relay) ────────────────
async function galiTv(url) {
  const { html } = await ambilHtml(url);
  const m = html.match(/var\s+urlPlay\s*=\s*'([^']+\.m3u8[^']*)'/);
  return m ? { tipe: 'hls', url: m[1] } : null;
}
async function galiFst(url) {
  const { html } = await ambilHtml(url);
  if (/restricted for this domain/i.test(html)) return null;
  const i = html.indexOf('eval(function(p,a,c,k,e,d)');
  if (i < 0) return null;
  const akhir = html.indexOf('\n', i);
  const buka = bukaPacked(html.slice(i, akhir < 0 ? undefined : akhir));
  if (!buka) return null;
  const m = buka.match(/"hls2"\s*:\s*"([^"]+\.m3u8[^"]*)"/) || buka.match(/(https?:\/\/[^"'\s]+\.m3u8[^"'\s]*)/);
  return m ? { tipe: 'hls', url: m[1] } : null;
}
async function galiSt(url) {
  const { html } = await ambilHtml(url);
  const m = html.match(/innerHTML\s*=\s*'([^']*)'\s*\+\s*\(\s*'([^']*)'\s*\)((?:\s*\.substring\(\s*\d+\s*\))+)/);
  if (!m) return null;
  let inti = m[2];
  for (const p of m[3].matchAll(/\.substring\(\s*(\d+)\s*\)/g)) inti = inti.substring(Number(p[1]));
  const gabung = m[1] + inti;
  const abs = gabung.startsWith('//') ? `https:${gabung}` : gabung;
  if (!/\/get_video\?/.test(abs)) return null;
  return { tipe: 'mp4', url: `${abs}${abs.includes('?') ? '&' : '?'}stream=1` };
}
async function galiVoe(url) {
  let { html } = await ambilHtml(url);
  for (let l = 0; l < 3 && /Redirecting/i.test(html) && html.length < 4000; l++) {
    const m = html.match(/location\.href\s*=\s*'(https?:\/\/[^']+)'/);
    if (!m) break;
    ({ html } = await ambilHtml(m[1]));
  }
  const cfg = bacaPayloadVoe(html);
  if (!cfg) return null;
  if (typeof cfg.source === 'string' && cfg.source) return { tipe: 'hls', url: cfg.source };
  if (typeof cfg.direct_access_url === 'string' && cfg.direct_access_url) return { tipe: 'mp4', url: cfg.direct_access_url };
  return null;
}

const PENGGALI = [
  { pola: /(^|\.)turbovidhls\.com$/i, gali: galiTv },
  { pola: /(^|\.)fc2stream\.tv$/i, gali: galiFst },
  { pola: /(^|\.)streamtape\.com$/i, gali: galiSt },
  { pola: /(^|\.)voe\.sx$/i, gali: galiVoe },
];

async function galiSumber(hex) {
  const terbalik = hex.split('').reverse().join('');
  const r = await fetch(`https://lk1.supremejav.com/supjav.php?c=${terbalik}`, { headers: H, redirect: 'follow' });
  const finalUrl = r.url, body = await r.text();
  if (body.trim() === '404' || finalUrl.includes('/supjav.php')) return null;
  const bersih = finalUrl.split('#')[0];
  const host = new URL(bersih).hostname;
  const p = PENGGALI.find((x) => x.pola.test(host));
  if (!p) return null;
  return await p.gali(bersih);
}

// ── probe ukuran ──────────────────────────────────────────────────────────────
async function probeMp4(url) {
  try {
    let r = await fetch(url, { method: 'HEAD', headers: H, redirect: 'follow' });
    let len = Number(r.headers.get('content-length') || 0);
    if (!len) { // sebagian host tolak HEAD → GET Range 0-0, baca Content-Range
      r = await fetch(url, { headers: { ...H, Range: 'bytes=0-0' }, redirect: 'follow' });
      const cr = r.headers.get('content-range'); // bytes 0-0/12345
      if (cr) len = Number(cr.split('/')[1] || 0);
      await r.body?.cancel();
    }
    return { bytes: len, tinggi: 0 };
  } catch { return { bytes: 0, tinggi: 0 }; }
}
async function ambilTeks(url) { const r = await fetch(url, { headers: H, redirect: 'follow' }); if (!r.ok) throw new Error('HTTP ' + r.status); return { teks: await r.text(), url: r.url }; }
async function probeHls(master) {
  // pilih varian BANDWIDTH/RESOLUTION tertinggi, estimasi byte = bw * durasi / 8
  try {
    const { teks, url } = await ambilTeks(master);
    const baris = teks.split('\n');
    let best = null;
    for (let i = 0; i < baris.length; i++) {
      if (!baris[i].startsWith('#EXT-X-STREAM-INF')) continue;
      const bw = Number((baris[i].match(/BANDWIDTH=(\d+)/) || [])[1] || 0);
      const res = (baris[i].match(/RESOLUTION=(\d+)x(\d+)/) || []);
      const tinggi = Number(res[2] || 0);
      const u = (baris[i + 1] || '').trim();
      if (!u || u.startsWith('#')) continue;
      const abs = new URL(u, url).href;
      if (!best || bw > best.bw) best = { bw, tinggi, url: abs };
    }
    if (!best) { // master ternyata media playlist langsung
      const dur = [...teks.matchAll(/#EXTINF:([\d.]+)/g)].reduce((a, m) => a + Number(m[1]), 0);
      return { bytes: 0, tinggi: 0, durasi: dur, varian: master };
    }
    const { teks: media } = await ambilTeks(best.url);
    const dur = [...media.matchAll(/#EXTINF:([\d.]+)/g)].reduce((a, m) => a + Number(m[1]), 0);
    const bytes = best.bw && dur ? Math.round((best.bw * dur) / 8) : 0;
    return { bytes, tinggi: best.tinggi, durasi: dur, varian: best.url };
  } catch { return { bytes: 0, tinggi: 0, durasi: 0, varian: master }; }
}

async function main() {
  const arg = process.argv[2];
  if (!arg) { console.log(JSON.stringify({ ok: false, pesan: 'butuh json data_links' })); process.exit(1); }
  const links = JSON.parse(arg);
  const onlyArg = (process.argv.find((a) => a.startsWith('--only=')) || '').split('=')[1];
  const only = onlyArg ? onlyArg.split(',') : null;

  const kandidat = [];
  for (const it of links) {
    const host = it.host || it.server;
    if (only && !only.includes(host)) continue;
    if (!it.link) continue;
    try {
      const s = await galiSumber(it.link);
      if (!s) { kandidat.push({ server: host, ok: false, pesan: 'resolve gagal' }); continue; }
      const probe = s.tipe === 'mp4' ? await probeMp4(s.url) : await probeHls(s.url);
      kandidat.push({ server: host, ok: true, tipe: s.tipe, url: s.url, varian: probe.varian || s.url, bytes: probe.bytes || 0, tinggi: probe.tinggi || 0, durasi: probe.durasi || 0 });
    } catch (e) { kandidat.push({ server: host, ok: false, pesan: String(e && e.message || e) }); }
  }
  const valid = kandidat.filter((k) => k.ok);
  // pilih terbesar: byte dulu; kalau byte 0 (probe gagal) jatuh ke tinggi resolusi
  valid.sort((a, b) => (b.bytes - a.bytes) || (b.tinggi - a.tinggi));
  const pilih = valid[0] || null;
  console.log(JSON.stringify({ ok: !!pilih, pilih, kandidat }));
}
main().catch((e) => { console.log(JSON.stringify({ ok: false, pesan: String(e && e.message || e) })); process.exit(1); });
