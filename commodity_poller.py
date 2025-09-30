#!/usr/bin/env python3
"""
Commodity + Derivatives poller (Upstox -> Telegram)

Features:
- Polls LTP for provided symbols (commodities/equities/indices) via Upstox v3 market-quote/ltp
- Optionally fetches Option Chain if ENABLE_OPTION_CHAIN=true and expiry provided
- Runs only during configured MARKET_START..MARKET_END (IST by default)
- Sends Telegram updates only when LTP changes (or when CHANGE_THRESHOLD_PCT exceeded) unless SEND_ALL_EVERY_POLL=true
- Uses instruments CSV (complete.csv.gz) to map trading_symbol -> instrument_key when EXPLICIT_INSTRUMENT_KEYS not provided
- Config via .env

Usage:
- Copy this file as commodity_poller.py
- Create .env from .env.example and fill values
- pip install requests
- python commodity_poller.py
"""
import os
import time
import logging
import requests
import gzip, io, csv, html
from urllib.parse import quote_plus
from datetime import datetime, time as dtime, timedelta, timezone

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# ---------- Config (from env) ----------
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Symbols: either COMMODITY_SYMBOLS as trading symbols (comma) OR EXPLICIT_INSTRUMENT_KEYS (NSE_EQ|... etc)
COMMODITY_SYMBOLS_RAW = os.getenv('COMMODITY_SYMBOLS')  # comma separated trading symbols (e.g. GOLD, SILVER, CRUDEOIL)
EXPLICIT_INSTRUMENT_KEYS = os.getenv('EXPLICIT_INSTRUMENT_KEYS')  # comma separated instrument_key values (preferred if known)

# If you want option chain data too:
ENABLE_OPTION_CHAIN = os.getenv('ENABLE_OPTION_CHAIN', 'false').lower() in ('1','true','yes')
# If ENABLE_OPTION_CHAIN true, provide expiries mapping as comma-separated: SYMBOL:YYYY-MM-DD,...
# e.g. OPTION_EXPIRIES=NSE_EQ|INE467B01029:2025-10-02,NSE_INDEX|Nifty 50:2025-10-02
OPTION_EXPIRIES_RAW = os.getenv('OPTION_EXPIRIES') or ""

POLL_INTERVAL = int(os.getenv('POLL_INTERVAL') or 60)
CHANGE_THRESHOLD_PCT = float(os.getenv('CHANGE_THRESHOLD_PCT') or 0.0)
SEND_ALL_EVERY_POLL = os.getenv('SEND_ALL_EVERY_POLL', 'false').lower() in ('1','true','yes')
STRIKE_WINDOW = int(os.getenv('STRIKE_WINDOW') or 5)

# Market hours (IST) - default broad hours which cover most commodity sessions.
# Format HH:MM (24h)
MARKET_START = os.getenv('MARKET_START') or "09:00"
MARKET_END = os.getenv('MARKET_END') or "23:30"

# Upstox endpoints
INSTRUMENTS_CSV_GZ = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
UPSTOX_LTP_URL = "https://api.upstox.com/v3/market-quote/ltp"
UPSTOX_OPTION_CHAIN_URL = "https://api.upstox.com/v3/option/chain"

# ---------- Basic validation ----------
if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Set UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID in env.")
    raise SystemExit(1)

HEADERS = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

# ---------- Helper state ----------
LAST_LTPS = {}  # instrument_key_or_symbol -> float

# ---------- Time helpers (IST) ----------
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

def parse_hhmm(s):
    h,m = [int(x) for x in s.split(":")]
    return dtime(hour=h, minute=m)

MARKET_START_T = parse_hhmm(MARKET_START)
MARKET_END_T = parse_hhmm(MARKET_END)

def is_market_open():
    t = now_ist().time()
    if MARKET_START_T <= MARKET_END_T:
        return MARKET_START_T <= t <= MARKET_END_T
    else:
        # overnight session (start > end)
        return t >= MARKET_START_T or t <= MARKET_END_T

# ---------- Instruments CSV mapping ----------
def download_instruments_rows():
    logging.info("Downloading Upstox instruments CSV ...")
    try:
        r = requests.get(INSTRUMENTS_CSV_GZ, timeout=60)
        r.raise_for_status()
    except Exception as e:
        logging.warning("Failed to download instruments CSV: %s", e)
        return []
    try:
        gz = gzip.GzipFile(fileobj=io.BytesIO(r.content))
        text = io.TextIOWrapper(gz, encoding='utf-8', errors='ignore')
        reader = csv.DictReader(text)
        rows = [row for row in reader]
        logging.info("Loaded %d instrument rows", len(rows))
        return rows
    except Exception as e:
        logging.warning("Error parsing instruments CSV: %s", e)
        return []

def build_symbol_map(rows):
    m = {}
    for row in rows:
        ts = (row.get('trading_symbol') or row.get('symbol') or "").strip()
        ik = (row.get('instrument_key') or row.get('instrumentKey') or row.get('instrument_token') or row.get('token') or "").strip()
        if ts and ik:
            m[ts.upper()] = ik
    return m

# ---------- Upstox fetching ----------
def fetch_ltps_for_keys(keys):
    if not keys:
        return None
    q = ",".join(keys)
    url = UPSTOX_LTP_URL + "?instrument_key=" + quote_plus(q)
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning("Upstox LTP fetch failed: %s", e)
        return None

def fetch_option_chain(symbol_key, expiry_date):
    if not expiry_date:
        logging.debug("No expiry specified for %s", symbol_key)
        return None
    url = UPSTOX_OPTION_CHAIN_URL + "?symbol=" + quote_plus(symbol_key) + "&expiry_date=" + quote_plus(expiry_date)
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning("Option chain fetch failed for %s %s: %s", symbol_key, expiry_date, e)
        return None

# ---------- Parsing utilities ----------
def find_ltp_in_obj(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        for key in ('ltp','last_traded_price','lastPrice','ltpPrice','lastTradedPrice'):
            if key in obj and obj[key] is not None:
                return obj[key]
        for v in obj.values():
            val = find_ltp_in_obj(v)
            if val is not None:
                return val
        return None
    elif isinstance(obj, list):
        for el in obj:
            val = find_ltp_in_obj(el)
            if val is not None:
                return val
        return None
    else:
        try:
            return float(obj)
        except Exception:
            return None

def parse_upstox_response(resp):
    parsed = []
    if resp is None:
        return parsed
    if isinstance(resp, dict) and 'data' in resp:
        data = resp['data']
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = [data]
        else:
            items = []
    elif isinstance(resp, list):
        items = resp
    elif isinstance(resp, dict):
        # mapping instrument_key -> payload
        items = []
        for k,v in resp.items():
            if isinstance(v, dict):
                ltp = find_ltp_in_obj(v)
                ts = v.get('trading_symbol') or v.get('symbol') or k
                items.append({'instrument_key': k, 'trading_symbol': ts, 'ltp': ltp})
        if items:
            return items
        items = [resp]
    else:
        items = []

    for it in items:
        if not isinstance(it, dict):
            continue
        ik = it.get('instrument_key') or it.get('instrumentKey') or None
        ts = it.get('trading_symbol') or it.get('symbol') or ik
        ltp = find_ltp_in_obj(it)
        parsed.append({'instrument_key': ik or ts, 'trading_symbol': ts, 'ltp': ltp})
    return parsed

# ---------- Formatting & send decision ----------
def safe_name_map(raw_name, name_map):
    return name_map.get(raw_name, raw_name)

def format_and_decide(parsed_list, name_map, threshold_pct=0.0):
    ts = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    header = f"📈 <b>Market Update</b> — {ts}"
    lines = [header]
    send_any = False
    for p in parsed_list:
        key = p.get('instrument_key') or p.get('trading_symbol') or 'UNKNOWN'
        raw = p.get('trading_symbol') or key
        name = safe_name_map(raw, name_map)
        ltp = p.get('ltp')
        if ltp is None:
            lines.append(f"{html.escape(str(name))}: NA")
            continue
        try:
            ltp_f = float(ltp)
            val = f"{ltp_f:,.2f}"
        except Exception:
            ltp_f = None
            val = str(ltp)
        prev = LAST_LTPS.get(key)
        if prev is None:
            should_send = True
        elif ltp_f is None:
            should_send = False
        else:
            if threshold_pct <= 0:
                should_send = (ltp_f != prev)
            else:
                if prev == 0:
                    diff_pct = 100.0 if ltp_f != 0 else 0.0
                else:
                    diff_pct = abs((ltp_f - prev)/prev)*100.0
                should_send = diff_pct >= threshold_pct
        if ltp_f is not None:
            LAST_LTPS[key] = ltp_f
        if should_send:
            send_any = True
        lines.append(f"{html.escape(str(name))}: {val}")
    return send_any or SEND_ALL_EVERY_POLL, "\n".join(lines)

# ---------- Option chain helpers (reused from earlier) ----------
def extract_strikes_from_chain(chain_json):
    if not chain_json:
        return []
    data = None
    if isinstance(chain_json, dict):
        if 'data' in chain_json:
            data = chain_json['data']
        elif 'results' in chain_json:
            data = chain_json['results']
        else:
            for v in chain_json.values():
                if isinstance(v, list):
                    data = v
                    break
    elif isinstance(chain_json, list):
        data = chain_json
    if not isinstance(data, list):
        return []
    strikes = []
    for item in data:
        try:
            strike_price = item.get('strike_price') or item.get('strike') or item.get('strikePrice')
            ce = item.get('ce') or item.get('CE') or item.get('call') or None
            pe = item.get('pe') or item.get('PE') or item.get('put') or None
            strikes.append({'strike': strike_price, 'ce': ce, 'pe': pe})
        except Exception:
            continue
    strikes_sorted = sorted([s for s in strikes if s.get('strike') is not None], key=lambda x: float(x['strike']))
    return strikes_sorted

def find_atm_strike(strikes):
    if not strikes:
        return None
    try:
        for s in strikes:
            ce = s.get('ce'); pe = s.get('pe')
            cand = None
            if ce and isinstance(ce, dict):
                cand = ce.get('underlying') or ce.get('underlying_price') or ce.get('underlyingPrice')
            if cand is None and pe and isinstance(pe, dict):
                cand = pe.get('underlying') or pe.get('underlying_price') or pe.get('underlyingPrice')
            if cand:
                try:
                    up = float(cand)
                    return min(strikes, key=lambda x: abs(float(x['strike']) - up))['strike']
                except Exception:
                    pass
        return strikes[len(strikes)//2]['strike']
    except Exception:
        return strikes[0]['strike']

def build_option_summary(name_label, strikes, atm_strike, window=5):
    lines = []
    ts = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"📊 <b>Option Chain — {html.escape(name_label)}</b> — {ts}")
    if not strikes:
        lines.append("No option chain data.")
        return "\n".join(lines)
    try:
        atm = float(atm_strike)
    except Exception:
        atm = None
    idx = None
    for i,s in enumerate(strikes):
        try:
            if float(s['strike']) == float(atm_strike):
                idx = i; break
        except Exception:
            continue
    if idx is None:
        idx = min(range(len(strikes)), key=lambda i: abs(float(strikes[i]['strike']) - (atm or float(strikes[len(strikes)//2]['strike']))))
    start = max(0, idx-window); end = min(len(strikes)-1, idx+window)
    lines.append("<code>Strike    CE(LTP / OI / IV)       |      PE(LTP / OI / IV)</code>")
    for i in range(start, end+1):
        s = strikes[i]; strike = s.get('strike'); ce = s.get('ce') or {}; pe = s.get('pe') or {}
        def short_info(side):
            if not side: return "NA"
            ltp = side.get('ltp') or side.get('last_traded_price') or side.get('lastPrice')
            oi = side.get('open_interest') or side.get('oi') or side.get('openInterest')
            iv = side.get('iv') or side.get('implied_volatility') or side.get('IV')
            l = f"{float(ltp):,.2f}" if ltp is not None else "NA"
            o = f"{int(oi):,}" if oi not in (None,"") and str(oi).isdigit() else (str(oi) if oi not in (None,"") else "NA")
            v = f"{float(iv):.2f}" if iv not in (None,"") else "NA"
            return f"{l} / {o} / {v}"
        ce_info = short_info(ce); pe_info = short_info(pe)
        atm_mark = " ⭑" if float(strike) == (atm or 0) else ""
        lines.append(f"<code>{str(int(float(strike))).rjust(6)}{atm_mark}   {ce_info.ljust(20)} | {pe_info}</code>")
    return "\n".join(lines)

# ---------- Startup: prepare instruments list ----------
def build_poll_list():
    keys = []
    name_map = {}
    # 1) explicit keys
    if EXPLICIT_INSTRUMENT_KEYS:
        for k in [x.strip() for x in EXPLICIT_INSTRUMENT_KEYS.split(",") if x.strip()]:
            keys.append(k)
    # 2) map COMMODITY_SYMBOLS via instruments CSV
    if COMMODITY_SYMBOLS_RAW:
        rows = download_instruments_rows()
        mapping = build_symbol_map(rows) if rows else {}
        for sym in [s.strip() for s in COMMODITY_SYMBOLS_RAW.split(",") if s.strip()]:
            ik = mapping.get(sym.upper())
            if ik:
                keys.append(ik)
                name_map[ik] = sym
            else:
                logging.warning("Ticker '%s' not found in instruments CSV; consider adding instrument_key to EXPLICIT_INSTRUMENT_KEYS", sym)
    # dedupe and return
    seen=set(); dedup=[]
    for k in keys:
        if k and k not in seen:
            dedup.append(k); seen.add(k)
    logging.info("Prepared %d instrument keys to poll.", len(dedup))
    return dedup, name_map

# parse option expiries mapping
def parse_option_expiries(raw):
    d={}
    for pair in [p.strip() for p in raw.split(",") if p.strip()]:
        if ":" in pair:
            k,v = pair.split(":",1); d[k.strip()] = v.strip()
    return d

OPTION_EXPIRIES = parse_option_expiries(OPTION_EXPIRIES_RAW)

# ---------- Main loop ----------
def main():
    poll_keys, name_map = build_poll_list()
    if not poll_keys:
        logging.error("No instrument keys configured to poll; fill COMMODITY_SYMBOLS or EXPLICIT_INSTRUMENT_KEYS.")
        return
    logging.info("Starting commodity poller. Poll interval %ds. Market hours %s-%s (IST)", POLL_INTERVAL, MARKET_START, MARKET_END)
    while True:
        try:
            if not is_market_open():
                logging.info("Market closed (per configured hours). Sleeping 60s.")
                time.sleep(60); continue
            # fetch in chunks
            CHUNK=50
            all_parsed=[]
            for i in range(0, len(poll_keys), CHUNK):
                chunk = poll_keys[i:i+CHUNK]
                resp = fetch_ltps_for_keys(chunk)
                parsed = parse_upstox_response(resp)
                if parsed:
                    # ensure instrument_key present
                    for p in parsed:
                        if not p.get('instrument_key'):
                            p['instrument_key'] = p.get('trading_symbol') or None
                    all_parsed.extend(parsed)
            if not all_parsed:
                logging.warning("No parsed LTPs this cycle.")
            else:
                send, text = format_and_decide(all_parsed, name_map, threshold_pct=CHANGE_THRESHOLD_PCT)
                if send:
                    send_telegram(text)
                    logging.info("Sent LTP update for %d items.", len(all_parsed))
                else:
                    logging.info("No significant LTP change; skipped Telegram.")
            # Option chain (if enabled)
            if ENABLE_OPTION_CHAIN:
                for key in poll_keys:
                    expiry = OPTION_EXPIRIES.get(key)
                    if expiry:
                        chain = fetch_option_chain(key, expiry)
                        strikes = extract_strikes_from_chain(chain)
                        atm = find_atm_strike(strikes) if strikes else None
                        if strikes:
                            summary = build_option_summary(key, strikes, atm, window=STRIKE_WINDOW)
                            send_telegram(summary)
                            logging.info("Sent option chain for %s (ATM %s)", key, atm)
                        else:
                            logging.info("No option chain for %s", key)
                    else:
                        logging.debug("No expiry provided for key %s - skipping option chain.", key)
        except Exception as e:
            logging.exception("Unhandled error in main loop: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
