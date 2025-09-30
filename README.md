# Commodity Poller (Upstox -> Telegram)

## What it does
- Polls Upstox LTP for configured commodities (or equities/indices).
- Sends Telegram updates every `POLL_INTERVAL` seconds.
- Optional: fetch and send Option Chain (if supported by Upstox and expiry provided).
- Runs only during configured market hours (IST).

## Files
- `commodity_poller.py`  (main poller script)
- `requirements.txt`
- `.env.example`
- `Procfile` (for Railway/Heroku)
- `start.sh`

## Setup
1. Copy `.env.example` to `.env` and fill:
   - `UPSTOX_ACCESS_TOKEN`
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - commodity `COMMODITY_SYMBOLS` or `EXPLICIT_INSTRUMENT_KEYS`.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
