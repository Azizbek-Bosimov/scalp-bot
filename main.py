"""
GOLD (XAUUSDT) 1/5/15/30m SMC + ICT Scalping Bot - Railway uchun tayyor
Yangilanishlar:
  1. 1, 5, 15 va 30 daqiqalik taymfreymlar to'liq sinxronlashtirildi.
  2. Faqat barcha 4 ta taymfreym bir xil trendni ko'rsatsagina savdoga kiradi.
"""

import datetime
import html
import json
import logging
import math
import os
import threading
import time
import traceback
from collections import deque

import requests
from flask import Flask

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold_smc_bot")

SYMBOL = "XAUUSDT"
BTC_SYMBOL = "BTCUSDT"
EUR_SYMBOL = "EURUSDT"
LIMIT = 200

# Scalping uchun maxsus sozlamalar
RR1, RR2 = 1.2, 2.0               
ATR_MIN_RISK_MULT = 0.1           
SL_BUFFER_ATR_MULT = 0.05         
SWING_LEFT, SWING_RIGHT = 3, 3
CHECK_INTERVAL_SEC = 1  
ZONE_MAX_DISTANCE_PCT = 0.4       
MAX_RISK_PCT = 0.02               
TRADE_STALE_WARNING_HOURS = 6     
LOG_FILE = os.path.join(os.path.dirname(__file__), "trade_log.json")
STATUS_FILE = os.path.join(os.path.dirname(__file__), "status.json")

# ==================== POSITION SIZING ====================
ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "account.json")
DEFAULT_RISK_PER_TRADE_PCT = 1.0   
MAX_RISK_PER_TRADE_PCT = 5.0       
XAUUSD_LOT_UNITS = 100              
MIN_LOT_STEP = 0.01                 

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

KILLZONES_ENABLED = True
KILLZONES = [
    {"start": datetime.time(7, 0), "end": datetime.time(11, 0)},   # London
    {"start": datetime.time(12, 0), "end": datetime.time(16, 0)}   # New York
]

def _get_secret(name):
    value = os.environ.get(name)
    if value:
        return value
    try:
        import config
        return getattr(config, name)
    except (ImportError, AttributeError):
        raise RuntimeError(
            f"'{name}' topilmadi. Railway Environment Variables bo'limiga qo'shing."
        )

BOT_TOKEN = _get_secret("BOT_TOKEN")
CHAT_ID = _get_secret("CHAT_ID")
ADMIN_CHAT_ID = str(CHAT_ID)  
SUBSCRIBERS_FILE = os.path.join(os.path.dirname(__file__), "subscribers.json")
PRICE_OFFSET = -5.0

def load_subscribers():
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, encoding='utf-8') as file:
                return set(json.load(file))
        except Exception as error:
            logger.error(f"Obunachilarni o'qishda xato: {error}")
    return {ADMIN_CHAT_ID}

def save_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, 'w', encoding='utf-8') as file:
            json.dump(list(SUBSCRIBERS), file)
    except Exception as error:
        logger.error(f"Obunachilarni saqlashda xato: {error}")

SUBSCRIBERS = load_subscribers()

def load_account():
    if os.path.exists(ACCOUNT_FILE):
        try:
            with open(ACCOUNT_FILE, encoding='utf-8') as file:
                data = json.load(file)
                return {
                    "balance": data.get("balance"),
                    "risk_pct": data.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT),
                }
        except Exception as error:
            logger.error(f"Hisob ma'lumotini o'qishda xato: {error}")
    return {"balance": None, "risk_pct": DEFAULT_RISK_PER_TRADE_PCT}

def save_account():
    _atomic_write(ACCOUNT_FILE, ACCOUNT)

ACCOUNT = load_account()

O, H, L, C = 1, 2, 3, 4

state_lock = threading.RLock()

current_trade = None
warned_flip = False
paused = False
last_impulse_ts = None
IMPULSE_LOOKBACK = 20
IMPULSE_THRESHOLD = 2.5

post_trade = None
POST_TRADE_CHECKS = 20
POST_TRADE_MIN_CONTINUATION = 20

PRICE_HISTORY_MAXLEN = 300   
PRICE_HISTORY = deque(maxlen=PRICE_HISTORY_MAXLEN)
last_impulse_info = None   

last_status = {
    "price": None,
    "bias1": None,
    "bias5": None,
    "bias15": None,
    "bias30": None,
    "bias1h": None,
    "bias4h": None,
    "bias1d": None,
    "checked_at": None,
}

RSI_PERIOD = 14
ATR_PERIOD = 14
VWAP_LOOKBACK = 48
MIN_CONFIRMATIONS = 3

HTF_FILTER_ENABLED = True
STRONG_HTF_FILTER_ENABLED = True
DAILY_HTF_FILTER_ENABLED = True   
ZONE_MAX_AGE_BARS = 40

NEWS_FILTER_ENABLED = True
NEWS_BLOCK_MINUTES_BEFORE = 30
NEWS_BLOCK_MINUTES_AFTER = 30
NEWS_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
NEWS_CACHE_TTL_SEC = 3600
_news_cache = {"events": None, "fetched_at": None}


def closed_only(candles):
    if len(candles) > 1:
        return candles[:-1]
    return candles

def in_killzone():
    if not KILLZONES_ENABLED:
        return True
    now_utc = datetime.datetime.now(datetime.timezone.utc).time()
    for kz in KILLZONES:
        if kz["start"] <= now_utc <= kz["end"]:
            return True
    return False

def detect_impulse(candles, lookback=IMPULSE_LOOKBACK, threshold=IMPULSE_THRESHOLD):
    if len(candles) < lookback + 1:
        return None
    ranges = [c[H] - c[L] for c in candles]
    avg_range = sum(ranges[-lookback - 1 : -1]) / lookback
    last = candles[-1]
    last_range = last[H] - last[L]
    if avg_range == 0:
        return None
    ratio = last_range / avg_range
    if ratio >= threshold:
        direction = "yuqoriga" if last[C] > last[O] else "pastga"
        return {
            "ts": last[0],
            "ratio": round(ratio, 1),
            "direction": direction,
            "range": round(last_range, 2),
            "price": round(last[C], 2),
        }
    return None

def load_log():
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, encoding='utf-8') as file:
                return json.load(file)
        except json.JSONDecodeError:
            return []
    return []

def _atomic_write(path, data):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

def save_log(log):
    _atomic_write(LOG_FILE, log)

def save_status(price, bias1, bias5, bias15, bias30, bias1h=None, bias4h=None, bias1d=None):
    with state_lock:
        trade_snapshot = current_trade
    _atomic_write(
        STATUS_FILE,
        {
            "currentTrade": trade_snapshot,
            "lastPrice": price,
            "lastCheckedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "bias1m": bias1,
            "bias5m": bias5,
            "bias15m": bias15,
            "bias30m": bias30,
            "bias1h": bias1h,
            "bias4h": bias4h,
            "bias1d": bias1d,
        },
    )

def win_rate_text(log):
    if not log:
        return "Hali statistika yo'q"
    wins = sum(1 for trade in log if trade["result"] in ("TP1", "TP2"))
    breakevens = sum(1 for trade in log if "BE" in trade["result"] or "Trailing" in trade["result"])
    total = len(log)
    return f"{wins / total * 100:.1f}% g'alaba, {breakevens} ta BE/Trailing, jami {total} ta savdo"

def fetch_ohlcv(timeframe, symbol=SYMBOL):
    interval_map = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "1h": "60", "4h": "240", "1d": "D"}
    interval = interval_map.get(timeframe, timeframe)
    response = requests.get(
        "https://api.bybit.com/v5/market/kline",
        params={"category": "linear", "symbol": symbol, "interval": interval, "limit": LIMIT},
        headers=HEADERS,
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    rows = data.get("result", {}).get("list", [])
    if not rows:
        raise RuntimeError(f"Bybit'dan candle ma'lumoti kelmadi")
    rows.sort(key=lambda row: int(row[0]))
    return [[int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])] for row in rows]

def find_swings(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = [], []
    for index in range(left, len(candles) - right):
        window = candles[index - left : index + right + 1]
        if candles[index][H] == max(candle[H] for candle in window):
            highs.append((index, candles[index][H]))
        if candles[index][L] == min(candle[L] for candle in window):
            lows.append((index, candles[index][L]))
    return highs, lows

def confirmed_structure_bias(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = find_swings(candles, left, right)
    if not highs or not lows:
        return None
    last_high_index, last_high_price = highs[-1]
    last_low_index, last_low_price = lows[-1]
    start_index = max(last_high_index, last_low_index) + 1
    bias = None
    for index in range(start_index, len(candles)):
        close = candles[index][C]
        if close > last_high_price:
            bias = "bullish"
            last_low_price = min(last_low_price, candles[index][L])
        elif close < last_low_price:
            bias = "bearish"
            last_high_price = max(last_high_price, candles[index][H])
    return bias

def detect_liquidity_sweep(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = find_swings(candles, left, right)
    if not highs or not lows:
        return None
    last_high_price = highs[-1][1]
    last_low_price = lows[-1][1]
    last_candle = candles[-1]
    if last_candle[H] > last_high_price and last_candle[C] < last_high_price:
        return "bearish_sweep"
    if last_candle[L] < last_low_price and last_candle[C] > last_low_price:
        return "bullish_sweep"
    return None

def detect_fvg(candles):
    fvgs = []
    for index in range(2, len(candles)):
        first, third = candles[index - 2], candles[index]
        if third[L] > first[H]:
            fvgs.append({"type": "bullish", "kind": "fvg", "top": third[L], "bottom": first[H], "index": index})
        elif third[H] < first[L]:
            fvgs.append({"type": "bearish", "kind": "fvg", "top": first[L], "bottom": third[H], "index": index})
    return fvgs

def detect_order_blocks(candles):
    bodies = [abs(candle[C] - candle[O]) for candle in candles]
    volumes = [candle[5] if len(candle) > 5 else 0 for candle in candles]
    order_blocks = []
    for index in range(10, len(candles) - 1):
        average_body = sum(bodies[index - 10 : index]) / 10
        average_volume = sum(volumes[index - 10 : index]) / 10 
        if average_body == 0 or average_volume == 0:
            continue
        impulsive_body = bodies[index + 1] > average_body * 1.5
        impulsive_volume = volumes[index + 1] > average_volume * 2.0 
        current, following = candles[index], candles[index + 1]
        bullish_ob = current[C] < current[O] and following[C] > following[O]
        bearish_ob = current[C] > current[O] and following[C] < following[O]
        if impulsive_body and impulsive_volume and bullish_ob:
            order_blocks.append({"type": "bullish", "kind": "ob", "top": current[O], "bottom": current[L], "index": index})
        if impulsive_body and impulsive_volume and bearish_ob:
            order_blocks.append({"type": "bearish", "kind": "ob", "top": current[H], "bottom": current[O], "index": index})
    return order_blocks

def calculate_rsi(candles, period=RSI_PERIOD):
    closes = [candle[C] for candle in candles]
    count = len(closes)
    if count < period + 1:
        return [None] * count
    rsis = [None] * period
    gains, losses = [], []
    for index in range(1, period + 1):
        difference = closes[index] - closes[index - 1]
        gains.append(max(difference, 0))
        losses.append(max(-difference, 0))
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period
    rsis.append(100 if average_loss == 0 else 100 - (100 / (1 + average_gain / average_loss)))
    for index in range(period + 1, count):
        difference = closes[index] - closes[index - 1]
        gain = max(difference, 0)
        loss = max(-difference, 0)
        average_gain = (average_gain * (period - 1) + gain) / period
        average_loss = (average_loss * (period - 1) + loss) / period
        rsis.append(100 if average_loss == 0 else 100 - (100 / (1 + average_gain / average_loss)))
    return rsis

def calculate_atr(candles, period=ATR_PERIOD):
    if len(candles) < period + 1:
        return None
    true_ranges = []
    for index in range(1, len(candles)):
        high, low, previous_close = candles[index][H], candles[index][L], candles[index - 1][C]
        true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return sum(true_ranges[-period:]) / period

def calculate_vwap(candles, lookback=VWAP_LOOKBACK):
    cumulative_price_volume = 0
    cumulative_volume = 0
    for candle in candles[-lookback:]:
        typical_price = (candle[H] + candle[L] + candle[C]) / 3
        volume = candle[5] if len(candle) > 5 else 0
        cumulative_price_volume += typical_price * volume
        cumulative_volume += volume
    if cumulative_volume == 0:
        return None
    return cumulative_price_volume / cumulative_volume

def detect_rsi_divergence(candles, rsis, lookback=20):
    if len(candles) < lookback:
        return False, False
    window = candles[-lookback:]
    offset = len(candles) - lookback
    highs, lows = find_swings(window)
    bullish_divergence, bearish_divergence = False, False
    if len(lows) >= 2:
        first_index, first_price = lows[-2]
        second_index, second_price = lows[-1]
        first_rsi, second_rsi = rsis[offset + first_index], rsis[offset + second_index]
        if first_rsi is not None and second_rsi is not None and second_price < first_price and second_rsi > first_rsi:
            bullish_divergence = True
    if len(highs) >= 2:
        first_index, first_price = highs[-2]
        second_index, second_price = highs[-1]
        first_rsi, second_rsi = rsis[offset + first_index], rsis[offset + second_index]
        if first_rsi is not None and second_rsi is not None and second_price > first_price and second_rsi < first_rsi:
            bearish_divergence = True
    return bullish_divergence, bearish_divergence

def confirmation_check(
    bias, price, vwap, bullish_divergence, bearish_divergence, btc_bias=None, eur_bias=None, zone_confluence=False, sweep=None
):
    confirmations = []
    if bias == "bullish":
        if bullish_divergence: confirmations.append("RSI divergence (bullish)")
        if vwap is not None and price > vwap: confirmations.append("Narx VWAP ustida")
        if btc_bias is not None and btc_bias != "bullish": confirmations.append("SMT: BTC mos kelmadi")
        if eur_bias is not None and eur_bias != "bullish": confirmations.append("SMT: EURUSD mos kelmadi")
        if sweep == "bullish_sweep": confirmations.append("Likvidlik yig'ildi (Bullish Sweep)")
    else:
        if bearish_divergence: confirmations.append("RSI divergence (bearish)")
        if vwap is not None and price < vwap: confirmations.append("Narx VWAP ostida")
        if btc_bias is not None and btc_bias != "bearish": confirmations.append("SMT: BTC mos kelmadi")
        if eur_bias is not None and eur_bias != "bearish": confirmations.append("SMT: EURUSD mos kelmadi")
        if sweep == "bearish_sweep": confirmations.append("Likvidlik yig'ildi (Bearish Sweep)")

    if zone_confluence: confirmations.append("Zone confluence (FVG + OB bir joyda)")
    return confirmations

def _zone_is_fresh(zone, candles, max_age_bars=ZONE_MAX_AGE_BARS):
    formation_index = zone["index"]
    last_index = len(candles) - 1
    if last_index - formation_index > max_age_bars:
        return False
    for index in range(formation_index + 1, len(candles)):
        close = candles[index][C]
        if zone["type"] == "bullish" and close < zone["bottom"]: return False
        if zone["type"] == "bearish" and close > zone["top"]: return False
    return True

def build_trade(candles, bias, price, atr=None):
    all_zones = [
        zone for zone in detect_fvg(candles) + detect_order_blocks(candles)
        if zone["type"] == bias and _zone_is_fresh(zone, candles)
    ]
    if not all_zones: return None
    all_zones.sort(key=lambda zone: abs(price - (zone["top"] + zone["bottom"]) / 2))
    zone = all_zones[0]

    lower, upper = min(zone["bottom"], zone["top"]), max(zone["bottom"], zone["top"])
    tolerance = price * (ZONE_MAX_DISTANCE_PCT / 100)
    if not (lower - tolerance <= price <= upper + tolerance): return None

    confluence = any(
        other is not zone and other["kind"] != zone["kind"] and
        other["bottom"] <= zone["top"] and other["top"] >= zone["bottom"]
        for other in all_zones
    )

    sl_buffer = atr * SL_BUFFER_ATR_MULT if atr else price * 0.001
    entry = price

    if bias == "bullish":
        stop_loss = zone["bottom"] - sl_buffer
        risk = entry - stop_loss
        if risk <= 0 or risk > entry * MAX_RISK_PCT: return None
        take_profit_1, take_profit_2 = entry + risk * RR1, entry + risk * RR2
        side = "LONG"
    else:
        stop_loss = zone["top"] + sl_buffer
        risk = stop_loss - entry
        if risk <= 0 or risk > entry * MAX_RISK_PCT: return None
        take_profit_1, take_profit_2 = entry - risk * RR1, entry - risk * RR2
        side = "SHORT"

    return {
        "signal": side, "bias": bias, "entry": round(entry, 2),
        "sl": round(stop_loss, 2), "tp1": round(take_profit_1, 2),
        "tp2": round(take_profit_2, 2), "confluence": confluence,
        "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

def calculate_position_size(entry, sl, balance, risk_pct):
    if not balance or balance <= 0: return None
    price_risk = abs(entry - sl)
    if price_risk <= 0: return None
    risk_usd_target = balance * (risk_pct / 100)
    raw_lot = risk_usd_target / (price_risk * XAUUSD_LOT_UNITS)
    lot = math.floor(raw_lot / MIN_LOT_STEP) * MIN_LOT_STEP
    lot = round(lot, 2)
    undersized = lot < MIN_LOT_STEP
    if undersized: lot = MIN_LOT_STEP
    risk_usd_actual = lot * price_risk * XAUUSD_LOT_UNITS
    risk_pct_actual = (risk_usd_actual / balance) * 100
    return {
        "lot": lot, "risk_usd": round(risk_usd_actual, 2),
        "risk_pct_actual": round(risk_pct_actual, 2), "undersized": undersized,
    }

def send_telegram(text, with_keyboard=False, chat_id=None):
    targets = [chat_id] if chat_id else list(SUBSCRIBERS)
    reply_markup = json.dumps({"keyboard": [["📊 Signal"]], "resize_keyboard": True}) if with_keyboard else None
    for target in targets:
        payload = {"chat_id": target, "text": text}
        if reply_markup: payload["reply_markup"] = reply_markup
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", data=payload, timeout=15)
        except Exception as e:
            logger.error(f"Telegram xatosi ({target}): {e}")

def _trade_age_text(opened_at_iso):
    if not opened_at_iso: return ""
    try:
        opened_at = datetime.datetime.fromisoformat(opened_at_iso)
        elapsed = datetime.datetime.now(datetime.timezone.utc) - opened_at
        hours = elapsed.total_seconds() / 3600
        return f" ({hours:.1f} soat oldin)"
    except: return ""

def build_signal_status_text():
    if last_status["price"] is None: return "Bot tekshiruv bajarmadi."
    lines = [
        f"Holat: {'⏸ PAUZADA' if paused else '▶️ Ishlamoqda'}",
        f"Tekshiruv: {last_status['checked_at']} | Narx: {round(last_status['price'], 2)}",
        f"1m: {last_status['bias1'] or 'n/a'} | 5m: {last_status['bias5'] or 'n/a'}",
        f"15m: {last_status['bias15'] or 'n/a'} | 30m: {last_status['bias30'] or 'n/a'}",
        f"1h: {last_status.get('bias1h') or 'n/a'} | 4h: {last_status.get('bias4h') or 'n/a'}",
    ]
    if current_trade:
        age_text = _trade_age_text(current_trade.get("opened_at"))
        lines.extend([
            "", f"OCHIQ: {current_trade['signal']}{age_text}",
            f"Entry: {current_trade['entry']} | SL: {current_trade['sl']} | TP1: {current_trade['tp1']} | TP2: {current_trade['tp2']}"
        ])
        with state_lock:
            balance, risk_pct = ACCOUNT.get("balance"), ACCOUNT.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT)
        if balance:
            sizing = calculate_position_size(current_trade["entry"], current_trade["sl"], balance, risk_pct)
            if sizing: lines.append(f"Tavsiya Lot: {sizing['lot']} (~{sizing['risk_usd']}$ risk)")
    else: lines.extend(["", "Ochiq bitim yo'q - scalping kutilmoqda."])
    
    lines.append(f"Statistika: {win_rate_text(load_log())}")
    return "\n".join(lines)

def register_bot_commands():
    commands = [
        {"command": "start", "description": "Obuna bo'lish"},
        {"command": "signal", "description": "Holatni ko'rish"},
        {"command": "pause", "description": "Pauza (admin)"},
        {"command": "resume", "description": "Davom etish (admin)"},
        {"command": "close", "description": "Yopish (admin)"},
        {"command": "balance", "description": "Balans (admin)"},
        {"command": "risk", "description": "Risk % (admin)"},
    ]
    try:
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands", json={"commands": commands}, timeout=15)
    except: pass

def telegram_listener():
    global paused
    offset = None
    backoff = 5
    while True:
        try:
            params = {"timeout": 25}
            if offset is not None: params["offset"] = offset
            response = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates", params=params, timeout=30)
            if response.status_code != 200:
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            backoff = 5 
            for update in response.json().get("result", []):
                offset = update["update_id"] + 1
                try:
                    message = update.get("message", {})
                    text = (message.get("text") or "").strip()
                    chat_id = str(message.get("chat", {}).get("id", ""))
                    if not chat_id: continue
                    is_admin = chat_id == ADMIN_CHAT_ID

                    if text == "/start":
                        with state_lock:
                            SUBSCRIBERS.add(chat_id)
                            save_subscribers()
                        send_telegram("✅ Obuna bo'ldingiz! (1/5/15/30m Scalping)", with_keyboard=True, chat_id=chat_id)
                    elif text in ("/signal", "📊 Signal"):
                        send_telegram(build_signal_status_text(), with_keyboard=True, chat_id=chat_id)
                    elif text == "/pause" and is_admin: paused = True; send_telegram("⏸ Pauza")
                    elif text == "/resume" and is_admin: paused = False; send_telegram("▶️ Davom")
                    elif text == "/close" and is_admin:
                        with state_lock:
                            if current_trade:
                                close_trade("MANUAL", last_status.get("price"))
                    elif text.startswith("/balance") and is_admin:
                        try:
                            ACCOUNT["balance"] = float(text.split()[1].replace(",", "."))
                            save_account()
                            send_telegram(f"✅ Balans saqlandi: {ACCOUNT['balance']}")
                        except: pass
                    elif text.startswith("/risk") and is_admin:
                        try:
                            ACCOUNT["risk_pct"] = float(text.split()[1].replace(",", "."))
                            save_account()
                            send_telegram(f"✅ Risk saqlandi: {ACCOUNT['risk_pct']}")
                        except: pass
                except: pass
        except:
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

def start_post_trade_tracking(side, close_price):
    global post_trade
    post_trade = {"side": side, "close_price": close_price, "extreme": close_price, "checks": 0}

def update_post_trade(price):
    global post_trade
    if post_trade is None: return
    if post_trade["side"] == "LONG": post_trade["extreme"] = max(post_trade["extreme"], price)
    else: post_trade["extreme"] = min(post_trade["extreme"], price)
    post_trade["checks"] += 1
    if post_trade["checks"] >= POST_TRADE_CHECKS:
        post_trade = None

def close_trade(result, price):
    global current_trade, warned_flip
    with state_lock:
        trade = current_trade
        if trade is None: return
        log = load_log()
        log.append({
            "id": len(log) + 1, "symbol": SYMBOL, "signal": trade["signal"],
            "entry": trade["entry"], "result": result, "close_price": price,
            "closed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
        save_log(log)
        send_telegram(f"GOLD {trade['signal']} yopildi - {result} ({price})\nStatistika: {win_rate_text(log)}")
        start_post_trade_tracking(trade["signal"], price)
        current_trade = None
        warned_flip = False

def monitor_open_trade(price, bias5):
    global current_trade, warned_flip
    with state_lock:
        trade = current_trade
        if trade is None: return
        side = trade["signal"]

        sl_hit = price <= trade["sl"] if side == "LONG" else price >= trade["sl"]
        tp2_hit = price >= trade["tp2"] if side == "LONG" else price <= trade["tp2"]
        
        if side == "LONG":
            if price >= trade["tp1"]:
                new_sl = trade["entry"] + (price - trade["entry"]) * 0.5 
                if new_sl > trade["sl"]:
                    trade["sl"] = round(new_sl, 2)
                    trade["breakeven"] = True
                    if not trade.get("tp1_notified"):
                        trade["tp1_notified"] = True
                        send_telegram(f"🔥 TP1! SL foydaga surildi: {trade['sl']}")
        else:
            if price <= trade["tp1"]:
                new_sl = trade["entry"] - (trade["entry"] - price) * 0.5
                if new_sl < trade["sl"]:
                    trade["sl"] = round(new_sl, 2)
                    trade["breakeven"] = True
                    if not trade.get("tp1_notified"):
                        trade["tp1_notified"] = True
                        send_telegram(f"🔥 TP1! SL foydaga surildi: {trade['sl']}")

        if sl_hit:
            close_trade("BE/Trailing SL" if trade.get("breakeven") else "SL", price)
            return
        if tp2_hit:
            close_trade("TP2", price)
            return

        if bias5 is not None and bias5 != trade["bias"] and not warned_flip:
            send_telegram(f"DIQQAT: Bozor struktura {bias5}ga o'zgardi (Erta yopish tavsiya etiladi).")
            warned_flip = True

def fetch_high_impact_news():
    now = datetime.datetime.now(datetime.timezone.utc)
    cached, fetched_at = _news_cache.get("events"), _news_cache.get("fetched_at")
    if cached is not None and fetched_at is not None and (now - fetched_at).total_seconds() < NEWS_CACHE_TTL_SEC:
        return cached
    events = []
    try:
        response = requests.get(NEWS_CALENDAR_URL, headers=HEADERS, timeout=10)
        for item in response.json():
            if item.get("country") != "USD" or item.get("impact") != "High": continue
            events.append({"title": item.get("title", "?"), "time": datetime.datetime.fromisoformat(item["date"].replace("Z", "+00:00"))})
    except: pass
    _news_cache["events"] = events
    _news_cache["fetched_at"] = now
    return events

def is_news_blackout():
    if not NEWS_FILTER_ENABLED: return False, None
    now = datetime.datetime.now(datetime.timezone.utc)
    for event in fetch_high_impact_news():
        delta_minutes = (event["time"] - now).total_seconds() / 60
        if -NEWS_BLOCK_MINUTES_AFTER <= delta_minutes <= NEWS_BLOCK_MINUTES_BEFORE:
            return True, event["title"]
    return False, None

def run():
    global current_trade, last_impulse_ts, last_impulse_info
    try:
        candles1_raw = fetch_ohlcv("1m")
        candles5_raw = fetch_ohlcv("5m")
        candles15_raw = fetch_ohlcv("15m")
        candles30_raw = fetch_ohlcv("30m")
    except Exception as e: return

    closed1 = closed_only(candles1_raw)
    closed5 = closed_only(candles5_raw)
    closed15 = closed_only(candles15_raw)
    closed30 = closed_only(candles30_raw)
    price = candles1_raw[-1][C] + PRICE_OFFSET

    bias1 = confirmed_structure_bias(closed1)
    bias5 = confirmed_structure_bias(closed5)
    bias15 = confirmed_structure_bias(closed15)
    bias30 = confirmed_structure_bias(closed30)
    atr5 = calculate_atr(closed5)

    bias1h, bias4h, bias1d = None, None, None
    try: bias1h = confirmed_structure_bias(closed_only(fetch_ohlcv("1h")))
    except: pass
    try: bias4h = confirmed_structure_bias(closed_only(fetch_ohlcv("4h")))
    except: pass
    try: bias1d = confirmed_structure_bias(closed_only(fetch_ohlcv("1d")))
    except: pass

    last_status.update({
        "price": price, "bias1": bias1, "bias5": bias5,
        "bias15": bias15, "bias30": bias30,
        "bias1h": bias1h, "bias4h": bias4h, "bias1d": bias1d,
        "checked_at": time.strftime("%H:%M:%S"),
    })
    save_status(round(price, 2), bias1, bias5, bias15, bias30, bias1h, bias4h, bias1d)

    with state_lock: PRICE_HISTORY.append(round(price, 2))

    impulse = detect_impulse(closed1)
    if impulse and impulse["ts"] != last_impulse_ts:
        last_impulse_ts = impulse["ts"]
        with state_lock:
            last_impulse_info = dict(impulse)
            last_impulse_info["detected_at"] = time.strftime("%H:%M:%S")

    with state_lock:
        if current_trade:
            monitor_open_trade(price, bias5)
            save_status(round(price, 2), bias1, bias5, bias15, bias30, bias1h, bias4h, bias1d)
            return

        update_post_trade(price)
        if paused or (KILLZONES_ENABLED and not in_killzone()): return

        blackout, event_title = is_news_blackout()
        if blackout: return
        
        # BAROVAR 1/5/15/30m tekshiruvi (faqat hammasi bir xil bo'lsa)
        if None in (bias1, bias5, bias15, bias30) or not (bias1 == bias5 == bias15 == bias30): 
            return
            
        if HTF_FILTER_ENABLED and bias1h is not None and bias1h != bias5: return
        if STRONG_HTF_FILTER_ENABLED and bias4h is not None and bias4h != bias5: return
        if DAILY_HTF_FILTER_ENABLED and bias1d is not None and bias1d != bias5: return

        trade = build_trade(closed5, bias5, price, atr5)

    if trade is None: return

    risk = abs(trade["entry"] - trade["sl"])
    if atr5 and risk < atr5 * ATR_MIN_RISK_MULT: return

    rsis5 = calculate_rsi(closed5)
    vwap5 = calculate_vwap(closed5)
    bullish_divergence, bearish_divergence = detect_rsi_divergence(closed5, rsis5)
    sweep5 = detect_liquidity_sweep(closed5)

    btc_bias5, eur_bias5 = None, None
    try: btc_bias5 = confirmed_structure_bias(closed_only(fetch_ohlcv("5m", symbol=BTC_SYMBOL)))
    except: pass
    try: eur_bias5 = confirmed_structure_bias(closed_only(fetch_ohlcv("5m", symbol=EUR_SYMBOL)))
    except: pass

    confirmations = confirmation_check(
        bias5, price, vwap5, bullish_divergence, bearish_divergence, 
        btc_bias5, eur_bias5, trade.get("confluence", False), sweep=sweep5
    )
    if len(confirmations) < MIN_CONFIRMATIONS: return

    log = load_log()
    with state_lock:
        balance, risk_pct = ACCOUNT.get("balance"), ACCOUNT.get("risk_pct", DEFAULT_RISK_PER_TRADE_PCT)
    if balance:
        sizing = calculate_position_size(trade["entry"], trade["sl"], balance, risk_pct)
        if sizing is None: position_line = "\nLot: hisoblab bo'lmadi."
        elif sizing["undersized"]: position_line = f"\n⚠️ Lot: {sizing['lot']} (minimal) - tavakkal ~{sizing['risk_pct_actual']}%"
        else: position_line = f"\nTavsiya etilgan lot: {sizing['lot']} (~{sizing['risk_usd']}$ risk)"
    else: position_line = "\nLot tavsiyasi uchun /balance <miqdor> kiriting."

    message = (
        f"GOLD SCALP - {trade['signal']}\n"
        f"1/5/15/30m bias: {bias5} | 1h: {bias1h or 'n/a'}\n"
        f"Narx: {round(price, 2)}\n"
        f"Entry: {trade['entry']}\n"
        f"SL: {trade['sl']}\n"
        f"TP1: {trade['tp1']} | TP2: {trade['tp2']}\n"
        f"ATR(5m): {round(atr5, 2) if atr5 else 'n/a'}\n"
        f"Tasdiqlash: {', '.join(confirmations) if confirmations else '-'}"
        f"{position_line}\nWin rate: {win_rate_text(log)}"
    )
    send_telegram(message)
    with state_lock:
        current_trade = trade
        save_status(round(price, 2), bias1, bias5, bias15, bias30, bias1h, bias4h, bias1d)


app = Flask(__name__)
BIAS_LABELS_UZ = {"bullish": "ko'tarilish", "bearish": "pasayish", None: "aniqlanmagan"}

def _bias_dot_color(bias):
    if bias == "bullish": return "#5B9C6D"
    if bias == "bearish": return "#C0553B"
    return "#5B5445"

def _render_bias_chip(tf_label, bias):
    color = _bias_dot_color(bias)
    text = BIAS_LABELS_UZ.get(bias, "aniqlanmagan")
    return f'<div class="chip"><span class="chip-dot" style="background:{color}"></span><span class="chip-tf">{html.escape(tf_label)}</span><span class="chip-val">{html.escape(text)}</span></div>'

def _build_sparkline(prices, width=272, height=54, color="#C9A227"):
    if not prices or len(prices) < 2: return '<div class="spark-empty">narx tarixi kutilmoqda&hellip;</div>'
    min_p, max_p = min(prices), max(prices)
    span = (max_p - min_p) or (min_p * 0.001 or 1)
    step = width / (len(prices) - 1)
    pts = [(index * step, height - ((price - min_p) / span) * (height - 8) - 4) for index, price in enumerate(prices)]
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    last_x, last_y = pts[-1]
    fill_path = f"M0,{height} L" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts) + f" L{width},{height} Z"
    return f"""<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" preserveAspectRatio="none" class="spark">
      <path d="{fill_path}" fill="url(#sparkfade)" stroke="none"/>
      <polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
      <circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="2.6" fill="{color}"/>
      <defs><linearGradient id="sparkfade" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="{color}" stop-opacity="0.28"/><stop offset="100%" stop-color="{color}" stop-opacity="0"/></linearGradient></defs></svg>"""

@app.route("/")
def home():
    with state_lock:
        trade = dict(current_trade) if current_trade else None
        status_snapshot = dict(last_status)
        is_paused = paused
        price_history = list(PRICE_HISTORY)

    price_value = status_snapshot["price"]
    price_display = f"${price_value:,.2f}" if price_value is not None else "&mdash;"
    time_display = status_snapshot["checked_at"] or "hali yangilanmadi"

    bias_grid = "".join([
        _render_bias_chip("1m", status_snapshot.get("bias1")),
        _render_bias_chip("5m", status_snapshot.get("bias5")),
        _render_bias_chip("15m", status_snapshot.get("bias15")),
        _render_bias_chip("30m", status_snapshot.get("bias30")),
        _render_bias_chip("1h", status_snapshot.get("bias1h")),
    ])

    if trade:
        side_color = "#5B9C6D" if trade["signal"] == "LONG" else "#C0553B"
        trade_block = f'<div class="trade-open"><span class="side-tag" style="background:{side_color}">{html.escape(trade["signal"])}</span><div class="trade-grid"><div><span class="k">entry</span><span class="v">{trade["entry"]}</span></div><div><span class="k">sl</span><span class="v">{trade["sl"]}</span></div><div><span class="k">tp1</span><span class="v">{trade["tp1"]}</span></div><div><span class="k">tp2</span><span class="v">{trade["tp2"]}</span></div></div></div>'
    else:
        trade_block = '<div class="muted-line italic">Hozircha ochiq savdo yo\'q.</div>'

    log = load_log()
    wins = sum(1 for entry in log if entry.get("result") in ("TP1", "TP2"))
    total = len(log)
    win_pct = f"{wins / total * 100:.0f}%" if total else "&mdash;"

    return f"""<!doctype html><html lang="uz"><head><meta charset="utf-8"><meta http-equiv="refresh" content="5"><meta name="viewport" content="width=device-width, initial-scale=1">
    <title>1/5/15/30m Scalping</title>
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
      :root {{ --bg:#14110D; --surface:#1C1710; --border:#33291C; --text:#EDE3CF; --muted:#8C8271; --gold:#C9A227; }}
      body {{ background:var(--bg); color:var(--text); font-family:'IBM Plex Mono', monospace; padding:20px; }}
      .ticket {{ max-width:400px; background:var(--surface); border:1px solid var(--border); border-radius:18px; padding:20px; margin:auto; }}
      .hero-price {{ font-size:2.5em; font-weight:600; margin:10px 0; }}
      .chips {{ display:flex; flex-wrap:wrap; gap:8px; margin:15px 0; }}
      .chip {{ border:1px solid var(--border); padding:5px 8px; border-radius:8px; font-size:0.8em; }}
      .trade-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:10px; }}
    </style></head><body>
      <div class="ticket">
        <div>GOLD SCALPING &middot; XAUUSDT</div>
        <div class="hero-price">{price_display}</div>
        <div style="color:var(--muted); font-size:0.8em; margin-bottom:15px;">{time_display} da yangilandi</div>
        {_build_sparkline(price_history)}
        <div class="chips">{bias_grid}</div><hr style="border-top:1px dashed var(--border); margin:15px 0;">
        {trade_block}
        <hr style="border-top:1px dashed var(--border); margin:15px 0;">
        <div>Win rate: {win_pct} ({total} ta savdo)</div>
      </div>
    </body></html>"""

def loop():
    logger.info(f"1/5/15/30m Scalping bot ishga tushdi - har {CHECK_INTERVAL_SEC}s tekshiradi")
    while True:
        try: run()
        except Exception as error: logger.error(f"Xatolik: {error}")
        time.sleep(CHECK_INTERVAL_SEC)

def restore_state():
    global current_trade
    if not os.path.exists(STATUS_FILE): return
    try:
        with open(STATUS_FILE, encoding='utf-8') as file: status = json.load(file)
        if status.get("currentTrade"):
            with state_lock: current_trade = status["currentTrade"]
    except: pass

if __name__ == "__main__":
    restore_state()
    threading.Thread(target=loop, daemon=True).start()
    register_bot_commands()
    threading.Thread(target=telegram_listener, daemon=True).start()
    app.run(host="::", port=int(os.environ.get("PORT", 8100)))