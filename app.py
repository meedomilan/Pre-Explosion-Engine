import asyncio
import html
import json
import logging
import math
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse


# =========================================================
# إعدادات Railway
# =========================================================

BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

PORT = int(os.getenv("PORT", "8080"))
TZ = ZoneInfo(os.getenv("TZ", "Asia/Riyadh"))

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "10"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "18"))
RADAR_POOL = int(os.getenv("RADAR_POOL", "160"))
DEEP_CANDIDATES = int(os.getenv("DEEP_CANDIDATES", "55"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "750000"))

TIMEFRAMES = [
    tf.strip()
    for tf in os.getenv("TIMEFRAMES", "15m,1h,4h").split(",")
    if tf.strip()
]
FIXED_MTF = ["1h", "4h", "1d"]

PREP_SCORE = float(os.getenv("PREP_SCORE", "48"))
EARLY_SCORE = float(os.getenv("EARLY_SCORE", "54"))
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "64"))
GOLD_SCORE = float(os.getenv("GOLD_SCORE", "78"))

PREP_MTF_COUNT = int(os.getenv("PREP_MTF_COUNT", "1"))
EARLY_MTF_COUNT = int(os.getenv("EARLY_MTF_COUNT", "1"))
ENTRY_MTF_COUNT = int(os.getenv("ENTRY_MTF_COUNT", "2"))
GOLD_MTF_COUNT = int(os.getenv("GOLD_MTF_COUNT", "3"))

MOM_VOL_MULT = float(os.getenv("MOM_VOL_MULT", "1.5"))
MOM_BODY_ATR = float(os.getenv("MOM_BODY_ATR", "0.60"))
ENTRY_BODY_ATR = float(os.getenv("ENTRY_BODY_ATR", "0.20"))
BREAKOUT_LOOKBACK = int(os.getenv("BREAKOUT_LOOKBACK", "10"))

MAX_ENTRY_EXTENSION_ATR = float(os.getenv("MAX_ENTRY_EXTENSION_ATR", "0.55"))
MIN_SCORE_DRIFT = float(os.getenv("MIN_SCORE_DRIFT", "1.5"))
DIRECTION_GAP = float(os.getenv("DIRECTION_GAP", "6"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "75"))

STOP_ATR_BUFFER = float(os.getenv("STOP_ATR_BUFFER", "0.20"))
STOP_MIN_ATR = float(os.getenv("STOP_MIN_ATR", "0.75"))
MIN_RR_TP1 = float(os.getenv("MIN_RR_TP1", "1.0"))

SEND_PREP_ALERTS = os.getenv("SEND_PREP_ALERTS", "true").lower() == "true"
SEND_CANCEL_ALERTS = os.getenv("SEND_CANCEL_ALERTS", "false").lower() == "true"
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_TEST_MESSAGE = os.getenv("SEND_TEST_MESSAGE", "true").lower() == "true"
ENABLE_MANUAL_TEST_ENDPOINT = os.getenv("ENABLE_MANUAL_TEST_ENDPOINT", "true").lower() == "true"

BINANCE_RETRIES = int(os.getenv("BINANCE_RETRIES", "4"))
SYMBOL_TIMEOUT = float(os.getenv("SYMBOL_TIMEOUT", "14"))
SCAN_TIMEOUT = float(os.getenv("SCAN_TIMEOUT", "55"))
EXCHANGE_CACHE_SECONDS = int(os.getenv("EXCHANGE_CACHE_SECONDS", "3600"))

DB_PATH = os.getenv("DB_PATH", "data/golden_entry.db")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("golden-entry")


# =========================================================
# أدوات الحساب
# =========================================================

def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def now_local() -> datetime:
    return datetime.now(TZ)


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.5f}".rstrip("0").rstrip(".")
    if value >= 0.01:
        return f"{value:.7f}".rstrip("0").rstrip(".")
    return f"{value:.10f}".rstrip("0").rstrip(".")


def ema_series(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (length + 1.0)
    output = [values[0]]
    for value in values[1:]:
        output.append(alpha * value + (1.0 - alpha) * output[-1])
    return output


def sma(values: list[float], length: int) -> float:
    window = values[-length:]
    return sum(window) / max(1, len(window))


def rsi(values: list[float], length: int = 14) -> float:
    if len(values) < length + 1:
        return 50.0
    gains = []
    losses = []
    for index in range(len(values) - length, len(values)):
        change = values[index] - values[index - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / length
    avg_loss = sum(losses) / length
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def macd(values: list[float]) -> tuple[float, float, float, float]:
    ema12 = ema_series(values, 12)
    ema26 = ema_series(values, 26)
    macd_line_series = [a - b for a, b in zip(ema12, ema26)]
    signal_series = ema_series(macd_line_series, 9)
    hist_series = [a - b for a, b in zip(macd_line_series, signal_series)]
    return (
        macd_line_series[-1],
        signal_series[-1],
        hist_series[-1],
        hist_series[-2] if len(hist_series) > 1 else hist_series[-1],
    )


def atr(rows: list[list[Any]], length: int = 14) -> float:
    if len(rows) < 2:
        return 0.0
    true_ranges = []
    for index in range(1, len(rows)):
        high = float(rows[index][2])
        low = float(rows[index][3])
        previous_close = float(rows[index - 1][4])
        true_ranges.append(
            max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        )
    return sum(true_ranges[-length:]) / max(1, min(length, len(true_ranges)))


def score_calc(rows: list[list[Any]], use_current: bool = True) -> float:
    """مطابقة دالة scoreCalc من مؤشر Ahmed Ultimate قدر الإمكان."""
    data = rows if use_current else rows[:-1]
    if len(data) < 205:
        return 50.0

    opens = [float(row[1]) for row in data]
    highs = [float(row[2]) for row in data]
    lows = [float(row[3]) for row in data]
    closes = [float(row[4]) for row in data]
    volumes = [float(row[5]) for row in data]

    close = closes[-1]
    open_price = opens[-1]
    high = highs[-1]
    low = lows[-1]

    ema20 = ema_series(closes, 20)[-1]
    ema50 = ema_series(closes, 50)[-1]
    ema200 = ema_series(closes, 200)[-1]
    rsi_value = rsi(closes, 14)
    macd_line, signal_line, hist, previous_hist = macd(closes)
    atr_value = atr(data, 14)
    average_volume = sma(volumes[:-1], 20)
    volume_ratio = safe_div(volumes[-1], average_volume, 1.0)
    body_ratio = safe_div(abs(close - open_price), atr_value, 0.0)
    candle_range = max(high - low, 1e-12)
    close_location = (close - low) / candle_range

    score = 50.0
    score += 7.0 if close > ema20 else -7.0
    score += 8.0 if ema20 > ema50 else -8.0
    score += 10.0 if ema50 > ema200 else -10.0
    score += 6.0 if close > ema200 else -6.0
    score += 7.0 if macd_line > signal_line else -7.0
    score += 5.0 if hist > 0 else -5.0
    score += 4.0 if hist > previous_hist else -4.0
    score += 5.0 if rsi_value > 55 else (-5.0 if rsi_value < 45 else 0.0)

    if close > open_price and volume_ratio >= 1.2:
        score += 4.0
    elif close < open_price and volume_ratio >= 1.2:
        score -= 4.0

    if close > open_price and body_ratio >= 0.6:
        score += 3.0
    elif close < open_price and body_ratio >= 0.6:
        score -= 3.0

    score += 3.0 if close_location >= 0.7 else (-3.0 if close_location <= 0.3 else 0.0)
    return clamp(score)


def golden_scores(rows: list[list[Any]]) -> dict[str, Any]:
    """مطابقة bullScore / bearScore من المؤشر، باستخدام الشمعة الحالية الحية."""
    if len(rows) < 205:
        return {}

    opens = [float(row[1]) for row in rows]
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    closes = [float(row[4]) for row in rows]
    volumes = [float(row[5]) for row in rows]

    open_price = opens[-1]
    high = highs[-1]
    low = lows[-1]
    close = closes[-1]

    ma25 = ema_series(closes, 25)[-1]
    ma50 = ema_series(closes, 50)[-1]
    ma200 = ema_series(closes, 200)[-1]

    macd_line, signal_line, hist, previous_hist = macd(closes)
    atr_value = atr(rows, 14)
    average_volume = sma(volumes[:-1], 20)
    volume_ratio = safe_div(volumes[-1], average_volume, 1.0)
    body_ratio = safe_div(abs(close - open_price), atr_value, 0.0)

    candle_range = max(high - low, 1e-12)
    close_position = clamp((close - low) / candle_range, 0.0, 1.0)
    buy_pct = close_position * 100.0
    sell_pct = 100.0 - buy_pct

    previous_high = max(highs[-BREAKOUT_LOOKBACK - 1:-1])
    previous_low = min(lows[-BREAKOUT_LOOKBACK - 1:-1])
    bull_breakout = close > previous_high and close > open_price
    bear_breakout = close < previous_low and close < open_price

    bull = 0.0
    bull += 8.0 if close > open_price else 0.0
    bull += 7.0 if close_position >= 0.75 else (4.0 if close_position >= 0.60 else 0.0)
    bull += 13.0 if volume_ratio >= 2.0 else (10.0 if volume_ratio >= MOM_VOL_MULT else (5.0 if volume_ratio >= 1.2 else 0.0))
    bull += 10.0 if body_ratio >= 1.0 else (7.0 if body_ratio >= MOM_BODY_ATR else 0.0)
    bull += 8.0 if macd_line > signal_line else 0.0
    bull += 7.0 if hist > 0 else 0.0
    bull += 5.0 if hist > previous_hist else 0.0
    bull += 5.0 if close > ma25 else 0.0
    bull += 6.0 if close > ma50 else 0.0
    bull += 7.0 if close > ma200 else 0.0
    bull += 4.0 if ma25 > ma50 else 0.0
    bull += 5.0 if ma50 > ma200 else 0.0
    bull += 8.0 if buy_pct >= 70 else (5.0 if buy_pct >= 60 else (3.0 if buy_pct >= 55 else 0.0))
    bull += 7.0 if bull_breakout else 0.0

    bear = 0.0
    bear += 8.0 if close < open_price else 0.0
    bear += 7.0 if close_position <= 0.25 else (4.0 if close_position <= 0.40 else 0.0)
    bear += 13.0 if volume_ratio >= 2.0 else (10.0 if volume_ratio >= MOM_VOL_MULT else (5.0 if volume_ratio >= 1.2 else 0.0))
    bear += 10.0 if body_ratio >= 1.0 else (7.0 if body_ratio >= MOM_BODY_ATR else 0.0)
    bear += 8.0 if macd_line < signal_line else 0.0
    bear += 7.0 if hist < 0 else 0.0
    bear += 5.0 if hist < previous_hist else 0.0
    bear += 5.0 if close < ma25 else 0.0
    bear += 6.0 if close < ma50 else 0.0
    bear += 7.0 if close < ma200 else 0.0
    bear += 4.0 if ma25 < ma50 else 0.0
    bear += 5.0 if ma50 < ma200 else 0.0
    bear += 8.0 if sell_pct >= 70 else (5.0 if sell_pct >= 60 else (3.0 if sell_pct >= 55 else 0.0))
    bear += 7.0 if bear_breakout else 0.0

    return {
        "bull": clamp(bull),
        "bear": clamp(bear),
        "price": close,
        "open": open_price,
        "high": high,
        "low": low,
        "atr": atr_value,
        "body_ratio": body_ratio,
        "close_position": close_position,
        "volume_ratio": volume_ratio,
        "buy_pct": buy_pct,
        "sell_pct": sell_pct,
        "bull_breakout": bull_breakout,
        "bear_breakout": bear_breakout,
        "open_time": int(rows[-1][0]),
        "close_time": int(rows[-1][6]),
    }


def choose_stage(direction: str, scores: dict[str, Any], mtf_count: int, is_closed: bool) -> str | None:
    directional = scores["bull"] if direction == "BUY" else scores["bear"]
    opposite = scores["bear"] if direction == "BUY" else scores["bull"]

    if directional <= opposite:
        return None

    direction_ok = (
        scores["price"] > scores["open"]
        and scores["close_position"] >= 0.60
        and scores["body_ratio"] >= ENTRY_BODY_ATR
        if direction == "BUY"
        else scores["price"] < scores["open"]
        and scores["close_position"] <= 0.40
        and scores["body_ratio"] >= ENTRY_BODY_ATR
    )

    breakout = scores["bull_breakout"] if direction == "BUY" else scores["bear_breakout"]

    if is_closed and directional >= GOLD_SCORE and mtf_count >= GOLD_MTF_COUNT and breakout:
        return "CONFIRM"

    if directional >= ENTRY_SCORE and mtf_count >= ENTRY_MTF_COUNT and direction_ok:
        return "ENTRY"

    if directional >= EARLY_SCORE and mtf_count >= EARLY_MTF_COUNT:
        return "EARLY"

    if SEND_PREP_ALERTS and directional >= PREP_SCORE and mtf_count >= PREP_MTF_COUNT:
        return "PREP"

    return None


def stage_rank(stage: str) -> int:
    return {"PREP": 1, "EARLY": 2, "ENTRY": 3, "CONFIRM": 4}[stage]


def build_trade_plan(direction: str, rows: list[list[Any]], price: float, atr_value: float):
    lows = [float(row[3]) for row in rows[-20:]]
    highs = [float(row[2]) for row in rows[-20:]]
    recent_low = min(lows)
    recent_high = max(highs)

    entry_half = atr_value * 0.08
    entry_low = price - entry_half
    entry_high = price + entry_half

    if direction == "BUY":
        structural_stop = recent_low - atr_value * STOP_ATR_BUFFER
        minimum_stop = price - atr_value * STOP_MIN_ATR
        stop = min(structural_stop, minimum_stop)
        mid = (entry_low + entry_high) / 2.0
        risk = max(mid - stop, atr_value * STOP_MIN_ATR)
        tp1 = mid + risk
        tp2 = mid + 2.0 * risk
        tp3 = mid + 3.0 * risk
    else:
        structural_stop = recent_high + atr_value * STOP_ATR_BUFFER
        minimum_stop = price + atr_value * STOP_MIN_ATR
        stop = max(structural_stop, minimum_stop)
        mid = (entry_low + entry_high) / 2.0
        risk = max(stop - mid, atr_value * STOP_MIN_ATR)
        tp1 = mid - risk
        tp2 = mid - 2.0 * risk
        tp3 = mid - 3.0 * risk

    rr = lambda target: abs(target - mid) / max(abs(mid - stop), 1e-12)

    return {
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr1": rr(tp1),
        "rr2": rr(tp2),
        "rr3": rr(tp3),
    }


@dataclass
class Signal:
    symbol: str
    timeframe: str
    direction: str
    stage: str
    score: float
    opposite_score: float
    mtf_count: int
    score_current: float
    score_1h: float
    score_4h: float
    score_1d: float
    drift: float
    extension_atr: float
    price: float
    candle_open_time: int
    entry_low: float
    entry_high: float
    stop: float
    tp1: float
    tp2: float
    tp3: float
    rr1: float
    rr2: float
    rr3: float
    reasons: list[str]


# =========================================================
# قاعدة البيانات
# =========================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    direction TEXT NOT NULL,
    stage TEXT NOT NULL,
    candle_open_time INTEGER NOT NULL,
    score REAL,
    opposite_score REAL,
    mtf_count INTEGER,
    drift REAL,
    extension_atr REAL,
    price REAL,
    entry_low REAL,
    entry_high REAL,
    stop REAL,
    tp1 REAL,
    tp2 REAL,
    tp3 REAL,
    rr1 REAL,
    rr2 REAL,
    rr3 REAL,
    reasons_json TEXT,
    status TEXT DEFAULT 'OPEN',
    tp1_at TEXT,
    tp2_at TEXT,
    tp3_at TEXT,
    stop_at TEXT,
    best_price REAL,
    worst_price REAL,
    outcome TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_stage
ON signals(symbol,timeframe,direction,stage,candle_open_time);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    scan_number INTEGER,
    symbols_total INTEGER,
    candidates_total INTEGER,
    analyzed_total INTEGER,
    opportunities_total INTEGER,
    alerts_sent INTEGER,
    scan_seconds REAL,
    error TEXT
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def save_signal(signal: Signal) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                """INSERT INTO signals (
                    created_at,symbol,timeframe,direction,stage,candle_open_time,
                    score,opposite_score,mtf_count,drift,extension_atr,price,
                    entry_low,entry_high,stop,tp1,tp2,tp3,rr1,rr2,rr3,reasons_json,
                    best_price,worst_price
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now_local().isoformat(),
                    signal.symbol,
                    signal.timeframe,
                    signal.direction,
                    signal.stage,
                    signal.candle_open_time,
                    signal.score,
                    signal.opposite_score,
                    signal.mtf_count,
                    signal.drift,
                    signal.extension_atr,
                    signal.price,
                    signal.entry_low,
                    signal.entry_high,
                    signal.stop,
                    signal.tp1,
                    signal.tp2,
                    signal.tp3,
                    signal.rr1,
                    signal.rr2,
                    signal.rr3,
                    json.dumps(signal.reasons, ensure_ascii=False),
                    signal.price,
                    signal.price,
                ),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def record_checkpoint(scan_no, symbols, candidates, analyzed, opportunities, alerts, seconds, error=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO checkpoints (
                created_at,scan_number,symbols_total,candidates_total,
                analyzed_total,opportunities_total,alerts_sent,scan_seconds,error
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                now_local().isoformat(),
                scan_no,
                symbols,
                candidates,
                analyzed,
                opportunities,
                alerts,
                seconds,
                error,
            ),
        )
        await db.commit()


# =========================================================
# Binance
# =========================================================

class BinanceClient:
    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.exchange_cache: tuple[float, list[str]] | None = None

    async def start(self):
        connector = aiohttp.TCPConnector(limit=max(40, MAX_CONCURRENCY * 3), ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20, connect=8),
            connector=connector,
            headers={"User-Agent": "Ahmed-Golden-Entry-AI/1.0"},
        )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def get(self, path: str, params=None):
        assert self.session is not None
        last_error = None

        async with self.semaphore:
            for attempt in range(BINANCE_RETRIES):
                try:
                    async with self.session.get(BINANCE_BASE + path, params=params) as response:
                        if response.status in (418, 429):
                            await asyncio.sleep(min(2 ** attempt + 1, 12))
                            continue
                        response.raise_for_status()
                        data = await response.json()
                        if data is None:
                            raise RuntimeError("Binance returned null")
                        return data
                except Exception as error:
                    last_error = error
                    if attempt < BINANCE_RETRIES - 1:
                        await asyncio.sleep(min(1.5 ** attempt, 8))

        raise RuntimeError(f"Binance request failed {path}: {last_error!r}")

    async def symbols(self) -> list[str]:
        if self.exchange_cache and time.time() - self.exchange_cache[0] < EXCHANGE_CACHE_SECONDS:
            return self.exchange_cache[1]

        data = await self.get("/fapi/v1/exchangeInfo")
        symbols = [
            item["symbol"]
            for item in data.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
        ]
        if not symbols:
            raise RuntimeError("No Binance USDT perpetual symbols")
        self.exchange_cache = (time.time(), symbols)
        return symbols

    async def tickers(self):
        data = await self.get("/fapi/v1/ticker/24hr")
        if not isinstance(data, list):
            raise RuntimeError("Invalid ticker response")
        return data

    async def klines(self, symbol: str, interval: str, limit: int = 230):
        data = await self.get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        if not isinstance(data, list) or len(data) < 205:
            raise RuntimeError(f"Not enough klines: {symbol} {interval}")
        return data

    async def prices(self) -> dict[str, float]:
        data = await self.get("/fapi/v1/ticker/price")
        return {
            item["symbol"]: float(item["price"])
            for item in data
            if item.get("symbol") and item.get("price")
        }


# =========================================================
# المحرك
# =========================================================

class Engine:
    def __init__(self):
        self.client = BinanceClient()
        self.telegram_session: aiohttp.ClientSession | None = None
        self.running = True
        self.scan_number = 0
        self.symbol_count = 0
        self.candidate_count = 0
        self.alert_count = 0
        self.last_scan = None
        self.last_error = None
        self.fast_state: dict[str, dict[str, float]] = {}
        self.score_history: dict[tuple[str, str, str], float] = {}
        self.active_stages: dict[tuple[str, str, str, int], str] = {}

    async def start(self):
        await init_db()
        await self.client.start()
        self.telegram_session = aiohttp.ClientSession()

        if SEND_STARTUP_MESSAGE:
            await self.send_telegram(
                "✅ <b>Ahmed Golden Entry AI v1 بدأ العمل</b>\n\n"
                "🧠 المصدر: منطق الشمعة الذهبية في مؤشر Ahmed Ultimate\n"
                f"⏰ الفريمات: {' / '.join(TIMEFRAMES)}\n"
                f"🔵 احتمال: {PREP_SCORE:.0f}\n"
                f"🟡 استعداد: {EARLY_SCORE:.0f}\n"
                f"🟠 دخول الآن: {ENTRY_SCORE:.0f}\n"
                f"🔥 تأكيد: {GOLD_SCORE:.0f}\n"
                "⚠️ لا ينفذ صفقات تلقائيًا."
            )

        if SEND_TEST_MESSAGE:
            await self.send_telegram(
                "🧪 <b>رسالة اختبار ناجحة</b>\n\n"
                "✅ Telegram متصل\n"
                "✅ Railway يعمل\n"
                "✅ Golden Entry Engine جاهز"
            )

        asyncio.create_task(self.loop())
        asyncio.create_task(self.track_positions())

    async def close(self):
        self.running = False
        await self.client.close()
        if self.telegram_session and not self.telegram_session.closed:
            await self.telegram_session.close()

    async def send_telegram(self, text: str) -> bool:
        if not BOT_TOKEN or not CHAT_ID or not self.telegram_session:
            log.warning("Telegram variables missing")
            return False

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with self.telegram_session.post(url, json=payload, timeout=20) as response:
                body = await response.text()
                if response.status != 200:
                    log.error("Telegram error %s: %s", response.status, body)
                    return False
                return True
        except Exception:
            log.exception("Telegram send failed")
            return False

    async def loop(self):
        while self.running:
            started = time.monotonic()
            self.scan_number += 1
            error = None
            analyzed = opportunities = alerts = 0

            try:
                alerts, analyzed, opportunities = await asyncio.wait_for(
                    self.scan(),
                    timeout=SCAN_TIMEOUT,
                )
                self.last_error = None
            except Exception as exception:
                error = repr(exception)
                self.last_error = error
                log.exception("Scan failed")

            elapsed = time.monotonic() - started
            self.last_scan = now_local().isoformat()

            await record_checkpoint(
                self.scan_number,
                self.symbol_count,
                self.candidate_count,
                analyzed,
                opportunities,
                alerts,
                elapsed,
                error,
            )

            log.info(
                "scan=%s symbols=%s candidates=%s analyzed=%s opportunities=%s alerts=%s seconds=%.1f",
                self.scan_number,
                self.symbol_count,
                self.candidate_count,
                analyzed,
                opportunities,
                alerts,
                elapsed,
            )
            await asyncio.sleep(max(2, SCAN_SECONDS - elapsed))

    def radar_score(self, ticker: dict) -> tuple[float, dict[str, float]]:
        symbol = ticker.get("symbol", "")
        price = float(ticker.get("lastPrice", 0) or 0)
        quote_volume = float(ticker.get("quoteVolume", 0) or 0)
        trades = float(ticker.get("count", 0) or 0)
        day_change = abs(float(ticker.get("priceChangePercent", 0) or 0))

        previous = self.fast_state.get(symbol)
        price_change = volume_change = trades_change = 0.0
        if previous:
            price_change = abs(safe_div(price - previous["price"], previous["price"], 0)) * 100
            volume_change = max(0, safe_div(quote_volume - previous["volume"], previous["volume"], 0)) * 100
            trades_change = max(0, safe_div(trades - previous["trades"], previous["trades"], 0)) * 100

        liquidity = clamp((math.log10(max(quote_volume, 1)) - 5.2) * 22)
        score = clamp(
            price_change * 650
            + volume_change * 5
            + trades_change * 4
            + liquidity * 0.25
            + day_change * 1.5
        )
        return score, {"price": price, "volume": quote_volume, "trades": trades}

    async def scan(self):
        symbols, tickers = await asyncio.gather(
            self.client.symbols(),
            self.client.tickers(),
        )
        self.symbol_count = len(symbols)
        allowed = set(symbols)
        ranked = []

        for ticker in tickers:
            symbol = ticker.get("symbol")
            if symbol not in allowed:
                continue
            quote_volume = float(ticker.get("quoteVolume", 0) or 0)
            if quote_volume < MIN_QUOTE_VOLUME:
                continue

            score, state = self.radar_score(ticker)
            self.fast_state[symbol] = state
            ranked.append((score, quote_volume, symbol))

        ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
        candidates = [item[2] for item in ranked[:RADAR_POOL]][:DEEP_CANDIDATES]
        self.candidate_count = len(candidates)

        async def guarded(symbol: str):
            try:
                return await asyncio.wait_for(
                    self.analyze_symbol(symbol),
                    timeout=SYMBOL_TIMEOUT,
                )
            except Exception as error:
                log.debug("Analysis failed %s: %r", symbol, error)
                return []

        results = await asyncio.gather(
            *(guarded(symbol) for symbol in candidates)
        )

        signals = [signal for group in results for signal in group]
        analyzed = len(results)
        opportunities = len(signals)

        # نرسل الأقوى أولًا، مع حد أعلى في كل دورة.
        signals.sort(
            key=lambda signal: (
                stage_rank(signal.stage),
                signal.score,
                signal.drift,
                -signal.extension_atr,
            ),
            reverse=True,
        )

        alerts = 0
        sent_symbols = set()
        for signal in signals:
            if alerts >= MAX_ALERTS_PER_SCAN:
                break
            symbol_key = (signal.symbol, signal.timeframe)
            if symbol_key in sent_symbols:
                continue

            inserted = await save_signal(signal)
            if not inserted:
                continue

            ok = await self.send_telegram(self.signal_message(signal))
            if ok:
                alerts += 1
                self.alert_count += 1
                sent_symbols.add(symbol_key)

        return alerts, analyzed, opportunities

    async def analyze_symbol(self, symbol: str) -> list[Signal]:
        required_intervals = sorted(set(TIMEFRAMES + FIXED_MTF))
        data = await asyncio.gather(
            *(self.client.klines(symbol, interval) for interval in required_intervals)
        )
        klines = dict(zip(required_intervals, data))
        signals = []

        for timeframe in TIMEFRAMES:
            rows = klines[timeframe]
            live = golden_scores(rows)
            if not live:
                continue

            mtf_scores = {
                "current": score_calc(rows, use_current=True),
                "1h": score_calc(klines["1h"], use_current=True),
                "4h": score_calc(klines["4h"], use_current=True),
                "1d": score_calc(klines["1d"], use_current=True),
            }

            bull_count = sum(score >= 58 for score in mtf_scores.values())
            bear_count = sum(score <= 42 for score in mtf_scores.values())

            now_ms = int(time.time() * 1000)
            is_current_closed = now_ms > live["close_time"]

            for direction in ("BUY", "SELL"):
                mtf_count = bull_count if direction == "BUY" else bear_count
                stage = choose_stage(direction, live, mtf_count, is_current_closed)
                if not stage:
                    continue

                directional_score = live["bull"] if direction == "BUY" else live["bear"]
                opposite_score = live["bear"] if direction == "BUY" else live["bull"]

                if directional_score - opposite_score < DIRECTION_GAP:
                    continue

                history_key = (symbol, timeframe, direction)
                previous_score = self.score_history.get(history_key, directional_score)
                drift = directional_score - previous_score
                self.score_history[history_key] = directional_score

                # ENTRY يحتاج صعود الدرجة أو وصولًا قويًا واضحًا.
                if stage == "ENTRY" and drift < MIN_SCORE_DRIFT and directional_score < ENTRY_SCORE + 5:
                    continue

                if direction == "BUY":
                    pivot = min(float(row[3]) for row in rows[-8:])
                    extension = safe_div(live["price"] - pivot, live["atr"], 99)
                else:
                    pivot = max(float(row[2]) for row in rows[-8:])
                    extension = safe_div(pivot - live["price"], live["atr"], 99)

                if stage in ("PREP", "EARLY", "ENTRY") and extension > MAX_ENTRY_EXTENSION_ATR:
                    continue

                plan = build_trade_plan(
                    direction,
                    rows,
                    live["price"],
                    live["atr"],
                )
                if plan["rr1"] < MIN_RR_TP1:
                    continue

                reasons = []
                if live["volume_ratio"] >= 1.2:
                    reasons.append(f"الحجم {live['volume_ratio']:.2f}× متوسطه")
                if live["body_ratio"] >= ENTRY_BODY_ATR:
                    reasons.append(f"جسم الشمعة {live['body_ratio']:.2f} ATR")
                if direction == "BUY" and live["buy_pct"] >= 55:
                    reasons.append(f"ضغط الشراء التقديري {live['buy_pct']:.0f}%")
                if direction == "SELL" and live["sell_pct"] >= 55:
                    reasons.append(f"ضغط البيع التقديري {live['sell_pct']:.0f}%")
                if direction == "BUY" and live["bull_breakout"]:
                    reasons.append("اختراق صاعد حي")
                if direction == "SELL" and live["bear_breakout"]:
                    reasons.append("اختراق هابط حي")
                reasons.append(f"توافق {mtf_count}/4 فريمات")
                reasons.append(f"الدرجة تتحرك {drift:+.1f}")

                signals.append(Signal(
                    symbol=symbol,
                    timeframe=timeframe,
                    direction=direction,
                    stage=stage,
                    score=directional_score,
                    opposite_score=opposite_score,
                    mtf_count=mtf_count,
                    score_current=mtf_scores["current"],
                    score_1h=mtf_scores["1h"],
                    score_4h=mtf_scores["4h"],
                    score_1d=mtf_scores["1d"],
                    drift=drift,
                    extension_atr=extension,
                    price=live["price"],
                    candle_open_time=live["open_time"],
                    entry_low=plan["entry_low"],
                    entry_high=plan["entry_high"],
                    stop=plan["stop"],
                    tp1=plan["tp1"],
                    tp2=plan["tp2"],
                    tp3=plan["tp3"],
                    rr1=plan["rr1"],
                    rr2=plan["rr2"],
                    rr3=plan["rr3"],
                    reasons=reasons,
                ))

        # أفضل إشارة فقط لكل عملة في الدورة.
        if not signals:
            return []
        signals.sort(
            key=lambda signal: (
                stage_rank(signal.stage),
                signal.score,
                signal.drift,
            ),
            reverse=True,
        )
        return [signals[0]]

    def signal_message(self, signal: Signal) -> str:
        titles = {
            "PREP": "🔵 احتمال مبكر قبل الشمعة الذهبية",
            "EARLY": "🟡 استعداد للشمعة الذهبية",
            "ENTRY": "🟠 دخول الآن قبل التأكيد",
            "CONFIRM": "🔥 تأكيد الشمعة الذهبية",
        }
        side = "شراء" if signal.direction == "BUY" else "بيع"
        reasons = "\n".join(
            f"✅ {html.escape(reason)}"
            for reason in signal.reasons[:6]
        )
        timestamp = now_local().strftime("%d-%m-%Y %H:%M:%S")
        binance_url = f"https://www.binance.com/en/futures/{signal.symbol}"
        tradingview_url = f"https://www.tradingview.com/chart/?symbol=BINANCE:{signal.symbol}.P"

        return f"""<b>{titles[signal.stage]} — {side}</b>

💰 العملة: <b>#{signal.symbol}.P</b>
⏰ الفريم: <b>{signal.timeframe.upper()}</b>
💵 السعر: <b>{fmt_price(signal.price)}</b>

🟢 درجة الشراء: <b>{signal.score:.1f}%</b>
🔴 الدرجة المقابلة: <b>{signal.opposite_score:.1f}%</b>
📈 تغير الدرجة: <b>{signal.drift:+.1f}</b>
📊 توافق الفريمات: <b>{signal.mtf_count}/4</b>
📏 امتداد الحركة: <b>{signal.extension_atr:.2f} ATR</b>

🎯 الدخول: <b>{fmt_price(signal.entry_low)} – {fmt_price(signal.entry_high)}</b>
🛑 وقف الخسارة: <b>{fmt_price(signal.stop)}</b>
✅ TP1: <b>{fmt_price(signal.tp1)}</b> ({signal.rr1:.1f}R)
✅ TP2: <b>{fmt_price(signal.tp2)}</b> ({signal.rr2:.1f}R)
✅ TP3: <b>{fmt_price(signal.tp3)}</b> ({signal.rr3:.1f}R)

{reasons}

🕒 {timestamp} (السعودية)
🔗 <a href="{binance_url}">Binance</a> | <a href="{tradingview_url}">TradingView</a>

⚠️ الإشارة مبنية على محاكاة منطق المؤشر، وليست ضمانًا أو تنفيذًا تلقائيًا."""

    async def track_positions(self):
        while self.running:
            try:
                prices = await self.client.prices()
                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    rows = await (
                        await db.execute(
                            "SELECT * FROM signals WHERE status='OPEN' ORDER BY id DESC LIMIT 1000"
                        )
                    ).fetchall()

                    for row in rows:
                        price = prices.get(row["symbol"])
                        if price is None:
                            continue

                        direction = row["direction"]
                        best = row["best_price"] if row["best_price"] is not None else price
                        worst = row["worst_price"] if row["worst_price"] is not None else price

                        if direction == "BUY":
                            best = max(best, price)
                            worst = min(worst, price)
                            hit_tp1 = price >= row["tp1"]
                            hit_tp2 = price >= row["tp2"]
                            hit_tp3 = price >= row["tp3"]
                            hit_stop = price <= row["stop"]
                        else:
                            best = min(best, price)
                            worst = max(worst, price)
                            hit_tp1 = price <= row["tp1"]
                            hit_tp2 = price <= row["tp2"]
                            hit_tp3 = price <= row["tp3"]
                            hit_stop = price >= row["stop"]

                        updates = {
                            "best_price": best,
                            "worst_price": worst,
                        }
                        timestamp = now_local().isoformat()

                        if hit_tp1 and not row["tp1_at"]:
                            updates["tp1_at"] = timestamp
                        if hit_tp2 and not row["tp2_at"]:
                            updates["tp2_at"] = timestamp
                        if hit_tp3:
                            updates.update({
                                "tp3_at": row["tp3_at"] or timestamp,
                                "status": "CLOSED",
                                "outcome": "TP3",
                            })
                        elif hit_stop:
                            updates.update({
                                "stop_at": row["stop_at"] or timestamp,
                                "status": "CLOSED",
                                "outcome": "SL_AFTER_TP" if row["tp1_at"] or hit_tp1 else "SL",
                            })

                        set_clause = ", ".join(f"{key}=?" for key in updates)
                        await db.execute(
                            f"UPDATE signals SET {set_clause} WHERE id=?",
                            (*updates.values(), row["id"]),
                        )
                    await db.commit()

            except Exception:
                log.exception("Position tracker failed")

            await asyncio.sleep(60)


engine = Engine()


# =========================================================
# FastAPI
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.start()
    yield
    await engine.close()


app = FastAPI(title="Ahmed Golden Entry AI v1", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "ok": engine.last_error is None,
        "service": "Ahmed Golden Entry AI v1",
        "last_scan": engine.last_scan,
        "last_error": engine.last_error,
        "scan_number": engine.scan_number,
        "symbols": engine.symbol_count,
        "candidates": engine.candidate_count,
        "alerts_since_start": engine.alert_count,
        "thresholds": {
            "prep": PREP_SCORE,
            "early": EARLY_SCORE,
            "entry": ENTRY_SCORE,
            "gold": GOLD_SCORE,
        },
        "timeframes": TIMEFRAMES,
        "time": now_local().isoformat(),
    }


@app.get("/test-telegram")
async def test_telegram():
    if not ENABLE_MANUAL_TEST_ENDPOINT:
        return JSONResponse({"ok": False, "error": "disabled"}, status_code=403)

    ok = await engine.send_telegram(
        "🧪 <b>اختبار يدوي ناجح</b>\n\n"
        "✅ Ahmed Golden Entry AI متصل\n"
        f"🕒 {now_local().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)"
    )
    return {"ok": ok}


@app.get("/signals")
async def signals(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM signals ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/stats")
async def stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        overall = await (
            await db.execute(
                """SELECT COUNT(*) total,
                          SUM(status='OPEN') open_count,
                          SUM(outcome='TP3') tp3,
                          SUM(outcome='SL') sl,
                          SUM(outcome='SL_AFTER_TP') sl_after_tp
                   FROM signals"""
            )
        ).fetchone()

        groups = await (
            await db.execute(
                """SELECT timeframe,direction,stage,COUNT(*) cases,
                          SUM(outcome='TP3') tp3,
                          SUM(outcome='SL') sl,
                          SUM(outcome='SL_AFTER_TP') sl_after_tp
                   FROM signals
                   GROUP BY timeframe,direction,stage
                   ORDER BY cases DESC"""
            )
        ).fetchall()

    return {
        "overall": dict(overall),
        "groups": [dict(row) for row in groups],
    }


@app.get("/checkpoints")
async def checkpoints(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    status = "يعمل ✅" if engine.last_error is None else "خطأ ⚠️"
    return f"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ahmed Golden Entry AI</title>
<style>
body{{font-family:Arial;background:#0b1020;color:#eef2ff;margin:0;padding:24px}}
.wrap{{max-width:1050px;margin:auto}}
.card{{background:#151d34;border:1px solid #2b3658;border-radius:16px;padding:18px;margin:12px 0}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}}
.k{{color:#aab4d2;font-size:13px}} .v{{font-size:25px;font-weight:bold;margin-top:6px}}
a{{color:#8cb7ff}}
</style>
</head>
<body>
<div class="wrap">
<h1>Ahmed Golden Entry AI v1</h1>
<div class="card"><b>{status}</b><br>آخر فحص: {engine.last_scan or "لم يبدأ"}</div>
<div class="grid">
<div class="card"><div class="k">رقم الفحص</div><div class="v">{engine.scan_number}</div></div>
<div class="card"><div class="k">العقود</div><div class="v">{engine.symbol_count}</div></div>
<div class="card"><div class="k">المرشحون</div><div class="v">{engine.candidate_count}</div></div>
<div class="card"><div class="k">التنبيهات</div><div class="v">{engine.alert_count}</div></div>
</div>
<div class="card">
🔵 {PREP_SCORE:.0f} · 🟡 {EARLY_SCORE:.0f} · 🟠 {ENTRY_SCORE:.0f} · 🔥 {GOLD_SCORE:.0f}
</div>
<div class="card">
<a href="/health">Health</a> ·
<a href="/test-telegram">Test Telegram</a> ·
<a href="/signals">Signals</a> ·
<a href="/stats">Stats</a> ·
<a href="/checkpoints">Checkpoints</a>
</div>
</div>
</body>
</html>"""


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
