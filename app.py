import asyncio
import html
import json
import logging
import math
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# =========================================================
# Settings
# =========================================================

BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

PORT = int(os.getenv("PORT", "8080"))
TZ = ZoneInfo(os.getenv("TZ", "Asia/Riyadh"))

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "20"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "14"))
RADAR_POOL = int(os.getenv("RADAR_POOL", "120"))
DEEP_CANDIDATES = int(os.getenv("DEEP_CANDIDATES", "40"))
MAX_ALERTS_PER_SCAN = int(os.getenv("MAX_ALERTS_PER_SCAN", "3"))
MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "1000000"))

ENTRY_MODE = os.getenv("ENTRY_MODE", "BALANCED").strip().upper()
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "90"))
DIRECTION_GAP = float(os.getenv("DIRECTION_GAP", "7"))
MIN_RR_TP1 = float(os.getenv("MIN_RR_TP1", "1.0"))
MAX_EARLY_EXTENSION_ATR = float(os.getenv("MAX_EARLY_EXTENSION_ATR", "0.85"))
MAX_ENTRY_EXTENSION_ATR = float(os.getenv("MAX_ENTRY_EXTENSION_ATR", "0.60"))
MIN_DRIFT = float(os.getenv("MIN_DRIFT", "2.0"))

BINANCE_RETRIES = int(os.getenv("BINANCE_RETRIES", "4"))
SYMBOL_TIMEOUT = float(os.getenv("SYMBOL_TIMEOUT", "14"))
SCAN_TIMEOUT = float(os.getenv("SCAN_TIMEOUT", "55"))
EXCHANGE_CACHE_SECONDS = int(os.getenv("EXCHANGE_CACHE_SECONDS", "3600"))

SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_TEST_MESSAGE = os.getenv("SEND_TEST_MESSAGE", "true").lower() == "true"
ENABLE_MANUAL_TEST_ENDPOINT = os.getenv("ENABLE_MANUAL_TEST_ENDPOINT", "true").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "data/quantum_entry.db")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

MODE = {
    "AGGRESSIVE": {"WATCH": 54, "READY": 63, "ENTRY": 71, "factors": 2},
    "BALANCED": {"WATCH": 58, "READY": 67, "ENTRY": 75, "factors": 3},
    "CONSERVATIVE": {"WATCH": 63, "READY": 72, "ENTRY": 81, "factors": 4},
}.get(ENTRY_MODE, {"WATCH": 58, "READY": 67, "ENTRY": 75, "factors": 3})

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("quantum-entry")


# =========================================================
# Helpers
# =========================================================

def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default

def pct_change(a: float, b: float) -> float:
    return safe_div(a - b, abs(b), 0.0) * 100.0

def now_local() -> datetime:
    return datetime.now(TZ)

def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")

def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (length + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out

def stddev(values: list[float]) -> float:
    if not values:
        return 0.0
    m = sum(values) / len(values)
    return math.sqrt(sum((v - m) ** 2 for v in values) / len(values))

def atr(rows: list[list[Any]], length: int = 14) -> float:
    trs = []
    for i in range(1, len(rows)):
        high, low = float(rows[i][2]), float(rows[i][3])
        prev_close = float(rows[i - 1][4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-length:]) / max(1, min(length, len(trs)))

def unpack(rows: list[list[Any]]) -> dict[str, list[float]]:
    return {
        "o": [float(x[1]) for x in rows],
        "h": [float(x[2]) for x in rows],
        "l": [float(x[3]) for x in rows],
        "c": [float(x[4]) for x in rows],
        "v": [float(x[5]) for x in rows],
        "tb": [float(x[9]) for x in rows],
    }

def vwap(rows: list[list[Any]]) -> float:
    pv = vol = 0.0
    for x in rows:
        high, low, close, volume = float(x[2]), float(x[3]), float(x[4]), float(x[5])
        pv += ((high + low + close) / 3.0) * volume
        vol += volume
    return safe_div(pv, vol, float(rows[-1][4]) if rows else 0.0)


# =========================================================
# Data models
# =========================================================

@dataclass
class Signal:
    symbol: str
    direction: str
    stage: str
    engine: str
    score: float
    timing: float
    opportunity: float
    price: float
    entry_low: float
    entry_high: float
    stop: float
    tp1: float
    tp2: float
    tp3: float
    rr1: float
    rr2: float
    rr3: float
    factors: list[str]
    details: dict[str, Any]


# =========================================================
# Database
# =========================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    engine TEXT NOT NULL,
    current_stage TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    price REAL,
    score REAL,
    timing REAL,
    opportunity REAL,
    entry_low REAL,
    entry_high REAL,
    stop REAL,
    tp1 REAL,
    tp2 REAL,
    tp3 REAL,
    rr1 REAL,
    rr2 REAL,
    rr3 REAL,
    factors_json TEXT,
    details_json TEXT,
    outcome TEXT
);
CREATE INDEX IF NOT EXISTS idx_opp_open
ON opportunities(status, symbol, direction, engine);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    scan_no INTEGER,
    symbols INTEGER,
    candidates INTEGER,
    analyzed INTEGER,
    found INTEGER,
    sent INTEGER,
    seconds REAL,
    error TEXT
);

CREATE TABLE IF NOT EXISTS rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    reason TEXT NOT NULL,
    details_json TEXT
);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()

async def reject(symbol: str, reason: str, details: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO rejections(created_at,symbol,reason,details_json) VALUES(?,?,?,?)",
            (now_local().isoformat(), symbol, reason, json.dumps(details)),
        )
        await db.commit()

async def get_open(symbol: str, direction: str, engine: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        return await (await db.execute(
            """SELECT * FROM opportunities
               WHERE status='OPEN' AND symbol=? AND direction=? AND engine=?
               ORDER BY id DESC LIMIT 1""",
            (symbol, direction, engine),
        )).fetchone()

async def save_signal(sig: Signal) -> bool:
    existing = await get_open(sig.symbol, sig.direction, sig.engine)
    stage_rank = {"WATCH": 1, "READY": 2, "ENTRY": 3}
    ts = now_local().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        if existing:
            last_update = datetime.fromisoformat(existing["updated_at"])
            if stage_rank[sig.stage] <= stage_rank[existing["current_stage"]]:
                return False
            if now_local() - last_update < timedelta(minutes=2):
                return False
            await db.execute(
                """UPDATE opportunities SET
                current_stage=?,updated_at=?,price=?,score=?,timing=?,opportunity=?,
                entry_low=?,entry_high=?,stop=?,tp1=?,tp2=?,tp3=?,rr1=?,rr2=?,rr3=?,
                factors_json=?,details_json=? WHERE id=?""",
                (
                    sig.stage, ts, sig.price, sig.score, sig.timing, sig.opportunity,
                    sig.entry_low, sig.entry_high, sig.stop, sig.tp1, sig.tp2, sig.tp3,
                    sig.rr1, sig.rr2, sig.rr3, json.dumps(sig.factors),
                    json.dumps(sig.details), existing["id"],
                ),
            )
            await db.commit()
            return True

        # symbol-level cooldown across engines
        row = await (await db.execute(
            """SELECT updated_at FROM opportunities
               WHERE symbol=? ORDER BY id DESC LIMIT 1""",
            (sig.symbol,),
        )).fetchone()
        if row:
            last = datetime.fromisoformat(row[0])
            if now_local() - last < timedelta(minutes=COOLDOWN_MINUTES):
                return False

        await db.execute(
            """INSERT INTO opportunities(
            symbol,direction,engine,current_stage,status,created_at,updated_at,
            price,score,timing,opportunity,entry_low,entry_high,stop,tp1,tp2,tp3,
            rr1,rr2,rr3,factors_json,details_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                sig.symbol, sig.direction, sig.engine, sig.stage, "OPEN", ts, ts,
                sig.price, sig.score, sig.timing, sig.opportunity,
                sig.entry_low, sig.entry_high, sig.stop, sig.tp1, sig.tp2, sig.tp3,
                sig.rr1, sig.rr2, sig.rr3,
                json.dumps(sig.factors), json.dumps(sig.details),
            ),
        )
        await db.commit()
        return True


# =========================================================
# Binance
# =========================================================

class Binance:
    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self.exchange_cache: tuple[float, list[str]] | None = None
        self.ok = False
        self.last_error = None

    async def start(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            connector=aiohttp.TCPConnector(limit=MAX_CONCURRENCY * 2, ttl_dns_cache=300),
        )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def get(self, path: str, params=None):
        assert self.session
        last = None
        async with self.sem:
            for attempt in range(BINANCE_RETRIES):
                try:
                    async with self.session.get(BINANCE_BASE + path, params=params) as r:
                        if r.status in (418, 429):
                            await asyncio.sleep(min(2 ** attempt + 1, 10))
                            continue
                        r.raise_for_status()
                        data = await r.json()
                        if data is None:
                            raise RuntimeError("null response")
                        self.ok = True
                        self.last_error = None
                        return data
                except Exception as exc:
                    last = exc
                    self.last_error = repr(exc)
                    await asyncio.sleep(min(1.5 ** attempt, 6))
        self.ok = False
        raise RuntimeError(f"Binance request failed {path}: {last!r}")

    async def symbols(self):
        if self.exchange_cache and time.time() - self.exchange_cache[0] < EXCHANGE_CACHE_SECONDS:
            return self.exchange_cache[1]
        data = await self.get("/fapi/v1/exchangeInfo")
        items = data.get("symbols") if isinstance(data, dict) else None
        if not isinstance(items, list):
            raise RuntimeError("invalid exchangeInfo")
        symbols = [
            x["symbol"] for x in items
            if x.get("status") == "TRADING"
            and x.get("contractType") == "PERPETUAL"
            and x.get("quoteAsset") == "USDT"
        ]
        self.exchange_cache = (time.time(), symbols)
        return symbols

    async def tickers(self):
        data = await self.get("/fapi/v1/ticker/24hr")
        if not isinstance(data, list):
            raise RuntimeError("invalid tickers")
        return data

    async def klines(self, symbol, interval, limit=100):
        data = await self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        if not isinstance(data, list) or len(data) < 30:
            raise RuntimeError(f"invalid klines {symbol} {interval}")
        return data

    async def oi(self, symbol):
        data = await self.get("/futures/data/openInterestHist", {"symbol": symbol, "period": "5m", "limit": 12})
        return data if isinstance(data, list) else []

    async def depth(self, symbol):
        data = await self.get("/fapi/v1/depth", {"symbol": symbol, "limit": 100})
        return data if isinstance(data, dict) else {}

    async def premium(self, symbol):
        data = await self.get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return data if isinstance(data, dict) else {}


# =========================================================
# Analysis
# =========================================================

def radar_score(t: dict, prev: dict | None) -> tuple[float, dict]:
    price = float(t.get("lastPrice", 0) or 0)
    qv = float(t.get("quoteVolume", 0) or 0)
    trades = float(t.get("count", 0) or 0)
    price_burst = vol_burst = trade_burst = 0.0
    if prev:
        price_burst = abs(pct_change(price, prev["price"]))
        vol_burst = max(0.0, pct_change(qv, prev["qv"]))
        trade_burst = max(0.0, pct_change(trades, prev["trades"]))
    liquidity = clamp((math.log10(max(qv, 1)) - 5.2) * 22)
    score = clamp(
        0.35 * clamp(price_burst * 1200)
        + 0.25 * clamp(vol_burst * 10)
        + 0.20 * clamp(trade_burst * 8)
        + 0.20 * liquidity
    )
    return score, {"price": price, "qv": qv, "trades": trades}

def micro(rows, direction):
    d = unpack(rows)
    sign = 1 if direction == "BUY" else -1
    a = atr(rows)
    price = d["c"][-1]
    deltas = [2 * tb - v for tb, v in zip(d["tb"], d["v"])]
    delta_now = sum(deltas[-2:]) * sign
    delta_prev = sum(deltas[-6:-2]) * sign
    cvd_now = sum(deltas[-12:]) * sign
    cvd_prev = sum(deltas[-24:-12]) * sign
    rh, rl = max(d["h"][-8:-1]), min(d["l"][-8:-1])

    if direction == "BUY":
        sweep = d["l"][-1] < rl and d["c"][-1] > rl
        reject = (min(d["o"][-1], d["c"][-1]) - d["l"][-1]) >= (d["h"][-1] - d["l"][-1]) * 0.40
        bos = d["c"][-1] > max(d["h"][-4:-1])
        pivot = min(d["l"][-8:])
        ext = safe_div(price - pivot, a, 99)
    else:
        sweep = d["h"][-1] > rh and d["c"][-1] < rh
        reject = (d["h"][-1] - max(d["o"][-1], d["c"][-1])) >= (d["h"][-1] - d["l"][-1]) * 0.40
        bos = d["c"][-1] < min(d["l"][-4:-1])
        pivot = max(d["h"][-8:])
        ext = safe_div(pivot - price, a, 99)

    avg_vol = sum(d["v"][-20:-1]) / 19
    vr = safe_div(d["v"][-1], avg_vol, 1)
    return {
        "price": price, "atr": a, "sweep": sweep, "reject": reject, "bos": bos,
        "ext": ext, "volume": vr,
        "delta": clamp(50 + safe_div(delta_now, max(sum(d["v"][-2:]), 1), 0) * 300),
        "delta_accel": clamp(50 + safe_div(delta_now - delta_prev, max(sum(d["v"][-6:]), 1), 0) * 330),
        "cvd": clamp(50 + safe_div(cvd_now, max(sum(d["v"][-12:]), 1), 0) * 280),
        "cvd_shift": clamp(50 + safe_div(cvd_now - cvd_prev, max(sum(d["v"][-24:]), 1), 0) * 320),
    }

def context(rows, direction):
    d = unpack(rows)
    sign = 1 if direction == "BUY" else -1
    a = atr(rows)
    price = d["c"][-1]
    e9, e21 = ema(d["c"], 9)[-1], ema(d["c"], 21)[-1]
    trend = clamp(50 + pct_change(e9, e21) * sign * 18)
    recent_range = max(d["h"][-8:]) - min(d["l"][-8:])
    compression = clamp((2.4 - safe_div(recent_range, a, 0)) / 1.8 * 100)
    basis = sum(d["c"][-20:]) / 20
    dev = stddev(d["c"][-20:])
    upper, lower = basis + 2 * dev, basis - 2 * dev
    vw = vwap(rows[-48:])
    if direction == "BUY":
        price_context = price >= vw or d["l"][-1] <= lower
    else:
        price_context = price <= vw or d["h"][-1] >= upper
    return {
        "atr": a, "trend": trend, "compression": compression,
        "price_context": price_context,
        "swing_low": min(d["l"][-20:]), "swing_high": max(d["h"][-20:]),
    }

def oi_features(rows):
    if len(rows) < 3:
        return {"change": 0.0, "accel": 50.0}
    vals = [float(x.get("sumOpenInterest", 0) or 0) for x in rows]
    change = pct_change(vals[-1], vals[-4] if len(vals) >= 4 else vals[0])
    recent = pct_change(vals[-1], vals[-2])
    prev = pct_change(vals[-2], vals[-3])
    return {"change": change, "accel": clamp(50 + (recent - prev) * 18)}

def book(depth, direction):
    bids = [(float(p), float(q)) for p, q in depth.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in depth.get("asks", [])]
    bn = sum(p * q for p, q in bids[:30])
    an = sum(p * q for p, q in asks[:30])
    raw = safe_div(bn - an, bn + an, 0)
    signed = raw if direction == "BUY" else -raw
    side = [q for _, q in (bids[:50] if direction == "BUY" else asks[:50])]
    avg = sum(side) / max(1, len(side))
    wall = safe_div(max(side, default=0), avg, 0)
    return {
        "imbalance": clamp(50 + signed * 180),
        "absorption": clamp(35 + max(0, wall - 2) * 12),
        "spoof": clamp(max(0, wall - 8) * 14),
    }

def detect_zone(rows, direction):
    d = unpack(rows)
    a = atr(rows)
    best = None
    for i in range(max(3, len(d["c"]) - 24), len(d["c"]) - 3):
        body = abs(d["c"][i + 1] - d["o"][i + 1])
        avg_body = sum(abs(d["c"][j] - d["o"][j]) for j in range(max(0, i - 8), i + 1)) / max(1, min(9, i + 1))
        if body < max(avg_body * 1.5, a * 0.35):
            continue
        if direction == "BUY":
            valid = d["c"][i] < d["o"][i] and d["c"][i + 1] > d["h"][i]
        else:
            valid = d["c"][i] > d["o"][i] and d["c"][i + 1] < d["l"][i]
        if valid:
            vr = safe_div(d["v"][i], sum(d["v"][max(0, i - 10):i]) / max(1, min(10, i)), 1)
            best = {"active": True, "low": d["l"][i], "high": d["h"][i], "strength": clamp(55 + vr * 12)}
    return best or {"active": False, "low": 0.0, "high": 0.0, "strength": 0.0}

def in_zone(price, zone, a):
    if not zone["active"]:
        return False
    b = a * 0.18
    return zone["low"] - b <= price <= zone["high"] + b

def plan(direction, price, a, swing_low, swing_high, zone=None):
    if zone and zone.get("active"):
        zlow, zhigh = zone["low"], zone["high"]
    else:
        zlow, zhigh = price - a * 0.18, price + a * 0.18
    if direction == "BUY":
        entry_low, entry_high = min(price, zhigh), max(price, zhigh + a * 0.06)
        stop = min(swing_low, zlow) - a * 0.18
        mid = (entry_low + entry_high) / 2
        risk = max(mid - stop, a * 0.55)
        tps = (mid + risk, mid + 2 * risk, mid + 3 * risk)
    else:
        entry_low, entry_high = min(price, zlow - a * 0.06), max(price, zlow)
        stop = max(swing_high, zhigh) + a * 0.18
        mid = (entry_low + entry_high) / 2
        risk = max(stop - mid, a * 0.55)
        tps = (mid - risk, mid - 2 * risk, mid - 3 * risk)
    rrs = tuple(abs(tp - mid) / max(abs(mid - stop), 1e-12) for tp in tps)
    return entry_low, entry_high, stop, *tps, *rrs

def stage(score, timing, ext, factors):
    if factors < MODE["factors"]:
        return None
    if score >= MODE["ENTRY"] and timing >= 68 and ext <= MAX_ENTRY_EXTENSION_ATR:
        return "ENTRY"
    if score >= MODE["READY"] and timing >= 54 and ext <= MAX_EARLY_EXTENSION_ATR:
        return "READY"
    if score >= MODE["WATCH"] and ext <= MAX_EARLY_EXTENSION_ATR:
        return "WATCH"
    return None

def message(sig: Signal):
    title = {
        "WATCH": "🟡 مراقبة ما قبل الانفجار",
        "READY": "🟠 دخول مبكر قبل الانفجار",
        "ENTRY": "🔥 دخول الآن — بداية الانطلاق",
    }[sig.stage]
    side = "شراء" if sig.direction == "BUY" else "بيع"
    checks = "\n".join(f"✅ {html.escape(x)}" for x in sig.factors[:7])
    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{sig.symbol}.P"
    bn = f"https://www.binance.com/en/futures/{sig.symbol}"
    return f"""<b>{title} — {side}</b>

💰 <b>#{sig.symbol}.P</b>
🧠 المحرك: <b>{sig.engine}</b>
💵 السعر: <b>{fmt_price(sig.price)}</b>

⚡ الدرجة: <b>{sig.score:.1f}%</b>
⏱️ التوقيت: <b>{sig.timing:.1f}%</b>
🎯 جدوى الفرصة: <b>{sig.opportunity:.1f}%</b>

🎯 الدخول: <b>{fmt_price(sig.entry_low)} – {fmt_price(sig.entry_high)}</b>
🛑 الإبطال: <b>{fmt_price(sig.stop)}</b>
✅ TP1: <b>{fmt_price(sig.tp1)}</b> ({sig.rr1:.1f}R)
✅ TP2: <b>{fmt_price(sig.tp2)}</b> ({sig.rr2:.1f}R)
✅ TP3: <b>{fmt_price(sig.tp3)}</b> ({sig.rr3:.1f}R)

{checks}

📏 الامتداد: <b>{sig.details.get("ext", 0):.2f} ATR</b>
📈 تغير الدرجة: <b>{sig.details.get("drift", 0):+.1f}</b>

🕒 {now_local().strftime("%d-%m-%Y %H:%M:%S")} (السعودية)
🔗 <a href="{bn}">Binance</a> | <a href="{tv}">TradingView</a>

⚠️ خطة احتمالية وليست ضمانًا أو تنفيذًا تلقائيًا."""


# =========================================================
# Engine
# =========================================================

class Engine:
    def __init__(self):
        self.b = Binance()
        self.tg: aiohttp.ClientSession | None = None
        self.running = True
        self.scan_no = 0
        self.last_scan = None
        self.last_error = None
        self.symbols_count = 0
        self.candidates_count = 0
        self.alerts_count = 0
        self.fast_state = {}
        self.score_history: dict[tuple[str, str, str], deque] = {}
        self.pipeline = {"eligible": 0, "deep": 0, "analyzed": 0, "found": 0, "sent": 0}

    async def start(self):
        await init_db()
        await self.b.start()
        self.tg = aiohttp.ClientSession()
        if SEND_STARTUP_MESSAGE:
            await self.send("✅ <b>Ahmed Quantum Entry AI بدأ العمل</b>\n\n"
                            f"🎛️ الوضع: <b>{ENTRY_MODE}</b>\n"
                            "🧠 Pre-Explosion + Order Flow First-Reaction\n"
                            f"📨 الحد الأعلى لكل دورة: {MAX_ALERTS_PER_SCAN}\n"
                            "⚠️ لا ينفذ صفقات تلقائيًا.")
        if SEND_TEST_MESSAGE:
            await self.send("🧪 <b>رسالة اختبار ناجحة</b>\n\n✅ Telegram متصل\n✅ Railway يعمل\n✅ قاعدة البيانات جاهزة")
        asyncio.create_task(self.loop())

    async def close(self):
        self.running = False
        await self.b.close()
        if self.tg:
            await self.tg.close()

    async def send(self, text):
        if not BOT_TOKEN or not CHAT_ID or not self.tg:
            return False
        try:
            async with self.tg.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=20,
            ) as r:
                if r.status != 200:
                    log.error("Telegram %s: %s", r.status, await r.text())
                    return False
                return True
        except Exception:
            log.exception("Telegram failed")
            return False

    async def loop(self):
        while self.running:
            started = time.monotonic()
            self.scan_no += 1
            error = None
            sent = analyzed = 0
            try:
                sent, analyzed = await asyncio.wait_for(self.scan(), timeout=SCAN_TIMEOUT)
                self.last_error = None
            except Exception as exc:
                error = repr(exc)
                self.last_error = error
                log.exception("scan failed")
            elapsed = time.monotonic() - started
            self.last_scan = now_local().isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    """INSERT INTO checkpoints(created_at,scan_no,symbols,candidates,analyzed,found,sent,seconds,error)
                    VALUES(?,?,?,?,?,?,?,?,?)""",
                    (now_local().isoformat(), self.scan_no, self.symbols_count, self.candidates_count,
                     analyzed, self.pipeline["found"], sent, elapsed, error),
                )
                await db.commit()
            log.info("scan=%s symbols=%s candidates=%s analyzed=%s found=%s sent=%s seconds=%.1f",
                     self.scan_no, self.symbols_count, self.candidates_count, analyzed,
                     self.pipeline["found"], sent, elapsed)
            await asyncio.sleep(max(5, SCAN_SECONDS - elapsed))

    async def scan(self):
        symbols, tickers = await asyncio.gather(self.b.symbols(), self.b.tickers())
        self.symbols_count = len(symbols)
        allowed = set(symbols)
        ranked = []
        for t in tickers:
            if not isinstance(t, dict):
                continue
            symbol = t.get("symbol")
            if symbol not in allowed:
                continue
            qv = float(t.get("quoteVolume", 0) or 0)
            if qv < MIN_QUOTE_VOLUME_USDT:
                continue
            s, st = radar_score(t, self.fast_state.get(symbol))
            self.fast_state[symbol] = st
            ranked.append((s, qv, symbol))
        ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
        selected = ranked[:RADAR_POOL][:DEEP_CANDIDATES]
        symbols_to_scan = [x[2] for x in selected]
        self.candidates_count = len(symbols_to_scan)

        async def guarded(symbol):
            try:
                return await asyncio.wait_for(self.analyze(symbol), timeout=SYMBOL_TIMEOUT)
            except Exception as exc:
                await reject(symbol, "ANALYSIS_ERROR", {"error": repr(exc)})
                return []

        results = await asyncio.gather(*(guarded(s) for s in symbols_to_scan))
        analyzed = len(results)
        all_signals = [sig for group in results for sig in group]
        all_signals.sort(
            key=lambda s: (
                {"WATCH": 1, "READY": 2, "ENTRY": 3}[s.stage],
                s.opportunity,
                s.timing,
            ),
            reverse=True,
        )

        # only strongest N signals per scan
        sent = 0
        used_symbols = set()
        for sig in all_signals:
            if sent >= MAX_ALERTS_PER_SCAN:
                break
            if sig.symbol in used_symbols:
                continue
            if await save_signal(sig):
                if await self.send(message(sig)):
                    sent += 1
                    self.alerts_count += 1
                    used_symbols.add(sig.symbol)

        self.pipeline = {
            "eligible": len(ranked),
            "deep": len(symbols_to_scan),
            "analyzed": analyzed,
            "found": len(all_signals),
            "sent": sent,
        }
        return sent, analyzed

    async def analyze(self, symbol):
        try:
            k1m, k3m, k15, k1h, k4h, oirows, depth, premium = await asyncio.gather(
                self.b.klines(symbol, "1m"),
                self.b.klines(symbol, "3m"),
                self.b.klines(symbol, "15m"),
                self.b.klines(symbol, "1h"),
                self.b.klines(symbol, "4h"),
                self.b.oi(symbol),
                self.b.depth(symbol),
                self.b.premium(symbol),
            )
        except Exception as exc:
            await reject(symbol, "DATA_FETCH", {"error": repr(exc)})
            return []

        candidates = []

        for direction in ("BUY", "SELL"):
            m1, m3 = micro(k1m, direction), micro(k3m, direction)
            c15, c1h, c4h = context(k15, direction), context(k1h, direction), context(k4h, direction)
            oi = oi_features(oirows)
            ob = book(depth, direction)
            funding = float(premium.get("lastFundingRate", 0) or 0) * 100
            funding_support = clamp(50 + ((-funding) if direction == "BUY" else funding) * 900)

            position = clamp(48 + max(0, oi["change"]) * 9 + (oi["accel"] - 50) * 0.35 + (funding_support - 50) * 0.15)
            execution = clamp(0.30*m1["delta_accel"] + 0.18*m3["delta_accel"] + 0.20*m1["cvd_shift"] + 0.14*ob["imbalance"] + 0.18*ob["absorption"])
            liquidity = clamp(0.50*ob["imbalance"] + 0.30*ob["absorption"] + 0.20*(100-ob["spoof"]))
            price_pressure = clamp(0.34*c15["compression"] + 0.20*(72 if c15["price_context"] else 42) + 0.18*m1["delta"] + 0.16*(72 if m1["volume"] >= 1.2 else 42) + 0.12*c1h["trend"])
            timing = clamp(0.28*(80 if m1["bos"] else 44) + 0.18*(76 if m1["reject"] else 42) + 0.14*(76 if m1["sweep"] else 42) + 0.20*m1["delta_accel"] + 0.20*(72 if m1["volume"] >= 1.2 else 42))
            score = clamp(0.18*position + 0.28*execution + 0.18*liquidity + 0.20*price_pressure + 0.16*timing)

            factors = []
            if oi["change"] > 0.10: factors.append(f"OI يرتفع {oi['change']:+.2f}%")
            if m1["delta_accel"] >= 58: factors.append("Delta يتسارع")
            if m1["cvd_shift"] >= 58: factors.append("CVD يتحول قبل السعر")
            if ob["imbalance"] >= 57: factors.append("دفتر الأوامر داعم")
            if ob["absorption"] >= 55: factors.append("Absorption محتمل")
            if c15["compression"] >= 55: factors.append("ضغط سعري")
            if m1["volume"] >= 1.2: factors.append("توسع حجم")
            if m1["bos"]: factors.append("أول كسر بنية صغير")

            key = (symbol, direction, "PRE_EXPLOSION")
            hist = self.score_history.setdefault(key, deque(maxlen=12))
            prev = hist[-1] if hist else score
            hist.append(score)
            drift = score - prev

            st = stage(score, timing, m1["ext"], len(factors))
            opp = clamp(0.52*score + 0.30*timing + 0.18*min(100, 55 + max(0, 1.3-m1["ext"])*28))
            if st and (st != "ENTRY" or drift >= MIN_DRIFT):
                p = plan(direction, m1["price"], max(m1["atr"], m3["atr"]), c15["swing_low"], c15["swing_high"])
                if p[6] >= MIN_RR_TP1:
                    candidates.append(Signal(symbol,direction,st,"PRE_EXPLOSION",score,timing,opp,m1["price"],*p,factors,{"ext":m1["ext"],"drift":drift}))

            # Order Flow first reaction
            zones = [("15M", detect_zone(k15, direction)), ("1H", detect_zone(k1h, direction)), ("4H", detect_zone(k4h, direction))]
            active = [(tf,z) for tf,z in zones if in_zone(m1["price"], z, m1["atr"])]
            if active:
                tf, z = max(active, key=lambda x: x[1]["strength"])
                rscore = clamp(0.24*z["strength"] + 0.22*m1["delta_accel"] + 0.18*m1["cvd_shift"] + 0.14*ob["imbalance"] + 0.10*ob["absorption"] + 0.12*timing)
                rf = [f"منطقة Order Flow {tf}"]
                if m1["reject"]: rf.append("رفض سعري")
                if m1["sweep"]: rf.append("سحب سيولة")
                if m1["delta_accel"] >= 56: rf.append("Delta بدأ ينقلب")
                if m1["cvd_shift"] >= 56: rf.append("CVD بدأ يتحول")
                if ob["absorption"] >= 55: rf.append("امتصاص")
                if m1["bos"]: rf.append("أول حركة من المنطقة")
                rtiming = clamp(0.36*timing + 0.22*m1["delta_accel"] + 0.18*(80 if m1["reject"] else 42) + 0.14*(80 if m1["bos"] else 42) + 0.10*(100-min(100,m1["ext"]*100)))

                key = (symbol, direction, "ORDER_FLOW_REACTION")
                hist = self.score_history.setdefault(key, deque(maxlen=12))
                prev = hist[-1] if hist else rscore
                hist.append(rscore)
                drift = rscore - prev

                st = stage(rscore, rtiming, m1["ext"], max(0, len(rf)-1))
                ropp = clamp(0.50*rscore + 0.32*rtiming + 0.18*z["strength"])
                if st and (st != "ENTRY" or drift >= MIN_DRIFT):
                    p = plan(direction, m1["price"], max(m1["atr"], m3["atr"]), c15["swing_low"], c15["swing_high"], z)
                    if p[6] >= MIN_RR_TP1:
                        candidates.append(Signal(symbol,direction,st,"ORDER_FLOW_FIRST_REACTION",rscore,rtiming,ropp,m1["price"],*p,rf,{"ext":m1["ext"],"drift":drift,"zone_tf":tf}))

        if not candidates:
            return []
        candidates.sort(key=lambda s: ({"WATCH":1,"READY":2,"ENTRY":3}[s.stage], s.opportunity, s.timing), reverse=True)
        best = candidates[0]
        if len(candidates) > 1 and best.direction != candidates[1].direction and best.opportunity - candidates[1].opportunity < DIRECTION_GAP:
            await reject(symbol, "DIRECTION_CONFLICT", {"a":best.opportunity,"b":candidates[1].opportunity})
            return []
        return [best]


engine = Engine()

@asynccontextmanager
async def lifespan(app):
    await engine.start()
    yield
    await engine.close()

app = FastAPI(title="Ahmed Quantum Entry AI", lifespan=lifespan)

@app.get("/health")
async def health():
    return {
        "ok": engine.last_error is None and engine.b.ok,
        "service": "Ahmed Quantum Entry AI",
        "entry_mode": ENTRY_MODE,
        "scan_no": engine.scan_no,
        "last_scan": engine.last_scan,
        "last_error": engine.last_error,
        "symbols": engine.symbols_count,
        "candidates": engine.candidates_count,
        "alerts_since_start": engine.alerts_count,
        "pipeline": engine.pipeline,
        "binance": {"ok": engine.b.ok, "last_error": engine.b.last_error},
        "time": now_local().isoformat(),
    }

@app.get("/test-telegram")
async def test_telegram():
    if not ENABLE_MANUAL_TEST_ENDPOINT:
        return JSONResponse({"ok": False, "error": "disabled"}, status_code=403)
    ok = await engine.send("🧪 <b>اختبار يدوي ناجح</b>\n\n✅ Ahmed Quantum Entry AI متصل")
    return {"ok": ok}

@app.get("/opportunities")
async def opportunities(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute("SELECT * FROM opportunities ORDER BY id DESC LIMIT ?", (limit,))).fetchall()
    return [dict(x) for x in rows]

@app.get("/checkpoints")
async def checkpoints(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute("SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?", (limit,))).fetchall()
    return [dict(x) for x in rows]

@app.get("/stats")
async def stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        overall = await (await db.execute("SELECT COUNT(*) total, SUM(status='OPEN') open_count FROM opportunities")).fetchone()
        groups = await (await db.execute("SELECT engine,direction,current_stage,COUNT(*) cases FROM opportunities GROUP BY engine,direction,current_stage")).fetchall()
    return {"overall": dict(overall), "groups": [dict(x) for x in groups]}

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    h = await health()
    p = h["pipeline"]
    return f"""<!doctype html><html lang="ar" dir="rtl"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Ahmed Quantum Entry AI</title>
    <style>body{{font-family:Arial;background:#0b1020;color:#eef2ff;padding:24px}}
    .wrap{{max-width:1000px;margin:auto}}.card{{background:#161e34;border:1px solid #2b3658;border-radius:16px;padding:18px;margin:12px 0}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}}
    .v{{font-size:24px;font-weight:bold}}a{{color:#8db9ff}}</style></head><body><div class="wrap">
    <h1>Ahmed Quantum Entry AI</h1>
    <div class="card">الحالة: {'✅ يعمل' if h['ok'] else '⚠️ يوجد خطأ'}<br>الوضع: {ENTRY_MODE}<br>آخر فحص: {h['last_scan']}</div>
    <div class="grid">
    <div class="card"><div>العقود</div><div class="v">{h['symbols']}</div></div>
    <div class="card"><div>المرشحون</div><div class="v">{h['candidates']}</div></div>
    <div class="card"><div>التنبيهات</div><div class="v">{h['alerts_since_start']}</div></div>
    <div class="card"><div>رقم الفحص</div><div class="v">{h['scan_no']}</div></div>
    </div>
    <div class="card">المسار: {p['eligible']} مؤهل ← {p['deep']} تحليل عميق ← {p['found']} فرصة ← {p['sent']} مرسل</div>
    <div class="card"><a href="/health">Health</a> · <a href="/test-telegram">Test Telegram</a> · <a href="/opportunities">Opportunities</a> · <a href="/stats">Stats</a> · <a href="/checkpoints">Checkpoints</a></div>
    </div></body></html>"""

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
