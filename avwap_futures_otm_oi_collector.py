"""Railway worker: all NSE stock futures, 3-minute AVWAP crossing scanner.

The worker subscribes before 09:15 IST, builds completed 3-minute futures
candles from cumulative-volume ticks, freezes the 09:15-10:15 AVWAP values,
and persists crossings of the continuing 3-minute AVWAP against those levels.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import requests
import upstox_client


IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")
MARKET_OPEN = dt_time(9, 15)
FREEZE_TIME = dt_time(10, 15)
MARKET_CLOSE = dt_time(15, 30)
BAR_MINUTES = 3
OI_BASELINE_TIME = dt_time(9, 20)
OI_BASELINE_LAST_RETRY = dt_time(9, 25)
OI_STRONG_THRESHOLD = float(os.getenv("AVWAP_OI_STRONG_THRESHOLD", "15"))
OI_VERY_STRONG_THRESHOLD = float(os.getenv("AVWAP_OI_VERY_STRONG_THRESHOLD", "20"))
FULL_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"
DEFAULT_INSTRUMENTS_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
)
INDEX_NAMES = {
    "NIFTY", "NIFTY 50", "BANKNIFTY", "NIFTY BANK", "FINNIFTY",
    "NIFTY FIN SERVICE", "MIDCPNIFTY", "NIFTY MID SELECT", "SENSEX",
    "BANKEX",
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("avwap-futures")


def env(name: str, *fallbacks: str) -> str:
    for key in (name, *fallbacks):
        value = os.getenv(key)
        if value:
            return value.strip()
    return ""


DATABASE_URL = env("NEON_DATABASE_URL", "DATABASE_URL")
ACCESS_TOKEN = env("UPSTOX_ACCESS_TOKEN", "UPSTOX_TOKEN")
INSTRUMENTS_URL = env("UPSTOX_INSTRUMENTS_URL") or DEFAULT_INSTRUMENTS_URL

# NSE F&O holidays for calendar year 2026.
# Extra/future dates can be added through:
# NSE_TRADING_HOLIDAYS=YYYY-MM-DD,YYYY-MM-DD,...
NSE_FO_HOLIDAYS_2026 = {
    "2026-01-26","2026-03-03","2026-03-26","2026-03-31","2026-04-03",
    "2026-04-14","2026-05-01","2026-05-28","2026-06-26","2026-09-14",
    "2026-10-02","2026-10-20","2026-11-10","2026-11-24","2026-12-25",
}

def configured_holidays() -> set[str]:
    dates = set(NSE_FO_HOLIDAYS_2026)
    raw = os.getenv("NSE_TRADING_HOLIDAYS", "").strip()
    if raw:
        dates.update(x.strip() for x in raw.split(",") if x.strip())
    return dates

NSE_TRADING_HOLIDAYS = configured_holidays()

def is_nse_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day.isoformat() not in NSE_TRADING_HOLIDAYS

def holiday_reason(day: date) -> str | None:
    if day.weekday() >= 5:
        return "WEEKEND"
    if day.isoformat() in NSE_TRADING_HOLIDAYS:
        return "NSE F&O TRADING HOLIDAY"
    return None

def stop_if_non_trading_day() -> None:
    today = datetime.now(IST).date()
    reason = holiday_reason(today)
    if reason:
        LOG.info("%s: %s | collector will not start.", reason, today.isoformat())
        raise SystemExit(0)



@dataclass(frozen=True)
class FutureInstrument:
    symbol: str
    trading_symbol: str
    instrument_key: str
    expiry: date


@dataclass(frozen=True)
class OptionInstrument:
    symbol: str
    option_type: str
    strike: float
    instrument_key: str
    expiry: date
    trading_symbol: str


@dataclass
class LiveBar:
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    cumulative_volume: int | None = None


@dataclass
class SymbolState:
    instrument: FutureInstrument
    current: LiveBar | None = None
    last_cumulative_volume: int | None = None
    cum_volume: int = 0
    cum_high_volume: float = 0.0
    cum_low_volume: float = 0.0
    previous_avwap_high: float | None = None
    previous_avwap_low: float | None = None
    frozen_hour_avwap_high: float | None = None
    frozen_hour_avwap_low: float | None = None
    first_hour_high: float | None = None
    first_hour_low: float | None = None
    first_hour_volume: int = 0
    first_tick_seen: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


DDL = """
CREATE TABLE IF NOT EXISTS public.avwap_futures_3m (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    trading_symbol TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    expiry DATE NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    candle_end TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume BIGINT NOT NULL,
    cumulative_volume BIGINT,
    avwap_high NUMERIC,
    avwap_low NUMERIC,
    hourly_avwap_high NUMERIC,
    hourly_avwap_low NUMERIC,
    high_cross TEXT,
    low_cross TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trading_date, instrument_key, candle_start)
);

CREATE INDEX IF NOT EXISTS idx_avwap_futures_latest
    ON public.avwap_futures_3m (trading_date, symbol, candle_start DESC);
CREATE INDEX IF NOT EXISTS idx_avwap_futures_crosses
    ON public.avwap_futures_3m (trading_date, candle_start DESC)
    WHERE high_cross IS NOT NULL OR low_cross IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.avwap_collector_heartbeat (
    service_name TEXT PRIMARY KEY,
    trading_date DATE,
    last_tick_at TIMESTAMPTZ,
    last_bar_at TIMESTAMPTZ,
    instruments INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.avwap_otm_oi_0920_baseline (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    future_instrument_key TEXT NOT NULL,
    futures_price_0920 NUMERIC NOT NULL,
    baseline_ts TIMESTAMPTZ NOT NULL,
    option_type TEXT NOT NULL,
    option_strike NUMERIC NOT NULL,
    option_expiry DATE NOT NULL,
    option_instrument_key TEXT NOT NULL,
    baseline_oi BIGINT,
    baseline_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trading_date, symbol, option_type)
);

CREATE INDEX IF NOT EXISTS idx_avwap_otm_oi_baseline_key
    ON public.avwap_otm_oi_0920_baseline (trading_date, option_instrument_key);

CREATE TABLE IF NOT EXISTS public.avwap_cross_otm_oi_live (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    future_instrument_key TEXT NOT NULL,
    crossing_time TIMESTAMPTZ NOT NULL,
    cross_source TEXT NOT NULL,
    cross_direction TEXT NOT NULL,
    futures_price_at_cross NUMERIC,

    option_type TEXT NOT NULL,
    option_strike NUMERIC,
    option_expiry DATE,
    option_instrument_key TEXT,

    baseline_ts TIMESTAMPTZ,
    baseline_oi BIGINT,
    oi_at_cross BIGINT,
    oi_reduction_pct NUMERIC,

    oi_strength TEXT,
    confirmed_15pct BOOLEAN NOT NULL DEFAULT FALSE,
    confirmed_20pct BOOLEAN NOT NULL DEFAULT FALSE,
    confirmation_state TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (trading_date, future_instrument_key, crossing_time, cross_source)
);

CREATE INDEX IF NOT EXISTS idx_avwap_cross_otm_oi_live_confirmed
    ON public.avwap_cross_otm_oi_live
       (trading_date, confirmed_15pct, crossing_time DESC);
"""

UPSERT_BAR = """
INSERT INTO public.avwap_futures_3m (
    trading_date, symbol, trading_symbol, instrument_key, expiry,
    candle_start, candle_end, open, high, low, close, volume,
    cumulative_volume, avwap_high, avwap_low,
    hourly_avwap_high, hourly_avwap_low, high_cross, low_cross
) VALUES (
    %(trading_date)s, %(symbol)s, %(trading_symbol)s, %(instrument_key)s,
    %(expiry)s, %(candle_start)s, %(candle_end)s, %(open)s, %(high)s,
    %(low)s, %(close)s, %(volume)s, %(cumulative_volume)s,
    %(avwap_high)s, %(avwap_low)s, %(hourly_avwap_high)s,
    %(hourly_avwap_low)s, %(high_cross)s, %(low_cross)s
)
ON CONFLICT (trading_date, instrument_key, candle_start) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    cumulative_volume = EXCLUDED.cumulative_volume,
    avwap_high = EXCLUDED.avwap_high,
    avwap_low = EXCLUDED.avwap_low,
    hourly_avwap_high = EXCLUDED.hourly_avwap_high,
    hourly_avwap_low = EXCLUDED.hourly_avwap_low,
    high_cross = EXCLUDED.high_cross,
    low_cross = EXCLUDED.low_cross,
    updated_at = NOW();
"""


def db_connect():
    return psycopg.connect(DATABASE_URL, autocommit=False)


def ensure_schema() -> None:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()


def parse_expiry(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Instrument dumps have used both seconds and milliseconds.
        seconds = float(value) / (1000 if float(value) > 10_000_000_000 else 1)
        return datetime.fromtimestamp(seconds, tz=UTC).date()
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def clean_symbol(row: dict[str, Any]) -> str:
    for key in ("underlying_symbol", "asset_symbol", "short_name", "name"):
        value = row.get(key)
        if value:
            return str(value).strip().upper()
    return str(row.get("trading_symbol", "UNKNOWN")).strip().upper()


def chunks(items, size=100):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i+size]


def get_full_quotes(keys: list[str]) -> dict[str, dict[str, Any]]:
    """REST full quotes keyed by instrument token/key."""
    keys = list(dict.fromkeys(k for k in keys if k))
    if not keys:
        return {}

    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    out: dict[str, dict[str, Any]] = {}

    for batch in chunks(keys, 100):
        response = requests.get(
            FULL_QUOTE_URL,
            params={"instrument_key": ",".join(batch)},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json().get("data", {}) or {}

        for returned_key, quote_data in payload.items():
            token = quote_data.get("instrument_token") or quote_data.get("instrument_key")
            if token:
                out[str(token)] = quote_data
            out[str(returned_key)] = quote_data

        time.sleep(0.05)

    # Some Upstox responses use descriptive keys. Resolve requested tokens through
    # the embedded instrument_token when possible.
    resolved = {}
    for key in keys:
        if key in out:
            resolved[key] = out[key]
            continue
        for q in out.values():
            if str(q.get("instrument_token") or q.get("instrument_key") or "") == key:
                resolved[key] = q
                break
    return resolved


def quote_oi(quote_data: dict[str, Any] | None) -> int | None:
    if not quote_data:
        return None
    for key in ("oi", "open_interest", "openInterest"):
        value = quote_data.get(key)
        if value is not None:
            try:
                return int(float(value))
            except (TypeError, ValueError):
                pass
    return None


def option_side(row: dict[str, Any]) -> str:
    raw = str(row.get("option_type") or row.get("instrument_type") or "").upper()
    if raw in {"CE", "PE"}:
        return raw

    trading_symbol = str(
        row.get("trading_symbol") or row.get("tradingsymbol") or ""
    ).upper()
    if trading_symbol.endswith("CE"):
        return "CE"
    if trading_symbol.endswith("PE"):
        return "PE"
    return ""


def download_instrument_master() -> list[dict[str, Any]]:
    LOG.info("Downloading Upstox instrument master")
    response = requests.get(INSTRUMENTS_URL, timeout=45)
    response.raise_for_status()
    raw = response.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def discover_stock_options(
    rows: list[dict[str, Any]], today: date
) -> dict[str, list[OptionInstrument]]:
    """Nearest-expiry stock CE/PE contracts grouped by underlying symbol."""
    grouped: dict[str, list[OptionInstrument]] = {}

    for row in rows:
        segment = str(row.get("segment", "")).upper()
        if segment not in {"NSE_FO", "NSE_F&O", "NFO"}:
            continue

        side = option_side(row)
        if side not in {"CE", "PE"}:
            continue

        expiry = parse_expiry(row.get("expiry"))
        key = row.get("instrument_key")
        trading_symbol = row.get("trading_symbol") or row.get("tradingsymbol")
        strike = row.get("strike_price", row.get("strike"))

        if not expiry or expiry < today or not key or not trading_symbol or strike is None:
            continue

        symbol = clean_symbol(row)
        if symbol in INDEX_NAMES:
            continue

        try:
            strike_value = float(strike)
        except (TypeError, ValueError):
            continue

        grouped.setdefault(symbol, []).append(
            OptionInstrument(
                symbol=symbol,
                option_type=side,
                strike=strike_value,
                instrument_key=str(key),
                expiry=expiry,
                trading_symbol=str(trading_symbol),
            )
        )

    # Keep only nearest option expiry for each stock.
    result: dict[str, list[OptionInstrument]] = {}
    for symbol, contracts in grouped.items():
        nearest = min(item.expiry for item in contracts)
        chosen = [item for item in contracts if item.expiry == nearest]
        result[symbol] = chosen

    return result


def select_nearest_otm_pair(
    contracts: list[OptionInstrument], futures_price: float
) -> dict[str, OptionInstrument]:
    ce = [c for c in contracts if c.option_type == "CE" and c.strike > futures_price]
    pe = [c for c in contracts if c.option_type == "PE" and c.strike < futures_price]

    out: dict[str, OptionInstrument] = {}
    if ce:
        out["CE"] = min(ce, key=lambda c: (c.strike - futures_price, c.strike))
    if pe:
        out["PE"] = min(pe, key=lambda c: (futures_price - c.strike, -c.strike))
    return out


def discover_current_month_stock_futures(
    today: date,
    rows: list[dict[str, Any]] | None = None,
) -> list[FutureInstrument]:
    if rows is None:
        rows = download_instrument_master()

    candidates: list[FutureInstrument] = []
    for row in rows:
        segment = str(row.get("segment", "")).upper()
        instrument_type = str(row.get("instrument_type", "")).upper()
        if segment not in {"NSE_FO", "NSE_F&O", "NFO"} or instrument_type != "FUT":
            continue
        expiry = parse_expiry(row.get("expiry"))
        key = row.get("instrument_key")
        trading_symbol = row.get("trading_symbol") or row.get("tradingsymbol")
        if not expiry or expiry < today or not key or not trading_symbol:
            continue

        symbol = clean_symbol(row)
        underlying_type = str(
            row.get("underlying_type") or row.get("asset_type") or ""
        ).upper()
        is_index = underlying_type in {"INDEX", "IDX"} or symbol in INDEX_NAMES
        if is_index:
            continue
        candidates.append(FutureInstrument(symbol, str(trading_symbol), str(key), expiry))

    if not candidates:
        raise RuntimeError("No NSE stock futures found in the Upstox instrument master")

    nearest_expiry = min(item.expiry for item in candidates)
    selected = [item for item in candidates if item.expiry == nearest_expiry]
    # Defensive de-duplication by instrument key.
    selected = list({item.instrument_key: item for item in selected}.values())
    selected.sort(key=lambda item: item.symbol)
    LOG.info("Discovered %d stock futures for expiry %s", len(selected), nearest_expiry)
    return selected


def floor_3m(ts: datetime) -> datetime:
    ts = ts.astimezone(IST)
    minute = ts.minute - (ts.minute % BAR_MINUTES)
    return ts.replace(minute=minute, second=0, microsecond=0)


def market_datetime(day: date, value: dt_time) -> datetime:
    return datetime.combine(day, value, tzinfo=IST)


def deep_find(obj: Any, keys: set[str]) -> Any:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if str(key).lower() in keys and value is not None:
                return value
        for value in obj.values():
            found = deep_find(value, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = deep_find(value, keys)
            if found is not None:
                return found
    return None


def normalise_message(message: Any) -> dict[str, Any]:
    if isinstance(message, dict):
        return message
    if isinstance(message, str):
        return json.loads(message)
    if hasattr(message, "to_dict"):
        return message.to_dict()
    if hasattr(message, "__dict__"):
        return vars(message)
    return {}


def extract_ticks(message: Any) -> list[tuple[str, datetime, float, int | None]]:
    payload = normalise_message(message)
    feeds = payload.get("feeds") or payload.get("data", {}).get("feeds") or {}
    ticks: list[tuple[str, datetime, float, int | None]] = []
    for instrument_key, feed in feeds.items():
        price = deep_find(feed, {"ltp", "last_price", "lastprice"})
        if price is None:
            continue
        epoch = deep_find(feed, {"ltt", "last_trade_time", "timestamp"})
        volume = deep_find(feed, {"vtt", "volume_traded_today", "volume"})
        now = datetime.now(IST)
        try:
            raw_epoch = float(epoch)
            if raw_epoch > 10_000_000_000:
                raw_epoch /= 1000
            tick_time = datetime.fromtimestamp(raw_epoch, tz=UTC).astimezone(IST)
        except (TypeError, ValueError, OSError):
            tick_time = now
        try:
            cumulative_volume = int(float(volume)) if volume is not None else None
        except (TypeError, ValueError):
            cumulative_volume = None
        ticks.append((str(instrument_key), tick_time, float(price), cumulative_volume))
    return ticks


class Collector:
    def __init__(
        self,
        instruments: list[FutureInstrument],
        option_catalog: dict[str, list[OptionInstrument]],
    ):
        self.instruments = instruments
        self.option_catalog = option_catalog
        self.states = {item.instrument_key: SymbolState(item) for item in instruments}
        self.otm_baselines: dict[tuple[str, str], dict[str, Any]] = {}
        self.last_baseline_attempt = 0.0
        self.stop_event = threading.Event()
        self.writer_stop = threading.Event()
        self.write_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.writer_thread: threading.Thread | None = None
        self.last_tick_at: datetime | None = None
        self.last_bar_at: datetime | None = None
        self.streamer = None

    def restore_today(self, trading_date: date) -> None:
        sql = """
        SELECT DISTINCT ON (instrument_key)
            instrument_key, cumulative_volume, avwap_high, avwap_low,
            hourly_avwap_high, hourly_avwap_low, candle_start
        FROM public.avwap_futures_3m
        WHERE trading_date = %s
        ORDER BY instrument_key, candle_start DESC;
        """
        totals_sql = """
        SELECT instrument_key,
               COALESCE(SUM(volume), 0)::bigint AS cum_volume,
               COALESCE(SUM(high * volume), 0) AS cum_high_volume,
               COALESCE(SUM(low * volume), 0) AS cum_low_volume
        FROM public.avwap_futures_3m
        WHERE trading_date = %s
        GROUP BY instrument_key;
        """
        first_hour_sql = """
        SELECT instrument_key, MAX(high), MIN(low), COALESCE(SUM(volume), 0)::bigint
        FROM public.avwap_futures_3m
        WHERE trading_date = %s
          AND (candle_start AT TIME ZONE 'Asia/Kolkata')::time >= TIME '09:15'
          AND (candle_end AT TIME ZONE 'Asia/Kolkata')::time <= TIME '10:15'
        GROUP BY instrument_key;
        """
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(totals_sql, (trading_date,))
                for key, cum_volume, high_pv, low_pv in cur.fetchall():
                    state = self.states.get(key)
                    if state:
                        state.cum_volume = int(cum_volume)
                        state.cum_high_volume = float(high_pv)
                        state.cum_low_volume = float(low_pv)
                cur.execute(sql, (trading_date,))
                for key, cum_vol_end, avh, avl, hour_h, hour_l, _ in cur.fetchall():
                    state = self.states.get(key)
                    if state:
                        state.last_cumulative_volume = int(cum_vol_end) if cum_vol_end is not None else None
                        state.previous_avwap_high = float(avh) if avh is not None else None
                        state.previous_avwap_low = float(avl) if avl is not None else None
                        state.frozen_hour_avwap_high = float(hour_h) if hour_h is not None else None
                        state.frozen_hour_avwap_low = float(hour_l) if hour_l is not None else None
                cur.execute(first_hour_sql, (trading_date,))
                for key, hour_high, hour_low, hour_volume in cur.fetchall():
                    state = self.states.get(key)
                    if state:
                        state.first_hour_high = float(hour_high) if hour_high is not None else None
                        state.first_hour_low = float(hour_low) if hour_low is not None else None
                        state.first_hour_volume = int(hour_volume)
        LOG.info("Restored today's completed AVWAP state from Neon")

    def restore_otm_baseline(self, trading_date: date) -> None:
        sql = """
        SELECT
            symbol, future_instrument_key, futures_price_0920, baseline_ts,
            option_type, option_strike, option_expiry, option_instrument_key,
            baseline_oi, baseline_status
        FROM public.avwap_otm_oi_0920_baseline
        WHERE trading_date=%s
        """
        try:
            with db_connect() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql, (trading_date,))
                    rows = cur.fetchall()

            for row in rows:
                item = dict(row)
                self.otm_baselines[(item["symbol"], item["option_type"])] = item

            if rows:
                LOG.info("Restored %d OTM option OI baseline rows for %s", len(rows), trading_date)
        except Exception:
            LOG.exception("Could not restore 09:20 OTM OI baselines")

    def _latest_futures_price(self, state: SymbolState) -> float | None:
        with state.lock:
            if state.current is not None:
                return float(state.current.close)
        return None

    def freeze_0920_otm_baseline(self, now: datetime) -> None:
        """Freeze nearest OTM CE + PE and their OI using the 09:20 futures price."""
        pending: list[tuple[SymbolState, float, OptionInstrument]] = []

        for state in self.states.values():
            symbol = state.instrument.symbol
            price = self._latest_futures_price(state)
            if price is None or price <= 0:
                continue

            contracts = self.option_catalog.get(symbol, [])
            if not contracts:
                continue

            pair = select_nearest_otm_pair(contracts, price)
            for side in ("CE", "PE"):
                if (symbol, side) in self.otm_baselines:
                    continue
                contract = pair.get(side)
                if contract is not None:
                    pending.append((state, price, contract))

        if not pending:
            return

        quotes = get_full_quotes([contract.instrument_key for _, _, contract in pending])
        rows = []

        for state, futures_price, contract in pending:
            quote_data = quotes.get(contract.instrument_key)
            baseline_oi = quote_oi(quote_data)
            status = "OK" if baseline_oi is not None and baseline_oi > 0 else "OI_UNAVAILABLE"

            row = {
                "trading_date": now.date(),
                "symbol": state.instrument.symbol,
                "future_instrument_key": state.instrument.instrument_key,
                "futures_price_0920": futures_price,
                "baseline_ts": now,
                "option_type": contract.option_type,
                "option_strike": contract.strike,
                "option_expiry": contract.expiry,
                "option_instrument_key": contract.instrument_key,
                "baseline_oi": baseline_oi,
                "baseline_status": status,
            }
            rows.append(row)
            self.otm_baselines[(state.instrument.symbol, contract.option_type)] = row

        sql = """
        INSERT INTO public.avwap_otm_oi_0920_baseline (
            trading_date, symbol, future_instrument_key, futures_price_0920,
            baseline_ts, option_type, option_strike, option_expiry,
            option_instrument_key, baseline_oi, baseline_status
        ) VALUES (
            %(trading_date)s, %(symbol)s, %(future_instrument_key)s,
            %(futures_price_0920)s, %(baseline_ts)s, %(option_type)s,
            %(option_strike)s, %(option_expiry)s, %(option_instrument_key)s,
            %(baseline_oi)s, %(baseline_status)s
        )
        ON CONFLICT (trading_date, symbol, option_type) DO UPDATE SET
            future_instrument_key=EXCLUDED.future_instrument_key,
            futures_price_0920=EXCLUDED.futures_price_0920,
            baseline_ts=EXCLUDED.baseline_ts,
            option_strike=EXCLUDED.option_strike,
            option_expiry=EXCLUDED.option_expiry,
            option_instrument_key=EXCLUDED.option_instrument_key,
            baseline_oi=EXCLUDED.baseline_oi,
            baseline_status=EXCLUDED.baseline_status,
            updated_at=NOW();
        """

        if rows:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                conn.commit()

            ok_count = sum(1 for r in rows if r["baseline_status"] == "OK")
            LOG.info(
                "09:20 OTM OI baseline updated: %d rows (%d with OI)",
                len(rows), ok_count
            )

    @staticmethod
    def oi_strength(reduction_pct: float | None) -> str:
        if reduction_pct is None:
            return "NO_OI_DATA"
        if reduction_pct >= OI_VERY_STRONG_THRESHOLD:
            return "VERY_STRONG"
        if reduction_pct >= OI_STRONG_THRESHOLD:
            return "STRONG"
        if reduction_pct >= 10:
            return "MODERATE"
        if reduction_pct >= 5:
            return "MILD"
        return "NONE"

    def record_cross_oi_confirmation(
        self,
        state: SymbolState,
        crossing_time: datetime,
        cross_source: str,
        cross_direction: str,
        futures_price: float,
    ) -> None:
        side = "CE" if cross_direction == "CROSS_ABOVE" else "PE"
        baseline = self.otm_baselines.get((state.instrument.symbol, side))

        if not baseline:
            payload = {
                "trading_date": crossing_time.date(),
                "symbol": state.instrument.symbol,
                "future_instrument_key": state.instrument.instrument_key,
                "crossing_time": crossing_time,
                "cross_source": cross_source,
                "cross_direction": cross_direction,
                "futures_price_at_cross": futures_price,
                "option_type": side,
                "option_strike": None,
                "option_expiry": None,
                "option_instrument_key": None,
                "baseline_ts": None,
                "baseline_oi": None,
                "oi_at_cross": None,
                "oi_reduction_pct": None,
                "oi_strength": "NO_OI_DATA",
                "confirmed_15pct": False,
                "confirmed_20pct": False,
                "confirmation_state": "NO_0920_BASELINE",
            }
        else:
            option_key = baseline["option_instrument_key"]
            current_quote = get_full_quotes([option_key]).get(option_key)
            current_oi = quote_oi(current_quote)
            baseline_oi = baseline.get("baseline_oi")

            reduction = None
            if baseline_oi is not None and baseline_oi > 0 and current_oi is not None:
                reduction = (baseline_oi - current_oi) / baseline_oi * 100.0

            strength = self.oi_strength(reduction)
            confirmed_15 = reduction is not None and reduction >= OI_STRONG_THRESHOLD
            confirmed_20 = reduction is not None and reduction >= OI_VERY_STRONG_THRESHOLD

            if confirmed_20:
                state_text = "VERY_STRONG_OPTION_CONFIRMED"
            elif confirmed_15:
                state_text = "STRONG_OPTION_CONFIRMED"
            elif reduction is None:
                state_text = "OI_UNAVAILABLE_AT_CROSS"
            else:
                state_text = "AVWAP_ONLY"

            payload = {
                "trading_date": crossing_time.date(),
                "symbol": state.instrument.symbol,
                "future_instrument_key": state.instrument.instrument_key,
                "crossing_time": crossing_time,
                "cross_source": cross_source,
                "cross_direction": cross_direction,
                "futures_price_at_cross": futures_price,
                "option_type": side,
                "option_strike": baseline["option_strike"],
                "option_expiry": baseline["option_expiry"],
                "option_instrument_key": option_key,
                "baseline_ts": baseline["baseline_ts"],
                "baseline_oi": baseline_oi,
                "oi_at_cross": current_oi,
                "oi_reduction_pct": reduction,
                "oi_strength": strength,
                "confirmed_15pct": confirmed_15,
                "confirmed_20pct": confirmed_20,
                "confirmation_state": state_text,
            }

        sql = """
        INSERT INTO public.avwap_cross_otm_oi_live (
            trading_date, symbol, future_instrument_key, crossing_time,
            cross_source, cross_direction, futures_price_at_cross,
            option_type, option_strike, option_expiry, option_instrument_key,
            baseline_ts, baseline_oi, oi_at_cross, oi_reduction_pct,
            oi_strength, confirmed_15pct, confirmed_20pct, confirmation_state
        ) VALUES (
            %(trading_date)s, %(symbol)s, %(future_instrument_key)s,
            %(crossing_time)s, %(cross_source)s, %(cross_direction)s,
            %(futures_price_at_cross)s, %(option_type)s, %(option_strike)s,
            %(option_expiry)s, %(option_instrument_key)s, %(baseline_ts)s,
            %(baseline_oi)s, %(oi_at_cross)s, %(oi_reduction_pct)s,
            %(oi_strength)s, %(confirmed_15pct)s, %(confirmed_20pct)s,
            %(confirmation_state)s
        )
        ON CONFLICT (trading_date, future_instrument_key, crossing_time, cross_source)
        DO UPDATE SET
            cross_direction=EXCLUDED.cross_direction,
            futures_price_at_cross=EXCLUDED.futures_price_at_cross,
            option_type=EXCLUDED.option_type,
            option_strike=EXCLUDED.option_strike,
            option_expiry=EXCLUDED.option_expiry,
            option_instrument_key=EXCLUDED.option_instrument_key,
            baseline_ts=EXCLUDED.baseline_ts,
            baseline_oi=EXCLUDED.baseline_oi,
            oi_at_cross=EXCLUDED.oi_at_cross,
            oi_reduction_pct=EXCLUDED.oi_reduction_pct,
            oi_strength=EXCLUDED.oi_strength,
            confirmed_15pct=EXCLUDED.confirmed_15pct,
            confirmed_20pct=EXCLUDED.confirmed_20pct,
            confirmation_state=EXCLUDED.confirmation_state,
            updated_at=NOW();
        """

        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
            conn.commit()

        LOG.info(
            "%s %s %s | %s %.2f | OI %s -> %s | reduction=%s | %s",
            state.instrument.symbol,
            cross_source,
            cross_direction,
            payload["option_type"],
            payload["option_strike"] or 0,
            payload["baseline_oi"],
            payload["oi_at_cross"],
            "NA" if payload["oi_reduction_pct"] is None else f"{payload['oi_reduction_pct']:.2f}%",
            payload["confirmation_state"],
        )

    def heartbeat(self, status: str, message: str = "") -> None:
        sql = """
        INSERT INTO public.avwap_collector_heartbeat
            (service_name, trading_date, last_tick_at, last_bar_at, instruments, status, message)
        VALUES ('avwap_futures_collector', %s, %s, %s, %s, %s, %s)
        ON CONFLICT (service_name) DO UPDATE SET
            trading_date=EXCLUDED.trading_date,
            last_tick_at=EXCLUDED.last_tick_at,
            last_bar_at=EXCLUDED.last_bar_at,
            instruments=EXCLUDED.instruments,
            status=EXCLUDED.status,
            message=EXCLUDED.message,
            updated_at=NOW();
        """
        try:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        datetime.now(IST).date(), self.last_tick_at, self.last_bar_at,
                        len(self.instruments), status, message[:500],
                    ))
                conn.commit()
        except Exception:
            LOG.exception("Heartbeat write failed")

    def database_writer(self) -> None:
        """Batch simultaneous 3-minute closes into one Neon transaction."""
        while not self.writer_stop.is_set() or not self.write_queue.empty():
            batch: list[dict[str, Any]] = []
            try:
                batch.append(self.write_queue.get(timeout=0.5))
            except queue.Empty:
                continue
            while len(batch) < 500:
                try:
                    batch.append(self.write_queue.get_nowait())
                except queue.Empty:
                    break
            try:
                with db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.executemany(UPSERT_BAR, batch)
                    conn.commit()
                for _ in batch:
                    self.write_queue.task_done()
            except Exception:
                LOG.exception("Neon batch write failed; retrying %d rows", len(batch))
                for row in batch:
                    self.write_queue.task_done()
                    self.write_queue.put(row)
                time.sleep(2)

    def on_message(self, message: Any) -> None:
        for key, tick_time, price, cumulative_volume in extract_ticks(message):
            state = self.states.get(key)
            if not state:
                continue
            if not (MARKET_OPEN <= tick_time.time().replace(tzinfo=None) <= MARKET_CLOSE):
                continue
            self.last_tick_at = tick_time
            self.process_tick(state, tick_time, price, cumulative_volume)

    def process_tick(
        self,
        state: SymbolState,
        tick_time: datetime,
        price: float,
        cumulative_volume: int | None,
    ) -> None:
        bar_start = floor_3m(tick_time)
        with state.lock:
            if state.current and bar_start > state.current.start:
                self.finalise_bar(state)

            volume_delta = 0
            if cumulative_volume is not None:
                if state.last_cumulative_volume is not None:
                    volume_delta = max(0, cumulative_volume - state.last_cumulative_volume)
                elif bar_start.time().replace(tzinfo=None) == MARKET_OPEN:
                    # VTT begins at zero for the session. Include the opening tick.
                    volume_delta = max(0, cumulative_volume)
                state.last_cumulative_volume = cumulative_volume

            if state.current is None:
                state.current = LiveBar(
                    start=bar_start,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=volume_delta,
                    cumulative_volume=cumulative_volume,
                )
            else:
                bar = state.current
                bar.high = max(bar.high, price)
                bar.low = min(bar.low, price)
                bar.close = price
                bar.volume += volume_delta
                bar.cumulative_volume = cumulative_volume
            state.first_tick_seen = True

    @staticmethod
    def crossing(previous: float | None, current: float, level: float | None) -> str | None:
        if previous is None or level is None:
            return None
        if previous <= level < current:
            return "CROSS_ABOVE"
        if previous >= level > current:
            return "CROSS_BELOW"
        return None

    def finalise_bar(self, state: SymbolState) -> None:
        bar = state.current
        if bar is None:
            return
        state.current = None
        day = bar.start.date()
        open_at = market_datetime(day, MARKET_OPEN)
        freeze_at = market_datetime(day, FREEZE_TIME)
        bar_end = bar.start + timedelta(minutes=BAR_MINUTES)
        if bar.start < open_at or bar_end > market_datetime(day, MARKET_CLOSE):
            return

        # A zero-volume bar cannot change AVWAP. Keep the candle for diagnostics.
        if bar.volume > 0:
            state.cum_volume += bar.volume
            state.cum_high_volume += bar.high * bar.volume
            state.cum_low_volume += bar.low * bar.volume
        avwap_high = (
            state.cum_high_volume / state.cum_volume if state.cum_volume else None
        )
        avwap_low = (
            state.cum_low_volume / state.cum_volume if state.cum_volume else None
        )

        # Build the separate 09:15-10:15 hourly candle. On the one-hour
        # timeframe, a High-source AVWAP after its first completed bar equals
        # that hourly candle's high; the Low-source AVWAP equals its low.
        if open_at <= bar.start and bar_end <= freeze_at:
            state.first_hour_high = (
                bar.high if state.first_hour_high is None
                else max(state.first_hour_high, bar.high)
            )
            state.first_hour_low = (
                bar.low if state.first_hour_low is None
                else min(state.first_hour_low, bar.low)
            )
            state.first_hour_volume += bar.volume
        if bar_end == freeze_at:
            state.frozen_hour_avwap_high = state.first_hour_high
            state.frozen_hour_avwap_low = state.first_hour_low

        high_cross = None
        low_cross = None
        if bar.start >= freeze_at and avwap_high is not None and avwap_low is not None:
            high_cross = self.crossing(
                state.previous_avwap_high, avwap_high, state.frozen_hour_avwap_high
            )
            low_cross = self.crossing(
                state.previous_avwap_low, avwap_low, state.frozen_hour_avwap_low
            )

        row = {
            "trading_date": day,
            "symbol": state.instrument.symbol,
            "trading_symbol": state.instrument.trading_symbol,
            "instrument_key": state.instrument.instrument_key,
            "expiry": state.instrument.expiry,
            "candle_start": bar.start,
            "candle_end": bar_end,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "cumulative_volume": bar.cumulative_volume,
            "avwap_high": avwap_high,
            "avwap_low": avwap_low,
            "hourly_avwap_high": state.frozen_hour_avwap_high,
            "hourly_avwap_low": state.frozen_hour_avwap_low,
            "high_cross": high_cross,
            "low_cross": low_cross,
        }
        self.write_queue.put(row)
        self.last_bar_at = bar_end
        if high_cross or low_cross:
            LOG.info(
                "%s high=%s low=%s at %s",
                state.instrument.symbol, high_cross, low_cross, bar_end,
            )

            try:
                if high_cross:
                    self.record_cross_oi_confirmation(
                        state=state,
                        crossing_time=bar_end,
                        cross_source="HIGH_AVWAP",
                        cross_direction=high_cross,
                        futures_price=bar.close,
                    )
                if low_cross:
                    self.record_cross_oi_confirmation(
                        state=state,
                        crossing_time=bar_end,
                        cross_source="LOW_AVWAP",
                        cross_direction=low_cross,
                        futures_price=bar.close,
                    )
            except Exception:
                LOG.exception(
                    "%s OTM OI confirmation failed at %s",
                    state.instrument.symbol, bar_end
                )

        state.previous_avwap_high = avwap_high
        state.previous_avwap_low = avwap_low

    def on_open(self) -> None:
        LOG.info("Market-data stream connected")
        self.heartbeat("CONNECTED")

    def on_error(self, error: Any) -> None:
        LOG.error("Market-data stream error: %s", error)
        self.heartbeat("ERROR", str(error))

    def on_close(self, *args: Any) -> None:
        LOG.warning("Market-data stream closed: %s", args)
        self.heartbeat("DISCONNECTED", str(args))

    def flush_completed_by_clock(self) -> None:
        now = datetime.now(IST)
        for state in self.states.values():
            with state.lock:
                if state.current and now >= state.current.start + timedelta(minutes=BAR_MINUTES, seconds=3):
                    self.finalise_bar(state)

    def connect(self) -> None:
        configuration = upstox_client.Configuration()
        configuration.access_token = ACCESS_TOKEN
        api_client = upstox_client.ApiClient(configuration)
        keys = [item.instrument_key for item in self.instruments]
        # Initial keys are supplied in the constructor so the SDK subscribes in
        # its open callback, exactly as documented by the official SDK.
        self.streamer = upstox_client.MarketDataStreamerV3(api_client, keys, "full")
        self.streamer.on("open", self.on_open)
        self.streamer.on("message", self.on_message)
        self.streamer.on("error", self.on_error)
        self.streamer.on("close", self.on_close)
        self.streamer.auto_reconnect(True, 5, 50)
        self.streamer.connect()
        LOG.info("Requested FULL-mode subscription for %d futures", len(keys))

    def stop(self, *_: Any) -> None:
        self.stop_event.set()

    def run(self) -> None:
        trading_day = datetime.now(IST).date()
        self.restore_today(trading_day)
        self.restore_otm_baseline(trading_day)
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        self.writer_thread = threading.Thread(
            target=self.database_writer, name="neon-writer", daemon=True
        )
        self.writer_thread.start()
        self.connect()
        last_heartbeat = 0.0
        while not self.stop_event.wait(1):
            now = datetime.now(IST)
            if not is_nse_trading_day(now.date()):
                LOG.info("%s: %s | stopping collector.",
                         holiday_reason(now.date()), now.date().isoformat())
                self.heartbeat("NON_TRADING_DAY", holiday_reason(now.date()) or "")
                break

            # Freeze/retry the 09:20 OTM CE/PE OI baseline before 09:25.
            local_time = now.time().replace(tzinfo=None)
            if OI_BASELINE_TIME <= local_time <= OI_BASELINE_LAST_RETRY:
                if time.monotonic() - self.last_baseline_attempt >= 15:
                    try:
                        self.freeze_0920_otm_baseline(now.replace(microsecond=0))
                    except Exception:
                        LOG.exception("09:20 OTM OI baseline attempt failed")
                    self.last_baseline_attempt = time.monotonic()

            self.flush_completed_by_clock()
            if time.monotonic() - last_heartbeat >= 30:
                self.heartbeat("RUNNING")
                last_heartbeat = time.monotonic()
            if datetime.now(IST).time().replace(tzinfo=None) > MARKET_CLOSE:
                self.flush_completed_by_clock()
                self.heartbeat("MARKET_CLOSED")
                break
        if self.streamer:
            try:
                self.streamer.disconnect()
            except Exception:
                LOG.exception("Streamer disconnect failed")
        self.writer_stop.set()
        if self.writer_thread:
            self.writer_thread.join(timeout=15)
        if not self.write_queue.empty():
            LOG.error("Worker stopped with %d queued Neon rows", self.write_queue.qsize())


def wait_until_subscription_window() -> None:
    """Connect close to market open, but never run on weekends/NSE holidays."""
    while True:
        now = datetime.now(IST)

        if not is_nse_trading_day(now.date()):
            LOG.info("%s: %s | collector will not run.",
                     holiday_reason(now.date()), now.date().isoformat())
            raise SystemExit(0)

        connect_at = market_datetime(now.date(), dt_time(9, 10))
        close_at = market_datetime(now.date(), MARKET_CLOSE)

        if now > close_at:
            LOG.info("Session already closed for %s; collector will not start.",
                     now.date().isoformat())
            raise SystemExit(0)

        if now < connect_at:
            seconds = min(300, max(1, int((connect_at - now).total_seconds())))
            LOG.info("Waiting %d seconds for 09:10 IST", seconds)
            time.sleep(seconds)
            continue

        return

def main() -> None:
    if not DATABASE_URL:
        raise RuntimeError("NEON_DATABASE_URL is required")
    if not ACCESS_TOKEN:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN (or UPSTOX_TOKEN) is required")
    stop_if_non_trading_day()
    ensure_schema()
    while True:
        wait_until_subscription_window()
        trading_day = datetime.now(IST).date()
        master_rows = download_instrument_master()
        instruments = discover_current_month_stock_futures(trading_day, master_rows)
        option_catalog = discover_stock_options(master_rows, trading_day)
        LOG.info("Option catalog available for %d stock symbols", len(option_catalog))
        collector = Collector(instruments, option_catalog)
        collector.run()
        if collector.stop_event.is_set():
            break
        # Remain alive as a Railway worker and prepare for the next session.
        time.sleep(30)


if __name__ == "__main__":
    main()