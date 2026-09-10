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


@dataclass(frozen=True)
class FutureInstrument:
    symbol: str
    trading_symbol: str
    instrument_key: str
    expiry: date


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


def discover_current_month_stock_futures(today: date) -> list[FutureInstrument]:
    LOG.info("Downloading Upstox instrument master")
    response = requests.get(INSTRUMENTS_URL, timeout=45)
    response.raise_for_status()
    raw = response.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    rows = json.loads(raw.decode("utf-8"))

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
    def __init__(self, instruments: list[FutureInstrument]):
        self.instruments = instruments
        self.states = {item.instrument_key: SymbolState(item) for item in instruments}
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
        LOG.info("Restored today's completed AVWAP state from Neon")

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

        # The 10:12-10:15 bar completes the first 60-minute window.
        if bar_end == freeze_at and avwap_high is not None and avwap_low is not None:
            state.frozen_hour_avwap_high = avwap_high
            state.frozen_hour_avwap_low = avwap_low

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
        self.restore_today(datetime.now(IST).date())
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        self.writer_thread = threading.Thread(
            target=self.database_writer, name="neon-writer", daemon=True
        )
        self.writer_thread.start()
        self.connect()
        last_heartbeat = 0.0
        while not self.stop_event.wait(1):
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
    """Keep Railway alive, but connect close to market open."""
    while True:
        now = datetime.now(IST)
        connect_at = market_datetime(now.date(), dt_time(9, 10))
        close_at = market_datetime(now.date(), MARKET_CLOSE)
        if now.weekday() >= 5 or now > close_at:
            days_ahead = 1
            while (now + timedelta(days=days_ahead)).weekday() >= 5:
                days_ahead += 1
            next_day = (now + timedelta(days=days_ahead)).date()
            next_connect = market_datetime(next_day, dt_time(9, 10))
            seconds = min(900, max(1, int((next_connect - now).total_seconds())))
            LOG.info("Outside market window; waiting for next session")
            time.sleep(seconds)
            continue
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
    ensure_schema()
    while True:
        wait_until_subscription_window()
        instruments = discover_current_month_stock_futures(datetime.now(IST).date())
        collector = Collector(instruments)
        collector.run()
        if collector.stop_event.is_set():
            break
        # Remain alive as a Railway worker and prepare for the next session.
        time.sleep(30)


if __name__ == "__main__":
    main()
