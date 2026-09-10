"""One-time Railway job to backfill today's all-futures AVWAP dashboard.

Run after the market closes:

    python backfill_today.py

It uses the same UPSTOX_ACCESS_TOKEN and NEON_DATABASE_URL as the live worker.
Existing rows are updated idempotently, so the command is safe to rerun.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote

import requests

from avwap_futures_collector import (
    ACCESS_TOKEN,
    BAR_MINUTES,
    DATABASE_URL,
    FREEZE_TIME,
    IST,
    MARKET_CLOSE,
    MARKET_OPEN,
    UPSERT_BAR,
    Collector,
    FutureInstrument,
    db_connect,
    discover_current_month_stock_futures,
    ensure_schema,
    market_datetime,
)


LOG = logging.getLogger("avwap-backfill")
INTRADAY_URL = (
    "https://api.upstox.com/v3/historical-candle/intraday/"
    "{instrument_key}/minutes/3"
)
HISTORICAL_URL = (
    "https://api.upstox.com/v3/historical-candle/"
    "{instrument_key}/minutes/3/{to_date}/{from_date}"
)
REQUEST_GAP_SECONDS = 0.20
MAX_ATTEMPTS = 4


def fetch_candles(instrument: FutureInstrument, trading_date: date) -> list[list[Any]]:
    encoded_key = quote(instrument.instrument_key, safe="")
    if trading_date == datetime.now(IST).date():
        url = INTRADAY_URL.format(instrument_key=encoded_key)
    else:
        iso_date = trading_date.isoformat()
        url = HISTORICAL_URL.format(
            instrument_key=encoded_key, to_date=iso_date, from_date=iso_date
        )
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            payload = response.json()
            if payload.get("status") != "success":
                raise RuntimeError(f"Unexpected Upstox response for {instrument.symbol}: {payload}")
            return payload.get("data", {}).get("candles", [])
        if response.status_code == 429 or response.status_code >= 500:
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(8, 2 ** (attempt - 1))
            LOG.warning(
                "%s returned HTTP %s; retrying in %.1fs",
                instrument.symbol, response.status_code, delay,
            )
            time.sleep(delay)
            continue
        detail = response.text[:500]
        raise RuntimeError(
            f"Upstox HTTP {response.status_code} for {instrument.symbol}: {detail}"
        )
    raise RuntimeError(f"Upstox request failed after retries for {instrument.symbol}")


def parse_session_candles(
    raw_candles: list[list[Any]], trading_date, now: datetime
) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    open_at = market_datetime(trading_date, MARKET_OPEN)
    close_at = market_datetime(trading_date, MARKET_CLOSE)
    for candle in raw_candles:
        if len(candle) < 6:
            continue
        start = datetime.fromisoformat(str(candle[0])).astimezone(IST)
        end = start + timedelta(minutes=BAR_MINUTES)
        if start.date() != trading_date:
            continue
        if start < open_at or end > close_at or end > now:
            continue
        parsed.append({
            "start": start,
            "end": end,
            "open": float(candle[1]),
            "high": float(candle[2]),
            "low": float(candle[3]),
            "close": float(candle[4]),
            "volume": max(0, int(float(candle[5]))),
        })
    parsed.sort(key=lambda row: row["start"])
    return parsed


def calculate_rows(
    instrument: FutureInstrument,
    candles: list[dict[str, Any]],
    trading_date,
) -> list[dict[str, Any]]:
    cum_volume = 0
    cum_high_volume = 0.0
    cum_low_volume = 0.0
    previous_high = None
    previous_low = None
    frozen_high = None
    frozen_low = None
    freeze_at = market_datetime(trading_date, FREEZE_TIME)
    rows: list[dict[str, Any]] = []

    for candle in candles:
        volume = candle["volume"]
        if volume > 0:
            cum_volume += volume
            cum_high_volume += candle["high"] * volume
            cum_low_volume += candle["low"] * volume
        avwap_high = cum_high_volume / cum_volume if cum_volume else None
        avwap_low = cum_low_volume / cum_volume if cum_volume else None

        if candle["end"] == freeze_at and avwap_high is not None and avwap_low is not None:
            frozen_high = avwap_high
            frozen_low = avwap_low

        high_cross = None
        low_cross = None
        if candle["start"] >= freeze_at and avwap_high is not None and avwap_low is not None:
            high_cross = Collector.crossing(previous_high, avwap_high, frozen_high)
            low_cross = Collector.crossing(previous_low, avwap_low, frozen_low)

        rows.append({
            "trading_date": trading_date,
            "symbol": instrument.symbol,
            "trading_symbol": instrument.trading_symbol,
            "instrument_key": instrument.instrument_key,
            "expiry": instrument.expiry,
            "candle_start": candle["start"],
            "candle_end": candle["end"],
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": volume,
            "cumulative_volume": cum_volume,
            "avwap_high": avwap_high,
            "avwap_low": avwap_low,
            "hourly_avwap_high": frozen_high,
            "hourly_avwap_low": frozen_low,
            "high_cross": high_cross,
            "low_cross": low_cross,
        })
        previous_high = avwap_high
        previous_low = avwap_low
    return rows


def write_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with db_connect() as conn:
        with conn.cursor() as cur:
            for offset in range(0, len(rows), 1000):
                cur.executemany(UPSERT_BAR, rows[offset:offset + 1000])
        conn.commit()


def update_heartbeat(trading_date, instruments: int, message: str) -> None:
    sql = """
    INSERT INTO public.avwap_collector_heartbeat
        (service_name, trading_date, last_tick_at, last_bar_at,
         instruments, status, message)
    VALUES ('avwap_futures_collector', %s, NULL, %s, %s, 'BACKFILL_COMPLETE', %s)
    ON CONFLICT (service_name) DO UPDATE SET
        trading_date=EXCLUDED.trading_date,
        last_bar_at=EXCLUDED.last_bar_at,
        instruments=EXCLUDED.instruments,
        status=EXCLUDED.status,
        message=EXCLUDED.message,
        updated_at=NOW();
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                trading_date,
                market_datetime(trading_date, MARKET_CLOSE),
                instruments,
                message,
            ))
        conn.commit()


def print_summary(trading_date) -> None:
    sql = """
    SELECT
      COUNT(*) AS candle_rows,
      COUNT(DISTINCT instrument_key) AS futures_written,
      COUNT(DISTINCT instrument_key) FILTER (
        WHERE hourly_avwap_high IS NOT NULL AND hourly_avwap_low IS NOT NULL
      ) AS futures_with_frozen_levels,
      COUNT(*) FILTER (
        WHERE high_cross IS NOT NULL OR low_cross IS NOT NULL
      ) AS crossing_events,
      COUNT(DISTINCT instrument_key) FILTER (
        WHERE high_cross IS NOT NULL OR low_cross IS NOT NULL
      ) AS futures_with_crossings,
      MIN(candle_start) AS first_candle,
      MAX(candle_end) AS last_candle
    FROM public.avwap_futures_3m
    WHERE trading_date=%s;
    """
    events_sql = """
    SELECT symbol, candle_end, high_cross, low_cross,
           ROUND(avwap_high, 2), ROUND(hourly_avwap_high, 2),
           ROUND(avwap_low, 2), ROUND(hourly_avwap_low, 2)
    FROM public.avwap_futures_3m
    WHERE trading_date=%s AND (high_cross IS NOT NULL OR low_cross IS NOT NULL)
    ORDER BY candle_end, symbol
    LIMIT 30;
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (trading_date,))
            summary = cur.fetchone()
            cur.execute(events_sql, (trading_date,))
            events = cur.fetchall()
    labels = [
        "candle_rows", "futures_written", "futures_with_frozen_levels",
        "crossing_events", "futures_with_crossings", "first_candle", "last_candle",
    ]
    LOG.info("BACKFILL SUMMARY")
    for label, value in zip(labels, summary):
        LOG.info("%-28s %s", label, value)
    for event in events:
        LOG.info("CROSS %s", event)


def requested_date() -> date:
    parser = argparse.ArgumentParser(description="Backfill the all-futures AVWAP dashboard")
    parser.add_argument(
        "--date",
        dest="trading_date",
        help="Trading date in YYYY-MM-DD; defaults to BACKFILL_DATE or today's IST date",
    )
    args = parser.parse_args()
    value = args.trading_date or os.getenv("BACKFILL_DATE")
    return date.fromisoformat(value) if value else datetime.now(IST).date()


def main() -> None:
    if not DATABASE_URL:
        raise RuntimeError("NEON_DATABASE_URL is required")
    if not ACCESS_TOKEN:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN (or UPSTOX_TOKEN) is required")

    ensure_schema()
    trading_date = requested_date()
    now = datetime.now(IST)
    # A past trading day is fully complete; preserve the actual current time for today.
    session_cutoff = (
        now if trading_date == now.date()
        else market_datetime(trading_date, MARKET_CLOSE)
    )
    instruments = discover_current_month_stock_futures(trading_date)
    all_rows: list[dict[str, Any]] = []
    failures: list[tuple[str, str]] = []

    LOG.info("Backfilling %s for %d futures", trading_date, len(instruments))
    for number, instrument in enumerate(instruments, start=1):
        try:
            raw = fetch_candles(instrument, trading_date)
            candles = parse_session_candles(raw, trading_date, session_cutoff)
            rows = calculate_rows(instrument, candles, trading_date)
            all_rows.extend(rows)
            LOG.info(
                "[%d/%d] %-20s %d completed candles",
                number, len(instruments), instrument.symbol, len(rows),
            )
        except Exception as exc:
            failures.append((instrument.symbol, str(exc)))
            LOG.error("[%d/%d] %s failed: %s", number, len(instruments), instrument.symbol, exc)
        time.sleep(REQUEST_GAP_SECONDS)

    if not all_rows:
        raise RuntimeError("No candles were downloaded; nothing was written")
    write_rows(all_rows)
    successful = len(instruments) - len(failures)
    message = f"{len(all_rows)} rows; {successful}/{len(instruments)} futures successful"
    update_heartbeat(trading_date, successful, message)
    print_summary(trading_date)

    if failures:
        LOG.warning("Failures (%d): %s", len(failures), failures)
    if failures and len(failures) / len(instruments) > 0.10:
        raise RuntimeError("More than 10% of futures failed; review the Railway logs")


if __name__ == "__main__":
    main()
