# All-Futures AVWAP Scanner — Railway + Neon

This package is intentionally separate from the existing money-flow engine.

## Logic

1. Discover every nearest-expiry NSE stock future from the Upstox instrument master.
2. Subscribe by 09:10 IST and accept ticks from 09:15 IST.
3. Construct completed 3-minute OHLCV candles.
4. Calculate cumulative volume-weighted candle highs and lows from 09:15:

   `AVWAP High = sum(3m high × 3m volume) / sum(3m volume)`

   `AVWAP Low = sum(3m low × 3m volume) / sum(3m volume)`

5. When the 10:12–10:15 candle completes, freeze both values as the hourly AVWAP lines.
6. From the completed 10:15–10:18 candle onward, detect the continuing 3-minute AVWAP crossing its matching frozen hourly line.

## Railway services

Create two services from the same repository and set their root directory to this folder.

### Service 1 — `avwap-futures-collector`

Start command:

```text
python avwap_futures_collector.py
```

Variables:

- `NEON_DATABASE_URL`
- `UPSTOX_ACCESS_TOKEN` (the existing `UPSTOX_TOKEN` name is also accepted)
- `LOG_LEVEL=INFO`

This is a continuously running worker. It waits until 09:10 IST, subscribes before the opening, records from 09:15, and stops after 15:30.

### Service 2 — `avwap-futures-dashboard`

Start command:

```text
streamlit run streamlit_app.py --server.port $PORT --server.address 0.0.0.0
```

Variable:

- `NEON_DATABASE_URL`

Do not put the Upstox token in the dashboard service.

## Neon objects

The collector safely creates these objects when absent:

- `public.avwap_futures_3m`
- `public.avwap_collector_heartbeat`
- indexes for latest-symbol reads and crossing-event reads

Writes are idempotent on `(trading_date, instrument_key, candle_start)`.
Simultaneous candle closes are written in batches rather than opening one database transaction per future.

## Deployment order

1. Deploy the collector service and confirm its log reports the discovered futures count and subscriptions.
2. Confirm `avwap_collector_heartbeat.status` becomes `RUNNING`.
3. Deploy the dashboard service.
4. At 09:18 IST confirm the first completed candle appears.
5. At 10:15 IST confirm the frozen hourly AVWAP columns become populated.
6. From 10:18 IST confirm crossings can begin appearing.

## Important operating notes

- The Upstox access token must be valid for that trading day.
- Railway must keep the collector worker running continuously during market hours.
- Zero-volume candles are retained for diagnostics but do not change AVWAP.
- The collector restores completed-day AVWAP accumulators from Neon after a restart.
- A restart during an unfinished candle may produce a partial candle; the dashboard exposes volume and heartbeat timestamps so this is visible.
