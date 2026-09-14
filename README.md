# AVWAP Futures Collector — Holiday Safe

Changes:
- skips Saturdays and Sundays
- skips NSE F&O 2026 trading holidays, including 14-Sep-2026
- exits cleanly before schema setup, instrument-master download, or Upstox streaming
- defensive non-trading-day check inside the live run loop
- supports extra/future holidays via:
  NSE_TRADING_HOLIDAYS=YYYY-MM-DD,YYYY-MM-DD,...

Railway Start Command:
python avwap_futures_holiday_safe.py

Recommended Railway Restart Policy:
On Failure
