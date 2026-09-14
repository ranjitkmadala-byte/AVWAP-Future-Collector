# AVWAP Futures Collector + Live OTM OI Confirmation

Enhancement:
1. Existing futures AVWAP logic is preserved.
2. At 09:20 IST, for every stock future:
   - select nearest OTM CE above the futures price
   - select nearest OTM PE below the futures price
   - freeze their OI from Upstox full quote
3. On AVWAP crossing:
   - CROSS_ABOVE -> inspect frozen OTM CE
   - CROSS_BELOW -> inspect frozen OTM PE
4. Calculate OI reduction from 09:20:
      (baseline_oi - current_oi) / baseline_oi * 100
5. Classify:
   - <5%       NONE
   - 5-10%     MILD
   - 10-15%    MODERATE
   - 15-20%    STRONG
   - >=20%     VERY_STRONG
6. Store every live crossing and continuous OI-reduction value.

New Neon tables:
- public.avwap_otm_oi_0920_baseline
- public.avwap_cross_otm_oi_live

Existing tables remain unchanged:
- public.avwap_futures_3m
- public.avwap_collector_heartbeat

Railway Start Command:
python avwap_futures_otm_oi_collector.py

Required variables:
- NEON_DATABASE_URL
- UPSTOX_TOKEN (or UPSTOX_ACCESS_TOKEN)

Optional:
- AVWAP_OI_STRONG_THRESHOLD=15
- AVWAP_OI_VERY_STRONG_THRESHOLD=20
- NSE_TRADING_HOLIDAYS=YYYY-MM-DD,...

Recommended Railway Restart Policy:
On Failure
