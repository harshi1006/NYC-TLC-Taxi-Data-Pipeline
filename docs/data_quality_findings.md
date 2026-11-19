# Data Quality Findings

Each rule in the Silver layer comes from an observation in the data. This document records the evidence and the decision.

| # | Finding | Evidence | Decision |
|---|---|---|---|
| 1 | Null `passenger_count` is a trip type, not bad data | About 3.06M nulls, concentrated in `payment_type = 0` (Flex Fare). `RatecodeID`, `store_and_fwd_flag` and `congestion_surcharge` are null on the same rows. Fares and distances are normal. | Keep the rows and flag `is_flex_fare`. Reject only `passenger_count = 0`. |
| 2 | Vendor 7 (Helix) reports `dropoff = pickup` | About 133.7k rows; fare and distance are normal. Strictly negative durations affect only 5 rows. | Reject `dropoff < pickup` only. Flag `duration_missing`; keep duration and speed null. |
| 3 | Z-score is unsuitable for `trip_distance` | Distribution is right-skewed; mean and standard deviation are inflated by extreme values. A z-score cutoff either misses the garbage or removes legitimate airport and out-of-city trips (25-100 miles). | Use domain caps instead: distance 500 mi, duration 12 h, average speed 80 mph. |
| 4 | Profiling must not use an outlier-filtered dataset | An early profile ran on rows already filtered by the z-score rule, which hid the real tail (counts above 1,000 miles looked like 4 rows instead of about 311). | Profile on a structurally valid base without outlier filters. |
| 5 | Long trips contain two populations | 100-200 mile trips include plausible out-of-city trips (about 45-65 mph, proportional fare) and meter errors implying several hundred mph. | Combine the distance cap with a speed cap. |
| 6 | Zero distance with normal fare | About 278k rows with `trip_distance <= 0` and median fare of about $23. | Flag `distance_missing`; do not reject. |
| 7 | Negative fares are reversals | Most carry `payment_type` 3 (no charge) or 4 (dispute). | Reject as invalid fare. |
| 8 | Unknown values are stored as null, not 0 | Zero would bias averages and break speed calculations. | Leave `trip_duration_min` and `avg_speed_mph` null when unknown; aggregates ignore nulls. |
| 9 | No duplicate trips | Zero duplicate groups on (vendor, pickup, dropoff, zones, distance, total amount). A looser key produced thousands of false positives because distinct trips share flat fares. | No deduplication required. |
| 10 | Non-revenue trips | About 99k valid trips have `payment_type` 3 or 4. | Excluded from revenue tables, kept in Silver. |
| 11 | Tip statistics | Cash tips are not recorded, so tip analysis is limited to credit-card trips. About 90% of card trips have a tip. | Report tip amount, mean tip percentage and fare-weighted tip percentage separately. |

**Outcome:** the share of valid rows rose from 92.1% to 98.7% after replacing the z-score rule with domain caps and changing three rules from reject to flag (Flex Fare nulls, Vendor 7 durations, zero distance). Reconciliation holds: `bronze = silver + rejected`.
