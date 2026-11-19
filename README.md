# NYC-TLC-Taxi-Data-Pipeline

An end-to-end data engineering project on NYC TLC Yellow Taxi trip records (January-March 2026, about 11 million trips), built with PySpark and Delta Lake on Databricks using the medallion architecture.

```
Raw Parquet  ->  Bronze (raw Delta)  ->  Silver (validated + quarantine)  ->  Gold (business aggregates)
```

## Highlights
- **Explicit schemas and audit columns** at ingestion (`ingest_ts`, `source_file` from file metadata).
- **Profiling-driven data quality rules.** Thresholds come from data analysis, not assumptions (see [docs/data_quality_findings.md](docs/data_quality_findings.md)).
- **Quarantine pattern.** Invalid rows go to a separate table with a `reject_reason`; nothing is silently dropped.
- **Reconciliation checks** at each layer: `bronze = silver + rejected`, and Gold totals match the revenue population.
- **Flag, don't delete.** Trips with missing (not invalid) fields, such as Flex Fare trips and Vendor 7 durations, are kept and flagged.
- **Broadcast join** for the small zone lookup, and window functions for per-day rankings.

## Results

| Layer | Table | Rows |
|---|---|---|
| Bronze | `bronze.yellow_trips` | 11,077,206 |
| Silver | `silver.yellow_trips` | 10,937,139 (98.7%) |
| Silver | `silver.yellow_trips_rejected` | 140,067 (1.3%) |
| Gold | revenue population (no-charge and dispute trips excluded) | 10,837,836 |

## Gold tables

| Table | Grain | Purpose |
|---|---|---|
| `revenue_by_zone` | pickup zone | Trips, revenue, average fare and distance per zone |
| `revenue_by_date_hour` | date x hour | Base table for time-based views |
| `hourly_pattern` | hour of day | Busiest and quietest hours |
| `dow_hour_heatmap` | weekday x hour | Weekday vs weekend demand |
| `peak_zones_daily` | date x rank | Top 5 pickup zones per day |
| `tip_trends` | hour of day | Tip amount and tip percentage (credit-card trips) |
| `top_routes` | zone pair | 100 busiest routes |

## Repository structure
```
notebooks/
  00_setup.ipynb              catalog, schemas, volume
  01_bronze_ingestion.ipynb   raw ingestion with explicit schema
  02_data_profiling.ipynb     analysis behind each cleaning rule
  03_silver_transform.ipynb   rules, flags, quarantine, reconciliation
  04_gold_aggregations.ipynb  broadcast join and Gold tables
docs/
  data_quality_findings.md
```

## How to run
1. Download the Yellow Taxi Parquet files (January-March 2026) and the Taxi Zone Lookup CSV from the NYC TLC trip record page. The data is not included in this repository.
2. Run `00_setup`, then upload the files to the volume `taxi_project.bronze.data`.
3. Run notebooks `01` to `04` in order.

Tested on Databricks Free Edition (serverless compute).

## Roadmap
- Incremental loading with Delta `MERGE` and an ingestion log
- BI dashboard on the Gold tables
