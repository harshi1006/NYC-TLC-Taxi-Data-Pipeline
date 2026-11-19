# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold Aggregations
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Build business-facing aggregate tables for reporting and dashboards |
# MAGIC | **Input** | `taxi_project.silver.yellow_trips`, `taxi_project.bronze.taxi_zone_lookup` |
# MAGIC | **Output** | Seven tables in `taxi_project.gold` |
# MAGIC
# MAGIC | Business question | Table | Grain (one row per) |
# MAGIC |---|---|---|
# MAGIC | Where is revenue generated? | `revenue_by_zone` | pickup zone |
# MAGIC | How does demand change over time? | `revenue_by_date_hour` | date and hour (base table) |
# MAGIC | Which hours are busiest? | `hourly_pattern` | hour of day |
# MAGIC | How do weekday and weekend patterns differ? | `dow_hour_heatmap` | weekday and hour |
# MAGIC | Which zones lead each day? | `peak_zones_daily` | date and rank (top 5) |
# MAGIC | How do tips vary through the day? | `tip_trends` | hour of day |
# MAGIC | Which routes are the busiest? | `top_routes` | pickup-dropoff zone pair |
# MAGIC
# MAGIC **Revenue definition.** `total_amount` is used for revenue and `fare_amount` for average fare. Trips with `payment_type` 3 (no charge) or 4 (dispute) are excluded from revenue tables. Tip analysis covers credit-card trips only, because cash tips are not recorded.
# MAGIC
# MAGIC `revenue_by_date_hour` is the base table at the finest grain; the hourly and weekday views are derived from it.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

def save(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"taxi_project.gold.{name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Join zone names (broadcast)
# MAGIC
# MAGIC The lookup table has about 265 rows, so it is broadcast to avoid shuffling the trips table. A left join keeps every trip even if a zone ID has no match.

# COMMAND ----------

silver = spark.table("taxi_project.silver.yellow_trips")
zones  = spark.table("taxi_project.bronze.taxi_zone_lookup")

pu_zones = zones.select(col("LocationID").alias("PULocationID"),
                        col("Borough").alias("pickup_borough"),
                        col("Zone").alias("pickup_zone"))
do_zones = zones.select(col("LocationID").alias("DOLocationID"),
                        col("Borough").alias("dropoff_borough"),
                        col("Zone").alias("dropoff_zone"))

joined = (silver
    .join(broadcast(pu_zones), "PULocationID", "left")
    .join(broadcast(do_zones), "DOLocationID", "left"))

assert joined.count() == silver.count(), "Join changed the row count"
joined.explain()   # expect BroadcastHashJoin

# COMMAND ----------

# Revenue population: exclude no-charge (3) and dispute (4) trips
rev = joined.filter(~col("payment_type").isin(3, 4))
expected_trips = rev.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `revenue_by_zone`

# COMMAND ----------

revenue_by_zone = (rev
    .groupBy("pickup_borough", "pickup_zone")
    .agg(count("*").alias("trips"),
         round(sum("total_amount"), 2).alias("total_revenue"),
         round(avg("fare_amount"), 2).alias("avg_fare"),
         round(avg(when(col("trip_distance") > 0, col("trip_distance"))), 2).alias("avg_distance_mi"))
    .orderBy(desc("total_revenue")))

save(revenue_by_zone, "revenue_by_zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `revenue_by_date_hour` (base table)

# COMMAND ----------

date_hour = (rev
    .groupBy("pickup_date", "pickup_hour")
    .agg(count("*").alias("trips"),
         round(sum("total_amount"), 2).alias("total_revenue"),
         round(avg("fare_amount"), 2).alias("avg_fare")))

save(date_hour, "revenue_by_date_hour")
base = spark.table("taxi_project.gold.revenue_by_date_hour")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `hourly_pattern`
# MAGIC
# MAGIC `avg_trips_per_day` divides by the number of days so that months with different lengths are comparable.

# COMMAND ----------

n_days = base.select("pickup_date").distinct().count()

hourly_pattern = (base
    .groupBy("pickup_hour")
    .agg(sum("trips").alias("trips"),
         round(sum("total_revenue"), 2).alias("total_revenue"))
    .withColumn("avg_trips_per_day", round(col("trips") / n_days, 1))
    .orderBy("pickup_hour"))

save(hourly_pattern, "hourly_pattern")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `dow_hour_heatmap`

# COMMAND ----------

dow_hour_heatmap = (base
    .withColumn("dow_num", dayofweek("pickup_date"))          # 1 = Sunday ... 7 = Saturday
    .withColumn("dow", date_format("pickup_date", "E"))
    .withColumn("day_type", when(col("dow_num").isin(1, 7), "Weekend").otherwise("Weekday"))
    .groupBy("dow_num", "dow", "day_type", "pickup_hour")
    .agg(sum("trips").alias("trips"),
         round(sum("total_revenue"), 2).alias("total_revenue"),
         countDistinct("pickup_date").alias("days"))
    .withColumn("avg_trips_per_day", round(col("trips") / col("days"), 1)))

save(dow_hour_heatmap, "dow_hour_heatmap")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `peak_zones_daily`
# MAGIC
# MAGIC `dense_rank` keeps tied zones, so a day can have more than five rows.

# COMMAND ----------

zone_day = rev.groupBy("pickup_date", "pickup_zone").agg(count("*").alias("trips"))
w = Window.partitionBy("pickup_date").orderBy(desc("trips"))

peak_zones_daily = (zone_day
    .withColumn("rank", dense_rank().over(w))
    .filter("rank <= 5"))

save(peak_zones_daily, "peak_zones_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `tip_trends`
# MAGIC
# MAGIC Credit-card trips with a recorded tip. `avg_tip_amount` is in dollars, `avg_tip_pct` is the mean of per-trip tip percentage, and `tip_to_fare_pct` is the fare-weighted ratio (total tips divided by total fares), which is less sensitive to very small fares.

# COMMAND ----------

tip_trends = (joined
    .filter((col("payment_type") == 1) & (col("tip_amount") > 0))
    .groupBy("pickup_hour")
    .agg(count("*").alias("tipped_trips"),
         round(avg("tip_amount"), 2).alias("avg_tip_amount"),
         round(avg("tip_pct"), 2).alias("avg_tip_pct"),
         round(sum("tip_amount") / sum("fare_amount") * 100, 2).alias("tip_to_fare_pct"))
    .orderBy("pickup_hour"))

save(tip_trends, "tip_trends")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. `top_routes`

# COMMAND ----------

top_routes = (rev
    .groupBy("pickup_zone", "dropoff_zone")
    .agg(count("*").alias("trips"),
         round(sum("total_amount"), 2).alias("total_revenue"),
         round(avg(when(col("trip_distance") > 0, col("trip_distance"))), 2).alias("avg_distance_mi"))
    .orderBy(desc("trips"))
    .limit(100))

save(top_routes, "top_routes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Reconciliation
# MAGIC
# MAGIC Total trips must be identical across the tables that cover the full revenue population.

# COMMAND ----------

for t in ["revenue_by_zone", "revenue_by_date_hour", "hourly_pattern", "dow_hour_heatmap"]:
    got = spark.table(f"taxi_project.gold.{t}").agg(sum("trips")).first()[0]
    print(f"{t:24s} {got:>12,}  {'OK' if got == expected_trips else 'MISMATCH'}")