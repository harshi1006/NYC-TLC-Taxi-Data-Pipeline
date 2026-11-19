# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Data Profiling
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Understand the data before defining cleaning rules |
# MAGIC | **Input** | `taxi_project.bronze.yellow_trips` |
# MAGIC | **Output** | Findings that drive the Silver rules (summarized in `docs/data_quality_findings.md`) |
# MAGIC
# MAGIC Profiling of value ranges (distance, speed) is run on a *structurally valid* base (positive fare, dropoff not before pickup) that has **no outlier filter applied**. Profiling an already outlier-filtered dataset to choose an outlier threshold would be circular.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.table("taxi_project.bronze.yellow_trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Date range and null counts

# COMMAND ----------

df.agg(min("tpep_pickup_datetime").alias("min_pickup"),
       max("tpep_pickup_datetime").alias("max_pickup")).display()

df.select([count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `passenger_count`: null vs zero
# MAGIC
# MAGIC About 3.06M rows have a null `passenger_count`, versus about 40k with zero. The nulls co-occur with null `RatecodeID`, `store_and_fwd_flag` and `congestion_surcharge`, and are concentrated in `payment_type = 0` (Flex Fare trips, per the TLC data dictionary), whose meter-related fields are not populated. Fares and distances on these rows are normal, so they are valid trips and are flagged (`is_flex_fare`), not rejected. Only `passenger_count = 0` is treated as invalid.

# COMMAND ----------

df.groupBy("passenger_count").count().orderBy(desc("count")).display()

(df.groupBy("VendorID")
   .agg(count("*").alias("rows"),
        count(when(col("passenger_count").isNull(), 1)).alias("null_passenger_count"))
   .display())

(df.groupBy("payment_type")
   .agg(count("*").alias("rows"),
        count(when(col("passenger_count").isNull(), 1)).alias("null_passenger_count"))
   .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `trip_distance` distribution
# MAGIC
# MAGIC A z-score rule was evaluated and rejected: the distribution is right-skewed, and mean and standard deviation are inflated by a small number of extreme values, so the cutoff either misses the garbage or removes legitimate airport and out-of-city trips. Threshold counts on the unfiltered base show a natural, smoothly decreasing tail up to about 100 miles, then a flat floor of roughly 300 rows spanning 500 to over 30,000 miles, which indicates sentinel or meter-error values.

# COMMAND ----------

base = df.filter((col("fare_amount") > 0) &
                 (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime")))

print(base.approxQuantile("trip_distance", [0.5, 0.95, 0.99, 0.999], 0.001))

base.select([count(when(col("trip_distance") > t, 1)).alias(f"gt_{t}")
             for t in [25, 30, 50, 100, 200, 500, 1000, 3000, 30000]]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Average speed
# MAGIC
# MAGIC Trips of 100-200 miles contain two groups: long out-of-city trips with plausible speeds (roughly 45-65 mph and a proportional fare) and meter errors implying several hundred mph. A speed cap separates them better than a distance cap alone.

# COMMAND ----------

speed = (base
    .filter((col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) & (col("trip_distance") > 0))
    .withColumn("mph", col("trip_distance") /
                ((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600)))

speed.select([count(when(col("mph") > t, 1)).alias(f"gt_{t}_mph")
              for t in [60, 80, 100, 200]]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dropoff equal to or before pickup, by vendor
# MAGIC
# MAGIC Almost all rows with `dropoff == pickup` belong to Vendor 7 (Helix), and their fare and distance look normal, so the trips are real but the duration is unknown. Strictly negative durations (`dropoff < pickup`) affect only 5 rows.

# COMMAND ----------

df.filter(col("tpep_dropoff_datetime") == col("tpep_pickup_datetime")).groupBy("VendorID").count().display()
df.filter(col("tpep_dropoff_datetime") <  col("tpep_pickup_datetime")).groupBy("VendorID").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Zero distance with a positive fare
# MAGIC
# MAGIC Roughly 278k rows have `trip_distance <= 0` with a normal fare (median about $23), so the meter did not record distance but the trip happened. These rows are flagged (`distance_missing`), not rejected.

# COMMAND ----------

(df.filter((col("trip_distance") <= 0) & (col("fare_amount") > 0))
   .select("fare_amount").describe().display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Non-positive fares
# MAGIC
# MAGIC Most negative fares carry `payment_type` 3 (no charge) or 4 (dispute): these are reversal or adjustment records rather than trips, so they are rejected.

# COMMAND ----------

(df.filter(col("fare_amount") < 0)
   .groupBy("payment_type").count().orderBy(desc("count")).display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Duplicates
# MAGIC
# MAGIC The data has no trip identifier. A duplicate is defined as a match on vendor, pickup time, dropoff time, both zones, distance and total amount. A looser key (vendor, zones, amount only) produced thousands of false positives, because distinct trips frequently share a flat fare.

# COMMAND ----------

dup_key = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
           "PULocationID", "DOLocationID", "trip_distance", "total_amount"]

print("duplicate groups:", df.groupBy(dup_key).count().filter("count > 1").count())