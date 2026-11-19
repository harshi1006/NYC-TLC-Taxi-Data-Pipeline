# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Transform
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Apply data-quality rules, derive analysis columns, and quarantine invalid rows |
# MAGIC | **Input** | `taxi_project.bronze.yellow_trips` |
# MAGIC | **Output** | `taxi_project.silver.yellow_trips` (valid rows), `taxi_project.silver.yellow_trips_rejected` (rejected rows with `reject_reason`) |
# MAGIC
# MAGIC **Processing order:** flags and derived columns, then the `reject_reason` rules, then the split and write, then reconciliation. Derived columns come first because the speed and duration rules depend on them.
# MAGIC
# MAGIC **Rule priority.** Rules are evaluated in the order below and the first match sets the reason, so each rejected row carries exactly one reason.
# MAGIC
# MAGIC | # | Rule | `reject_reason` |
# MAGIC |---|---|---|
# MAGIC | 1 | Pickup or dropoff timestamp is null | Invalid Timestamp |
# MAGIC | 2 | Pickup outside January-March 2026 | Invalid Date |
# MAGIC | 3 | Dropoff earlier than pickup | Dropoff Before Pickup |
# MAGIC | 4 | `passenger_count = 0` (null is allowed) | Invalid Passenger Count |
# MAGIC | 5 | `fare_amount <= 0` or null | Invalid Fare Amount |
# MAGIC | 6 | `total_amount <= 0` or null | Invalid Total Amount |
# MAGIC | 7 | `trip_distance` above cap | Distance Above Cap |
# MAGIC | 8 | Duration above cap | Duration Above Cap |
# MAGIC | 9 | Average speed above cap | Unrealistic Speed |
# MAGIC
# MAGIC **Flagged, not rejected**
# MAGIC
# MAGIC | Column | Condition | Reason |
# MAGIC |---|---|---|
# MAGIC | `is_flex_fare` | `payment_type = 0` | Meter fields are not populated for Flex Fare trips |
# MAGIC | `duration_missing` | `dropoff = pickup` | Duration unknown (all Vendor 7 trips); fare and distance are normal |
# MAGIC | `distance_missing` | `trip_distance <= 0` | Distance not recorded; fare is normal |
# MAGIC
# MAGIC **Unknown values are left null, not set to 0.** `trip_duration_min` and `avg_speed_mph` are null when duration or distance is unknown, so aggregates such as `avg()` ignore them instead of being biased toward zero.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# Thresholds chosen from the profiling in notebook 02
START_TS = "2026-01-01"   # inclusive
END_TS   = "2026-04-01"   # exclusive
MAX_DISTANCE_MI = 500     # above this the profile shows a flat floor of sentinel/meter-error values
MAX_DURATION_HR = 12
MAX_SPEED_MPH   = 80      # above this average speed is not physically plausible in NYC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Flags and derived columns

# COMMAND ----------

bronze = spark.table("taxi_project.bronze.yellow_trips")

duration_sec = (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"))

df = (bronze
    .withColumn("is_flex_fare",     col("payment_type") == 0)
    .withColumn("duration_missing", col("tpep_dropoff_datetime") == col("tpep_pickup_datetime"))
    .withColumn("distance_missing", col("trip_distance") <= 0)
    .withColumn("trip_duration_min", when(duration_sec > 0, round(duration_sec / 60, 2)))
    .withColumn("avg_speed_mph",
                when((duration_sec > 0) & (col("trip_distance") > 0),
                       col("trip_distance") / (duration_sec / 3600)))
    .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    .withColumn("tip_pct",
                when(col("fare_amount") > 0,
                       col("tip_amount") / col("fare_amount") * 100)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reject reason and validity flag

# COMMAND ----------

pickup  = col("tpep_pickup_datetime")
dropoff = col("tpep_dropoff_datetime")

df = df.withColumn("reject_reason",
      when(pickup.isNull() | dropoff.isNull(), "Invalid Timestamp")
     .when((pickup < lit(START_TS).cast("timestamp")) |
           (pickup >= lit(END_TS).cast("timestamp")), "Invalid Date")
     .when(dropoff < pickup, "Dropoff Before Pickup")
     .when(col("passenger_count") == 0, "Invalid Passenger Count")
     .when(col("fare_amount").isNull() | (col("fare_amount") <= 0), "Invalid Fare Amount")
     .when(col("total_amount").isNull() | (col("total_amount") <= 0), "Invalid Total Amount")
     .when(col("trip_distance") > MAX_DISTANCE_MI, "Distance Above Cap")
     .when(duration_sec > MAX_DURATION_HR * 3600, "Duration Above Cap")
     .when(col("avg_speed_mph") > MAX_SPEED_MPH, "Unrealistic Speed")
     .otherwise("Valid")
).withColumn("is_valid", col("reject_reason") == "Valid")

df.groupBy("reject_reason").count().orderBy(desc("count")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split and write (quarantine pattern)

# COMMAND ----------

valid_df    = df.filter("is_valid").drop("is_valid", "reject_reason")
rejected_df = df.filter(~col("is_valid")).drop("is_valid")

(valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("taxi_project.silver.yellow_trips"))

(rejected_df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("taxi_project.silver.yellow_trips_rejected"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reconciliation
# MAGIC
# MAGIC Every Bronze row must end up in exactly one of the two Silver tables.
# MAGIC
# MAGIC The Silver tables are not partitioned. The dataset covers three months (about 11M rows), so partitioning would create many small files without improving pruning.

# COMMAND ----------

bronze_n   = spark.table("taxi_project.bronze.yellow_trips").count()
valid_n    = spark.table("taxi_project.silver.yellow_trips").count()
rejected_n = spark.table("taxi_project.silver.yellow_trips_rejected").count()

print(f"bronze={bronze_n:,}  valid={valid_n:,}  rejected={rejected_n:,}")
assert bronze_n == valid_n + rejected_n, "Row count mismatch"

spark.table("taxi_project.silver.yellow_trips_rejected") \
     .groupBy("reject_reason").count().orderBy(desc("count")).display()