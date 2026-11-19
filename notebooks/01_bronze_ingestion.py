# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Ingest raw trip data into Delta without altering values |
# MAGIC | **Input** | `/Volumes/taxi_project/bronze/data/yellow_tripdata_2026-*.parquet`, `taxi_zone_lookup.csv` |
# MAGIC | **Output** | `taxi_project.bronze.yellow_trips`, `taxi_project.bronze.taxi_zone_lookup` |
# MAGIC
# MAGIC **Design notes**
# MAGIC - An explicit schema is used instead of `inferSchema`, so a change in the source files fails loudly instead of silently changing column types.
# MAGIC - Two audit columns are added: `ingest_ts` and `source_file` (taken from the file metadata, not hard-coded).
# MAGIC - The load is a full refresh (`overwrite`), which makes the notebook safe to re-run. Incremental loading is a planned extension.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

trip_schema = StructType([
    StructField('VendorID', IntegerType(), True), 
    StructField('tpep_pickup_datetime', TimestampType(), True), 
    StructField('tpep_dropoff_datetime', TimestampType(), True), 
    StructField('passenger_count', LongType(), True), 
    StructField('trip_distance', DoubleType(), True), 
    StructField('RatecodeID', LongType(), True), 
    StructField('store_and_fwd_flag', StringType(), True), 
    StructField('PULocationID', IntegerType(), True), 
    StructField('DOLocationID', IntegerType(), True), 
    StructField('payment_type', LongType(), True), 
    StructField('fare_amount', DoubleType(), True), 
    StructField('extra', DoubleType(), True), 
    StructField('mta_tax', DoubleType(), True), 
    StructField('tip_amount', DoubleType(), True), 
    StructField('tolls_amount', DoubleType(), True), 
    StructField('improvement_surcharge', DoubleType(), True), 
    StructField('total_amount', DoubleType(), True), 
    StructField('congestion_surcharge', DoubleType(), True), 
    StructField('Airport_fee', DoubleType(), True), 
    StructField('cbd_congestion_fee', DoubleType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw files and add audit columns

# COMMAND ----------

raw = (spark.read.format("parquet")
       .schema(trip_schema)
       .load("/Volumes/taxi_project/bronze/data/yellow_tripdata_2026-*.parquet")
       .select("*",
               current_timestamp().alias("ingest_ts"),
               col("_metadata.file_path").alias("source_file")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the trips table

# COMMAND ----------

(raw.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("taxi_project.bronze.yellow_trips"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zone lookup table

# COMMAND ----------

zones = (spark.read.format("csv")
         .option("header", True)
         .option("inferSchema", True)
         .load("/Volumes/taxi_project/bronze/data/taxi_zone_lookup.csv"))

zones.write.format("delta").mode("overwrite").saveAsTable("taxi_project.bronze.taxi_zone_lookup")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation: row counts per source file and per pickup month

# COMMAND ----------

trips = spark.table("taxi_project.bronze.yellow_trips")
print("total rows:", trips.count())

trips.groupBy("source_file").count().orderBy("source_file").display()

(trips.groupBy(year("tpep_pickup_datetime").alias("year"),
               month("tpep_pickup_datetime").alias("month"))
      .count().orderBy("year", "month").display())