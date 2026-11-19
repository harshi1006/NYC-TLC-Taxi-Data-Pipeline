# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Create the catalog objects used by the pipeline |
# MAGIC | **Input** | None |
# MAGIC | **Output** | Schemas `taxi_project.bronze`, `taxi_project.silver`, `taxi_project.gold`; volume `taxi_project.bronze.data` |
# MAGIC
# MAGIC Source data (NYC TLC Yellow Taxi trip records for January-March 2026 in Parquet format, and the Taxi Zone Lookup CSV) is uploaded to the volume before running notebook 01.

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS taxi_project.{schema}")

spark.sql("CREATE VOLUME IF NOT EXISTS taxi_project.bronze.data")