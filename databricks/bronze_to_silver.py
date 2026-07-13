# Databricks notebook source

# Create the schema
spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.tfl_reliability"
)

# COMMAND ----------

# Create a volume for schema and checkpoint information
spark.sql("""
CREATE VOLUME IF NOT EXISTS
workspace.tfl_reliability.checkpoints
""")

# COMMAND ----------

# Define the S3 source and target table paths
bronze_path = (
    "s3://tfl-bronze-933832340858-eu-west-2-dev/"
    "raw/line-244/"
)

schema_path = (
    "/Volumes/workspace/tfl_reliability/checkpoints/"
    "tfl_arrivals_schema"
)

checkpoint_path = (
    "/Volumes/workspace/tfl_reliability/checkpoints/"
    "tfl_arrivals_checkpoint"
)

silver_table = (
    "workspace.tfl_reliability.silver_arrivals"
)

# COMMAND ----------

# Read the Bronze JSON files using Auto Loader
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(bronze_path)
)

# COMMAND ----------

# Display the detected schema
bronze_stream.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# Transform Bronze data into a clean Silver structure
silver_stream = (
    bronze_stream
    .select(
        F.col("id").alias("arrival_id"),
        F.col("lineId").alias("line_id"),
        F.col("vehicleId").alias("vehicle_id"),
        F.col("stationName").alias("station_name"),
        F.col("destinationName").alias("destination_name"),
        F.col("towards"),
        F.col("timeToStation")
        .cast("integer")
        .alias("time_to_station_seconds"),
        (
            F.col("timeToStation").cast("double") / F.lit(60)
        ).alias("delay_minutes"),
        F.to_timestamp("expectedArrival")
        .alias("expected_arrival"),
        F.to_timestamp("timestamp")
        .alias("source_timestamp"),
        F.current_timestamp()
        .alias("processed_at")
    )
)

# COMMAND ----------

# Apply data-quality checks
valid_silver_stream = (
    silver_stream
    .filter(F.col("time_to_station_seconds").isNotNull())
    .filter(F.col("station_name").isNotNull())
    .filter(F.trim(F.col("station_name")) != "")
    .filter(F.col("time_to_station_seconds") >= 0)
)

# COMMAND ----------

# Write the cleaned data to a Silver Delta table
silver_query = (
    valid_silver_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(silver_table)
)

silver_query.awaitTermination()

# COMMAND ----------

# Display the Silver table
spark.sql("""
SELECT *
FROM workspace.tfl_reliability.silver_arrivals
LIMIT 20
""").display()

# COMMAND ----------

# Check the total number of records
spark.sql("""
SELECT COUNT(*) AS total_silver_records
FROM workspace.tfl_reliability.silver_arrivals
""").display()

# COMMAND ----------

# Data-quality test: station name
null_station_count = spark.sql("""
SELECT COUNT(*) AS invalid_count
FROM workspace.tfl_reliability.silver_arrivals
WHERE station_name IS NULL
   OR TRIM(station_name) = ''
""").first()["invalid_count"]

assert null_station_count == 0, (
    f"Data quality failed: "
    f"{null_station_count} invalid station names"
)

print("PASS: station_name contains no null or blank values")

# COMMAND ----------

# Data-quality test: timeToStation
null_time_count = spark.sql("""
SELECT COUNT(*) AS invalid_count
FROM workspace.tfl_reliability.silver_arrivals
WHERE time_to_station_seconds IS NULL
""").first()["invalid_count"]

assert null_time_count == 0, (
    f"Data quality failed: "
    f"{null_time_count} null timeToStation values"
)

print("PASS: timeToStation contains no null values")

# COMMAND ----------

# Data-quality test: negative values
negative_time_count = spark.sql("""
SELECT COUNT(*) AS invalid_count
FROM workspace.tfl_reliability.silver_arrivals
WHERE time_to_station_seconds < 0
""").first()["invalid_count"]

assert negative_time_count == 0, (
    f"Data quality failed: "
    f"{negative_time_count} negative values"
)

print("PASS: no negative timeToStation values")