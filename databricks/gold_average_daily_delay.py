# Databricks notebook source

from pyspark.sql import functions as F

silver_table = "workspace.tfl_reliability.silver_arrivals"

silver_df = spark.table(silver_table)

# COMMAND ----------

gold_df = (
    silver_df
    .withColumn(
        "arrival_date",
        F.to_date(
            F.coalesce(
                F.col("expected_arrival"),
                F.col("processed_at")
            )
        )
    )
    .filter(F.col("arrival_date").isNotNull())
    .filter(F.col("station_name").isNotNull())
    .filter(F.col("delay_minutes").isNotNull())
    .groupBy(
        "arrival_date",
        "station_name"
    )
    .agg(
        F.round(
            F.avg("delay_minutes"),
            2
        ).alias("average_daily_delay_minutes"),
        F.round(
            F.max("delay_minutes"),
            2
        ).alias("maximum_delay_minutes"),
        F.count("*").alias("arrival_record_count")
    )
)

# COMMAND ----------

gold_table = (
    "workspace.tfl_reliability."
    "gold_average_daily_delay_by_station"
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM workspace.tfl_reliability.gold_average_daily_delay_by_station
    ORDER BY
        arrival_date DESC,
        average_daily_delay_minutes DESC
    """)
)