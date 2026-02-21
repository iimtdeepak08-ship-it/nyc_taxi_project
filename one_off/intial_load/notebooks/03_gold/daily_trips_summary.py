# Databricks notebook source
from pyspark.sql.functions import max,min,avg,sum,count,round

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
display(df)

# COMMAND ----------

df = df.\
    groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
    agg(
        count("*").alias("total_trips"),
        round(avg("passenger_count"),1).alias("avg_passenger"),
        round(avg("trip_distance"),1).alias("average_distance"),
        round(avg("fare_amount"),1).alias("average_fare_per_trip"),
        max("fare_amount").alias("max_fare_per_trip"),
        min("fare_amount").alias("min_fare_per_trip"),
        round(sum("total_amount"),2).alias("total_amount")
       
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")

# COMMAND ----------

spark.read.table("nyctaxi.03_gold.daily_trip_summary").display()

# COMMAND ----------

