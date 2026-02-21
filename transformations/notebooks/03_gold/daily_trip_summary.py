# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import count, max, min, avg, sum, round
from dateutil.relativedelta import relativedelta
from datetime import date
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=3)

# COMMAND ----------

# Load the enriched trip dataset 
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime > '{two_months_ago_start}'")

# COMMAND ----------
from pyspark.sql import functions as F
# DBTITLE 1,Cell 4
# Aggregate trip data by pickup date with key metrics
df = (
    df
    .groupBy(F.to_date("tpep_pickup_datetime").alias("pickup_date"))
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("passenger_count"), 1).alias("average_passengers"),
        F.round(F.avg("trip_distance"), 1).alias("average_distance"),
        F.round(F.avg("fare_amount"), 2).alias("average_fare_per_trip"),
        F.max("fare_amount").alias("max_fare"),
        F.min("fare_amount").alias("min_fare"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue")
    )
)

# COMMAND ----------

# Write the daily summary to a Unity Catalog managed Delta table in the gold schema
df.write.mode("append").option("mergeSchema", "true").saveAsTable("nyctaxi.03_gold.daily_trip_summary")