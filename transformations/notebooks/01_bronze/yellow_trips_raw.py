# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from datetime import date

# COMMAND ----------

# DBTITLE 1,Cell 2
# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=3)
formatted_date = two_months_ago.strftime("%Y-%m")

# Check if the target month exists, otherwise use the most recent available month
base_path = "/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow"
target_path = f"{base_path}/{formatted_date}"

try:
    # Try to access the target month
    dbutils.fs.ls(target_path)
    data_path = target_path
    print(f"Using target month: {formatted_date}")
except:
    # If target month doesn't exist, find the most recent available month
    available_months = [f.name.rstrip('/') for f in dbutils.fs.ls(base_path) if f.name.endswith('/')]
    available_months.sort(reverse=True)
    if available_months:
        most_recent = available_months[0]
        data_path = f"{base_path}/{most_recent}"
        print(f"Target month {formatted_date} not found. Using most recent available: {most_recent}")
    else:
        raise Exception(f"No data available in {base_path}")

# Read all Parquet files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(data_path)

# COMMAND ----------

# DBTITLE 1,Cell 3
# Add a column to capture when the data was processed
df = df.withColumn("processed_time", current_timestamp())

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new data
df.write.mode("append").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")