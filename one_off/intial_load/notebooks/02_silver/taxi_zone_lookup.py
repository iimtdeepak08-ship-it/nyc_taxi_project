# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit,col
from pyspark.sql.types import TimestampType,IntegerType

# COMMAND ----------

df = spark.read.format('csv').option('header',True).load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")
df.display()

# COMMAND ----------

df = df.select(
    col('LocationID').cast(IntegerType()).alias('LocationID'),
    col('Borough').alias('Borough'),
    col('Zone').alias("zone"),
    col('service_zone'),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
    )

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

spark.read.table("nyctaxi.02_silver.taxi_zone_lookup").display()