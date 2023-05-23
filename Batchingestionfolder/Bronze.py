# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

dbutils.jobs.taskValues.set(key = 'notebook', value = 'iam bronze task want to communicate to silver')

# COMMAND ----------

dbutils.widgets.text("bronze_db_name", "", "bronze_db_name")
bronze_db_name= dbutils.widgets.get("bronze_db_name")
bronze_db_name

# COMMAND ----------

dbutils.widgets.text("pathofcsv", "", "pathofcsv")
pathofcsv= dbutils.widgets.get("pathofcsv")
pathofcsv

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CSVReader").getOrCreate()

# COMMAND ----------

# Read CSV file with inferred schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f'{pathofcsv}')

# Print the inferred schema
df.printSchema()

# Show the data


# COMMAND ----------

df.write.format("parquet").mode("overwrite").saveAsTable(f"{bronze_db_name}.customer_raw")


# COMMAND ----------

