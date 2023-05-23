# Databricks notebook source
from pyspark.sql.functions import year, current_date

# COMMAND ----------

dbutils.widgets.text("gold_db_name", "", "gold_db_name")
gold_db_name= dbutils.widgets.get("gold_db_name")
gold_db_name


# COMMAND ----------

df_customers_silver = spark.read.table("silver.customer_cleaned")

# COMMAND ----------

current_year = year(current_date())
df_gold_final = df_customers_silver.withColumn("Age", current_year - year(df_customers_silver["dob"]))
df_gold_final.write.format("parquet").mode("overwrite").saveAsTable(f"{gold_db_name}.customer_gold")

# COMMAND ----------

