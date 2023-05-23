# Databricks notebook source
dbutils.widgets.text("silver_db_name", "", "silver_db_name")
silver_db_name= dbutils.widgets.get("silver_db_name")
silver_db_name

# COMMAND ----------

dbutils.jobs.taskValues.get(taskKey = "Bronze", key = "notebook", default = 42, debugValue = 0)


# COMMAND ----------

df_customers = spark.read.table("bronze.customer_raw")

# COMMAND ----------

# Assuming you have a DataFrame named "df" with the given column names

# Alias the columns
df_customers_transform1 = df_customers.withColumnRenamed("Index", "idx") \
       .withColumnRenamed("User Id", "user_id") \
       .withColumnRenamed("First Name", "first_name") \
       .withColumnRenamed("Last Name", "last_name") \
       .withColumnRenamed("Sex", "gender") \
       .withColumnRenamed("Email", "email") \
       .withColumnRenamed("Phone", "phone") \
       .withColumnRenamed("Date of birth", "dob") \
       .withColumnRenamed("Job Title", "job_title")


# COMMAND ----------

df_customers_transform2=df_customers_transform1.drop('idx')

# COMMAND ----------

df_customers_transform2.write.format("parquet").mode("overwrite").saveAsTable(f"{silver_db_name}.customer_cleaned")
