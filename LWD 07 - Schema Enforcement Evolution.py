# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Enforcement & Evolution
# MAGIC 
# MAGIC **Objective:** Work with evolving schema

# COMMAND ----------

# MAGIC %md ## Classroom Setup
# MAGIC Run the following cell to configure this course's environment:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Health tracker data sample
# MAGIC 
# MAGIC <code>
# MAGIC {"device_id":0,"heartrate":57.6447293596,"name":"Deborah Powell","time":1.5830208E9,"device_type":"version 2"}<br/>
# MAGIC {"device_id":0,"heartrate":57.6175546013,"name":"Deborah Powell","time":1.5830244E9,"device_type":"version 2"}<br/>
# MAGIC {"device_id":0,"heartrate":57.8486376876,"name":"Deborah Powell","time":1.583028E9,"device_type":"version 2"}<br/>
# MAGIC {"device_id":0,"heartrate":57.8821378637,"name":"Deborah Powell","time":1.5830316E9,"device_type":"version 2"}<br/>
# MAGIC {"device_id":0,"heartrate":59.0531490807,"name":"Deborah Powell","time":1.5830352E9,"device_type":"version 2"}<br/>
# MAGIC </code>
# MAGIC This shows a sample of the health tracker data we will be using. Note that each line is a valid JSON object.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Health tracker data schema
# MAGIC The data has the following schema:
# MAGIC 
# MAGIC 
# MAGIC | Column     | Type      |
# MAGIC |------------|-----------|
# MAGIC | name       | string    |
# MAGIC | heartrate  | double    |
# MAGIC | device_id  | int       |
# MAGIC | time       | long      |
# MAGIC | device_type| string    |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load the Next Month of Data
# MAGIC We begin by loading the data from the file **health_tracker_data_2020_3.json**, using the **`format("json")`** option as before.

# COMMAND ----------

file_path = f"{DA.paths.raw}/health_tracker_data_2020_3.json"


health_tracker_data_2020_3_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Transform the Data
# MAGIC 
# MAGIC We perform the same data engineering on the data:
# MAGIC - Use the **`from_unixtime`** Spark SQL function to transform the unixtime into a time string
# MAGIC - Cast the time column to type **`timestamp`** to replace the column **`time`**
# MAGIC - Cast the time column to type **`date`** to create the column **`dte`**

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime
def process_health_tracker_data(dataframe):
    return (
     dataframe
     .select(
         from_unixtime("time").cast("date").alias("dte"),
         from_unixtime("time").cast("timestamp").alias("time"),
         "heartrate",
         "name",
         col("device_id").cast("integer").alias("p_device_id"),
         "device_type"
       )
     )
processedDF = process_health_tracker_data(health_tracker_data_2020_3_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Append the Data to the health_tracker_processed Delta table
# MAGIC We do this using **`mode("append")`**.

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit

try:
  (
    processedDF.write
    .mode("append")
    .format("delta")
    .save(DA.paths.processed)
  )
except AnalysisException as error:
  print("Analysis Exception:")
  print(error)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema Mismatch
# MAGIC The command above produces the error: 
# MAGIC 
# MAGIC **AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: ...)**

# COMMAND ----------

# MAGIC %md
# MAGIC To enable schema migration using DataFrameWriter or DataStreamWriter, set: **`option("mergeSchema", "true")`**.
# MAGIC 
# MAGIC For other operations, set the session configuration **`spark.databricks.delta.schema.autoMerge.enabled`** to **`"true"`**.
# MAGIC 
# MAGIC See <a href="https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html" target="_blank">the documentation</a> specific to the operation for details.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Is Schema Enforcement?
# MAGIC Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by rejecting writes to a table that do not match the table’s schema. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Is Schema Evolution?
# MAGIC 
# MAGIC Schema evolution is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time.
# MAGIC 
# MAGIC Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Append the Data with Schema Evolution to the health_tracker_processed Delta table
# MAGIC We do this using **`mode("append")`**.

# COMMAND ----------

# TODO
(processedDF.write
.mode("append")
.option("mergeSchema","True")
.format("delta")
.save(DA.paths.processed))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Commit
# MAGIC ### Step 1: Count the Most Recent Version

# COMMAND ----------

total = spark.read.table("health_tracker_processed").count()
assert total == 10920, f"Expected 10920, found {total}"
print(f"Total: {total}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
