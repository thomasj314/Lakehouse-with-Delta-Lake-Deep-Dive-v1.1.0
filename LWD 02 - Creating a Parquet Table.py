# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Parquet Table

# COMMAND ----------

# MAGIC %md ## Classroom Setup
# MAGIC Run the following cell to configure this course's environment:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC Reload data to DataFrame

# COMMAND ----------

print("Processing Directory:")
print(DA.paths.processed)

file_path = f"{DA.paths.raw}/health_tracker_data_2020_1.json"
print("\nFile Path:")
print(file_path)

health_tracker_data_2020_1_df = spark.read.format("json").load(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a Parquet Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Make Idempotent
# MAGIC First, we remove the files in the "processed" directory.
# MAGIC 
# MAGIC Then, we drop the table we will create from the Metastore if it exists.
# MAGIC 
# MAGIC This step will make the notebook idempotent. In other words, it could be run more than once without throwing errors or introducing extra files.
# MAGIC 
# MAGIC 🚨 **NOTE** Throughout this lesson, we'll be writing files to the root location of the Databricks File System (DBFS). In general, best practice is to write files to your cloud object storage. We use DBFS root here for demonstration purposes.

# COMMAND ----------

dbutils.fs.rm(DA.paths.processed, recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_tracker_processed

# COMMAND ----------

# MAGIC %md Quick Note: with the help of the **`%sql`** directive, we can use SQL as opposed to the Python APIs.
# MAGIC 
# MAGIC As you will see in the next cell, we can also do it in Python.
# MAGIC 
# MAGIC You will see both of these methods used throught this course.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS health_tracker_processed")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Transform the Data
# MAGIC We perform transformations by selecting columns in the following ways:
# MAGIC - use **`from_unixtime`** to transform **`"time"`**, cast as a **`date`**, and aliased to **`dte`**
# MAGIC - use **`from_unixtime`** to transform **`"time"`**, cast as a **`timestamp`**, and aliased to **`time`**
# MAGIC - **`heartrate`** is selected as is
# MAGIC - **`name`** is selected as is
# MAGIC - cast **`"device_id"`** as an integer aliased to **`p_device_id`**

# COMMAND ----------

# TODO
from pyspark.sql.functions import col, from_unixtime

def process_health_tracker_data(dataframe):
  return dataframe.select(
    from_unixtime("time").cast("date").alias("dte"),
    from_unixtime("time").cast("timestamp").alias("time"),
    "heartrate",
    "name",
    col("device_id").cast("integer").alias("p_device_id")
  )
processed_df = process_health_tracker_data(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Write the Files to the processed directory
# MAGIC Note that we are partitioning the data by device id.
# MAGIC 
# MAGIC 1. use **`format("parquet")`**
# MAGIC 1. partition by **`"p_device_id"`**

# COMMAND ----------

#TODO
(processed_df.write
 .mode("overwrite")
 .format("parquet")
 .partitionBy("p_device_id")
 .save(DA.paths.processed))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Register the Table in the Metastore
# MAGIC Next, use Spark SQL to register the table in the metastore.
# MAGIC 
# MAGIC Upon creation we specify the format as parquet and that the location where the parquet files were written should be used.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed
# MAGIC   USING PARQUET
# MAGIC   LOCATION "${DA.paths.processed}";

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 5: Verify Parquet-based Data Lake table
# MAGIC 
# MAGIC Count the records in the **health_tracker_processed** Table

# COMMAND ----------

# TODO
health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the count does not return results.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6: Register the Partitions
# MAGIC 
# MAGIC Per best practice, we have created a partitioned table.
# MAGIC 
# MAGIC However, if you create a partitioned table from existing data, Spark SQL does not automatically discover the partitions and register them in the Metastore.
# MAGIC 
# MAGIC **`MSCK REPAIR TABLE`** will register the partitions in the Hive Metastore. Learn more about this command in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-repair-table.html" target="_blank">
# MAGIC the docs</a>.

# COMMAND ----------

# MAGIC %sql 
# MAGIC MSCK REPAIR TABLE health_tracker_processed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 7: Count the Records in the health_tracker_processed table
# MAGIC 
# MAGIC Count the records in the **health_tracker_processed** table.
# MAGIC 
# MAGIC With the table repaired and the partitions registered, we now have results.
# MAGIC We expect there to be 3720 records: five device measurements, 24 hours a day for 31 days.

# COMMAND ----------

# TODO
total = health_tracker_processed.count()
assert total == 3720, f"Expected 3720 records, found {total}"
print(f"Total: {total}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
