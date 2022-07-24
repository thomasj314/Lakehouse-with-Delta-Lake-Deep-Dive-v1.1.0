# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Batch Write to Delta Tables
# MAGIC 
# MAGIC **Objective:** Append files to an existing Delta Table

# COMMAND ----------

# MAGIC %md ## Classroom Setup
# MAGIC Run the following cell to configure this course's environment:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Load New Data
# MAGIC 
# MAGIC Within the context of our data ingestion pipeline, this is the addition of new raw files to our Single Source of Truth.
# MAGIC 
# MAGIC We begin by loading the data from the file **health_tracker_data_2020_2.json**, using the **`format("json")`** option as before.

# COMMAND ----------

file_path = f"{DA.paths.raw}/health_tracker_data_2020_2.json"

health_tracker_data_2020_2_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Transform the Data
# MAGIC We perform the same data engineering on the data:
# MAGIC - Use the from_unixtime Spark SQL function to transform the unixtime into a time string
# MAGIC - Cast the time column to type timestamp to replace the column time
# MAGIC - Cast the time column to type date to create the column dte

# COMMAND ----------

processed_df = DA.process_health_tracker_data(health_tracker_data_2020_2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Append the Data to the health_tracker_processed Delta table
# MAGIC 
# MAGIC We do this using **`mode("append")`**.
# MAGIC 
# MAGIC Note that it is not necessary to perform any action on the Metastore.

# COMMAND ----------

(processed_df.write
             .mode("append")
             .format("delta")
             .save(DA.paths.processed))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the Commit Using Time Travel
# MAGIC Delta Lake can query an earlier version of a Delta table using a feature known as time travel. 
# MAGIC 
# MAGIC Here, we query the data as of version 0, that is, the initial conversion of the table from Parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: View the table as of Version 0
# MAGIC This is done by specifying the option **`"versionAsOf"`** as **`0`**. 
# MAGIC 
# MAGIC When we time travel to Version 0, we see **only** the first month of data, five device measurements, 24 hours a day for 31 days.

# COMMAND ----------

(spark.read
      .option("versionAsOf", 0)
      .format("delta")
      .load(DA.paths.processed)
      .count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Count the Most Recent Version
# MAGIC When we query the table without specifying a version, it shows the latest version of the table and includes the new records added.
# MAGIC 
# MAGIC When we look at the current version, we expect to see two months of data: January 2020 and February 2020. 
# MAGIC 
# MAGIC The data should include the following records: 
# MAGIC 
# MAGIC **5 devices * 60 days * 24 hours = 7200 records**
# MAGIC 
# MAGIC Note that the range of data includes the month of February during a leap year. 29 days in Feb plus 31 in January gives us 60 days total.

# COMMAND ----------

(spark.read
      .format("delta")
      .load(DA.paths.processed)
      .count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note that we do not have a correct count. 
# MAGIC 
# MAGIC We are missing 72 records.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
