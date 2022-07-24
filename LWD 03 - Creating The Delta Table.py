# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Delta Tables
# MAGIC  
# MAGIC Objective: Convert a Parquet-based table to a Delta table. 
# MAGIC 
# MAGIC Recall that a Delta table consists of three things:
# MAGIC - the data files kept in object storage (i.e. AWS S3, Azure Data Lake Storage)
# MAGIC - the Delta Transaction Log saved with the data files in object storage
# MAGIC - a table registered in the Metastore. This step is optional, but usually recommended. 

# COMMAND ----------

# MAGIC %md ## Classroom Setup
# MAGIC Run the following cell to configure this course's environment:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Table
# MAGIC With Delta Lake, you create tables:
# MAGIC * When ingesting new files into a Delta Table for the first time
# MAGIC * By transforming an existing Parquet-based data lake table to a Delta table 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **NOTE:**  Throughout this section, we'll be writing files to the root location of the Databricks File System (DBFS).
# MAGIC In general, best practice is to write files to your cloud object storage.  We use DBFS root here for demonstration purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Describe the health_tracker_processed Table
# MAGIC Before we convert the **health_tracker_processed** table, let's use the Spark SQL **`DESCRIBE`** command, with the optional parameter **`EXTENDED`**, to display the attributes of the table.
# MAGIC 
# MAGIC Note that the table has the **provider** field listed as "**PARQUET**".
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"/> You will have to scroll to the **#Detailed Table Information** to find the provider. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed
# MAGIC     USING PARQUET
# MAGIC     LOCATION "${DA.paths.processed}";
# MAGIC     
# MAGIC DESCRIBE EXTENDED health_tracker_processed;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert an Existing Parquet Table to a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC #### Step 1: Convert the Files to Delta Files
# MAGIC 
# MAGIC First, we'll convert the files in-place to Delta files. The conversion creates a Delta Lake transaction log that tracks associated files. 

# COMMAND ----------

from delta.tables import DeltaTable

parquet_table = f"parquet.`{DA.paths.processed}`"
partitioning_scheme = "p_device_id int"

DeltaTable.convertToDelta(spark, parquet_table, partitioning_scheme)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Register the Delta Table
# MAGIC At this point, the files containing our records have been converted to Delta files.
# MAGIC 
# MAGIC The Metastore, however, has not been updated to reflect the change.
# MAGIC 
# MAGIC To change this we re-register the table in the Metastore.
# MAGIC 
# MAGIC The Spark SQL command will automatically infer the data schema by reading the footers of the Delta files.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed
# MAGIC     USING DELTA
# MAGIC     LOCATION "${DA.paths.processed}";

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Add column comments
# MAGIC 
# MAGIC Comments can make your tables easier to read and maintain. We use an `ALTER TABLE` command to add new column comments to the exiting Delta table.  

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE
# MAGIC   health_tracker_processed
# MAGIC REPLACE COLUMNS
# MAGIC   (dte DATE COMMENT "Format: YYYY/mm/dd", 
# MAGIC   time TIMESTAMP, 
# MAGIC   heartrate DOUBLE,
# MAGIC   name STRING COMMENT "Format: First Last",
# MAGIC   p_device_id INT COMMENT "range 0 - 4")
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Describe the health_tracker_processed Table
# MAGIC We can verify that comments have been added to the table by using the **`DESCRIBE`** Spark SQL command followed by the optional parameter, **`EXTENDED`**. 
# MAGIC 
# MAGIC You can see the column comments that we added as well as some additional information. Scrool down to confirm that the new table had Delta listed as the provider.  

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED health_tracker_processed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Count the Records in the health_tracker_processed table
# MAGIC We count the records in **health_tracker_processed** with Apache Spark.
# MAGIC 
# MAGIC With Delta Lake, the Delta table requires no repair and is immediately ready for use.

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_processed")
total = health_tracker_processed.count()

assert total == 3720, f"Expected 3720 files, found {total}"
print(f"Total: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a New Delta Table
# MAGIC Next, we'll create a new Delta table. We'll do this by creating an aggregate table
# MAGIC 
# MAGIC from the data in the health_track_processed Delta table we just created.
# MAGIC 
# MAGIC Within the context of our EDSS, this is a downstream aggregate table or data mart.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Remove files in the health_tracker_user_analytics directory
# MAGIC This step will make the notebook idempotent.
# MAGIC 
# MAGIC In other words, it could be run more than once without throwing errors or introducing extra files.

# COMMAND ----------

user_analytics_path = f"{DA.paths.working_dir}/gold/health_tracker_user_analytics"
spark.conf.set("my.paths.user_analytics_path", user_analytics_path)   # Make available in %sql below

spark.sql("DROP TABLE IF EXISTS health_tracker_gold_user_analytics;") # Drop the table
dbutils.fs.rm(user_analytics_path, recurse=True)                      # Then remove any files

print(f"Location: {user_analytics_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Create an Aggregate DataFrame
# MAGIC The subquery used to define the table is an aggregate query over the **health_tracker_processed** Delta table using summary statistics for each device.

# COMMAND ----------

from pyspark.sql.functions import col, avg, max, stddev

health_tracker_gold_user_analytics = (
  health_tracker_processed
  .groupby("p_device_id")
  .agg(avg(col("heartrate")).alias("avg_heartrate"),
       max(col("heartrate")).alias("max_heartrate"),
       stddev(col("heartrate")).alias("stddev_heartrate"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Write the Delta Files

# COMMAND ----------

(health_tracker_gold_user_analytics.write
                                   .format("delta")
                                   .mode("overwrite")
                                   .save(user_analytics_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Register the Delta table in the Metastore
# MAGIC Finally, register this table in the Metastore.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE health_tracker_gold_user_analytics
# MAGIC     USING DELTA
# MAGIC     LOCATION "${my.paths.user_analytics_path}";

# COMMAND ----------

display(spark.read.table("health_tracker_gold_user_analytics"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configuring the Visualization
# MAGIC Create a Databricks visualization to view the aggregate sensor data.
# MAGIC We have used the following options to configure the visualization:
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | Keys | **p_device_id** |
# MAGIC | Series groupings | **None** |
# MAGIC | Values | **max_heartrate**, **avg_heartrate**, **stddev_heartrate** |
# MAGIC | Aggregation | **SUM** |
# MAGIC | Display Type | **Bar Chart** |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
