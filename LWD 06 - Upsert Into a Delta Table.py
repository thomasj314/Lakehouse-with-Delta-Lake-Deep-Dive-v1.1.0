# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Upsert Into a Delta Table
# MAGIC 
# MAGIC **Objective:**  Repair records with an upsert
# MAGIC 
# MAGIC In the previous lesson, we identified two issues with the **health_tracker_processed** table:
# MAGIC - There were 72 missing records
# MAGIC - There were 60 records with broken readings
# MAGIC 
# MAGIC In this lesson, we will repair the table by modifying the **health_tracker_processed** table.

# COMMAND ----------

# MAGIC %md ## Classroom Setup
# MAGIC Run the following cell to configure this course's environment:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prepare Updates DataFrame
# MAGIC To repair the broken sensor readings (less than zero), we'll interpolate using the value recorded before and after for each device. The Spark SQL functions LAG and LEAD will make this a trivial calculation.
# MAGIC We'll write these values to a temporary view called updates. This view will be used later to upsert values into our health_tracker_processed Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Create a DataFrame Interpolating Broken Values
# MAGIC Recall that we want to partition on our Device ID column, which we named:
# MAGIC **`"p_device_id"`**

# COMMAND ----------

# TODO
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead

dteWindow = Window.partitionBy("p_device_id").orderBy("dte")

interpolatedDF = (spark.read
                       .table("health_tracker_processed")
                       .select(col("dte"),
                               col("time"),
                               col("heartrate"),
                               lag(col("heartrate")).over(dteWindow).alias("prev_amt"),
                               lead(col("heartrate")).over(dteWindow).alias("next_amt"),
                               col("name"),
                               col("p_device_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Create a DataFrame of Updates

# COMMAND ----------

updatesDF = (interpolatedDF.where(col("heartrate") < 0)
                           .select(col("dte"),
                                   col("time"),
                                   ((col("prev_amt") + col("next_amt"))/2).alias("heartrate"),
                                   col("name"),
                                   col("p_device_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: View the schemas of the **`updatesDF`** and health_tracker_processed table
# MAGIC 
# MAGIC We use the **`printSchema()`** function to view the schema of the **health_tracker_processed** table.
# MAGIC 
# MAGIC Fill in the format we should use and run the cell below. 

# COMMAND ----------

# TODO
updatesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Verify **`updatesDF`**
# MAGIC 
# MAGIC Perform a **`count()`** on the **`updatesDF`** view.
# MAGIC 
# MAGIC It should have the same number of records as the **`SUM`** performed on the broken_readings view.

# COMMAND ----------

updatesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prepare Inserts DataFrame
# MAGIC It turns out that our expectation of receiving the missing records late was correct.
# MAGIC 
# MAGIC These records have subsequently been made available to us as the file **health_tracker_data_2020_02_01.json**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Load the Late-Arriving Data

# COMMAND ----------

file_path = f"{DA.paths.raw}/health_tracker_data_2020_2_late.json"

health_tracker_data_2020_2_late_df = (spark.read
                                           .format("json")
                                           .load(file_path))

# COMMAND ----------

# MAGIC %md 
# MAGIC Next, count the number of records

# COMMAND ----------

# TODO
health_tracker_data_2020_2_late_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Transform the Data
# MAGIC In addition to updating the broken records, we wish to add this late-arriving data. We begin by preparing another temporary view with the appropriate transformations:
# MAGIC * Use the **`from_unixtime`** Spark SQL function to transform the unixtime into a time string
# MAGIC * Cast the **`time`** column to type **`timestamp`** to replace the column **`time`**
# MAGIC * Cast the **`time`** column to type **`date`** to create the column **`dte`**

# COMMAND ----------

insertsDF = DA.process_health_tracker_data(health_tracker_data_2020_2_late_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: View the Schema of the Inserts DataFrame

# COMMAND ----------

insertsDF.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Create the Union DataFrame
# MAGIC Finally, we prepare the **`upsertsDF`** that consists of all the records in both the **`updatesDF`** and the **`insertsDF`**.
# MAGIC 
# MAGIC We use the DataFrame **`union()`** command to create the view.

# COMMAND ----------

upsertsDF = updatesDF.union(insertsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: View the Schema

# COMMAND ----------

upsertsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Perform Upsert Into the health_tracker_processed Table
# MAGIC You can upsert data into a Delta table using the merge operation.
# MAGIC 
# MAGIC This operation is similar to the SQL **`MERGE`** command but has added support for deletes and other conditions in updates, inserts, and deletes.
# MAGIC 
# MAGIC In other words, using the DeltaTable command **`merge()`** provides full support for an upsert operation.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Perform the Upsert

# COMMAND ----------

from delta.tables import DeltaTable
processedDeltaTable = DeltaTable.forPath(spark, DA.paths.processed)

update_match = """health_tracker.time = upserts.time
                  AND
                  health_tracker.p_device_id = upserts.p_device_id"""

update = {"heartrate" : "upserts.heartrate"}

insert = {
    "p_device_id" : "upserts.p_device_id",
    "heartrate" : "upserts.heartrate",
    "name" : "upserts.name",
    "time" : "upserts.time",
    "dte" : "upserts.dte"
}

(processedDeltaTable.alias("health_tracker")
                    .merge(upsertsDF.alias("upserts"), update_match)
                    .whenMatchedUpdate(set=update)
                    .whenNotMatchedInsert(values=insert)
                    .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## View the Commit Using Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: View the table as of Version 2
# MAGIC This is done by specifying the option **`"versionAsOf"`** as **`2`**.
# MAGIC 
# MAGIC When we time travel to Version 0, we see only the first month of data.
# MAGIC 
# MAGIC In version 1, we see the table after we added comments. 
# MAGIC 
# MAGIC When we time travel to Version 2, we see the first two months of data, minus the 72 missing records.

# COMMAND ----------

(spark.read
      .option("versionAsOf", 2)
      .format("delta")
      .load(DA.paths.processed)
      .count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Count the Most Recent Version
# MAGIC When we query the table without specifying a version, it shows the latest version of the table and includes the full two months of data.
# MAGIC 
# MAGIC Note that the range of data includes the month of February during a leap year.
# MAGIC 
# MAGIC That is why there are 29 days in the month.

# COMMAND ----------

spark.read.table("health_tracker_processed").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Describe the History of the **health_tracker_processed** Table
# MAGIC The **`history()`** Delta Table command provides provenance information, including the operation, user, and so on, for each write to a table.
# MAGIC 
# MAGIC Note that each operation performed on the table is given a version number.
# MAGIC 
# MAGIC These are the numbers we have been using when performing a time travel query on the table, e.g., **`SELECT COUNT(*) FROM health_tracker_processed VERSION AS OF 1`**.

# COMMAND ----------

display(processedDeltaTable.history())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Perform a Second Upsert
# MAGIC In the previous lesson, we performed an upsert to the **health_tracker_processed** table, which updated records containing broken readings. When we inserted the late arriving data, we inadvertently added more broken readings!

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Sum the Broken Readings

# COMMAND ----------

# TODO
from pyspark.sql.functions import col,count

broken_readings = (spark.read
                  .format("delta")
                  .load(DA.paths.processed)
                  .select(col("heartrate"), col("dte"))
                  .where(col("heartrate") < 0)
                  .groupBy("dte")
                  .agg(count("heartrate"))
                  .orderBy("dte")
                        )
broken_readings.createOrReplaceTempView("broken_readings")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Verify That These are New Broken Readings
# MAGIC 
# MAGIC Let’s query the broken_readings with a **`WHERE`** clause to verify that these are indeed new broken readings introduced by inserting the late-arriving data.
# MAGIC 
# MAGIC Note that there are no broken readings before **2020-02-25**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(`count(heartrate)`) AS total
# MAGIC FROM broken_readings 
# MAGIC WHERE dte < '2020-02-25'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Verify Updates
# MAGIC Perform a **`count()`** on the **`updatesDF`** view.
# MAGIC 
# MAGIC **Note:** It is not necessary to redefine the DataFrame.
# MAGIC 
# MAGIC Recall that a Spark DataFrame is lazily defined, pulling the correct number of updates when an action is triggered.
# MAGIC 
# MAGIC It should have the same number of records as the **`SUM`** performed on the **broken_readings** view.

# COMMAND ----------

# TODO
from pyspark.sql.functions import count

updatesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Perform Upsert Into the health_tracker_processed Table
# MAGIC 
# MAGIC Once more, we upsert into the **health_tracker_processed** table using the DeltaTable command **`merge()`**.

# COMMAND ----------

upsertsDF = updatesDF

(processedDeltaTable.alias("health_tracker")
                    .merge(upsertsDF.alias("upserts"), update_match)
                    .whenMatchedUpdate(set=update)
                    .whenNotMatchedInsert(values=insert)
                    .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 5: Sum the Broken Readings
# MAGIC Let’s sum the records in the **broken_readings** view one last time. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(`count(heartrate)`) AS total
# MAGIC FROM broken_readings

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
