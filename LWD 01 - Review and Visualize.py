# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviewing and Visualizing data
# MAGIC #### Health tracker data
# MAGIC One common use case for working with Delta Lake is to collect and process Internet of Things (IoT) Data.
# MAGIC 
# MAGIC Here, we provide a mock IoT sensor dataset for demonstration purposes.
# MAGIC 
# MAGIC The data simulates heart rate data measured by a health tracker device.

# COMMAND ----------

# MAGIC %md ## Classroom Setup
# MAGIC Run the following cell to configure this course's environment:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Health tracker data sample
# MAGIC <br/>
# MAGIC 
# MAGIC <code>
# MAGIC {"device_id":0,"heartrate":52.8139067501,"name":"Deborah Powell","time":1.5778368E9}<br/>
# MAGIC {"device_id":0,"heartrate":53.9078900098,"name":"Deborah Powell","time":1.5778404E9}<br/>
# MAGIC {"device_id":0,"heartrate":52.7129593616,"name":"Deborah Powell","time":1.577844E9}<br/>
# MAGIC {"device_id":0,"heartrate":52.2880422685,"name":"Deborah Powell","time":1.5778476E9}<br/>
# MAGIC {"device_id":0,"heartrate":52.5156095386,"name":"Deborah Powell","time":1.5778512E9}<br/>
# MAGIC {"device_id":0,"heartrate":53.6280743846,"name":"Deborah Powell","time":1.5778548E9}<br/>
# MAGIC </code>
# MAGIC 
# MAGIC This shows a sample of the health tracker data we will be using.
# MAGIC 
# MAGIC Note that each line is a valid JSON object.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Health tracker data schema
# MAGIC The data has the following schema:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC | Column    | Type      |
# MAGIC |-----------|-----------|
# MAGIC | name      | string    |
# MAGIC | heartrate | double    |
# MAGIC | device_id | int       |
# MAGIC | time      | long      |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load the Data
# MAGIC Load the data as a Spark DataFrame from the raw directory.
# MAGIC 
# MAGIC The location of the JSON file is provided for you.
# MAGIC 
# MAGIC To load it into a DataFrame, use **`spark.read`** while setting the format to "JSON" and passing to the **`load()`** function the path we provided for you.

# COMMAND ----------

# TODO
file_path = f"{DA.paths.raw}/health_tracker_data_2020_1.json"
health_tracker_data_2020_1_df = (spark.read
                                 .format("json")
                                 .load(file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualize Data
# MAGIC ### Step 1: Display the Data
# MAGIC Strictly speaking, this is not part of the ETL process, but displaying the data gives us a look at the data that we are working with.

# COMMAND ----------

display(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Configure the Visualization
# MAGIC Create a Databricks visualization to visualize the sensor data over time.
# MAGIC 
# MAGIC We have used the following plot options to configure the visualization:
# MAGIC 
# MAGIC 
# MAGIC | Field | Label |
# MAGIC |-------|-------|
# MAGIC | Keys | **time** |
# MAGIC | Series groupings | **device_id** |
# MAGIC | Values | **heartrate** |
# MAGIC | Aggregation | **SUM** |
# MAGIC | Display Type | **Bar Chart** |`
# MAGIC 
# MAGIC Now that we have a better idea of the data we're working with, let's move on to create a Parquet-based table from this data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
