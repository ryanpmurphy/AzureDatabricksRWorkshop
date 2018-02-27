# Databricks notebook source
# MAGIC %md
# MAGIC # Work with Databases and Tables
# MAGIC 
# MAGIC Create and query Databricks tables.
# MAGIC 
# MAGIC #####In this notebook we will:
# MAGIC 1. Create a database.
# MAGIC 2. Create a table based on remote Parquet data.
# MAGIC 3. Create a table based on remote CSV data.
# MAGIC 4. Query tables using SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create a database
# MAGIC Use the `%sql` magic to simplify SQL code.
# MAGIC 
# MAGIC View the new database from Data sidebar.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS fannie_mae;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create a table based on remote Parquet data.
# MAGIC Create a table with SQL that is based on the Parquet-formatted loan performance data you previously wrote to Blob Storage.
# MAGIC 
# MAGIC View the new table and sample data from the Data sidebar.
# MAGIC 
# MAGIC Substitute the placeholder with your container name.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fannie_mae;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS loan_performance;
# MAGIC CREATE TABLE IF NOT EXISTS loan_performance(
# MAGIC   loan_id BIGINT,
# MAGIC   period STRING,
# MAGIC   servicer_name STRING,
# MAGIC   new_int_rt DOUBLE,
# MAGIC   act_endg_upb DOUBLE,
# MAGIC   loan_age INT,
# MAGIC   mths_remng INT,
# MAGIC   aj_mths_remng INT,
# MAGIC   dt_matr STRING,
# MAGIC   cd_msa INT,
# MAGIC   delq_sts STRING,
# MAGIC   flag_mod STRING,
# MAGIC   cd_zero_bal INT,
# MAGIC   dt_zero_bal STRING)
# MAGIC USING parquet
# MAGIC LOCATION "/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex";

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create a table based on remote CSV data.
# MAGIC Create a table with SQL that is based on the CSV-formatted loan performance data you previously wrote to Blob Storage.
# MAGIC 
# MAGIC View the new table and sample data from the Data sidebar.
# MAGIC 
# MAGIC Substitute the placeholder with your container name.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE fannie_mae;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS loan_performance_csv;
# MAGIC CREATE TABLE IF NOT EXISTS loan_performance_csv(
# MAGIC   loan_id BIGINT,
# MAGIC   period STRING,
# MAGIC   servicer_name STRING,
# MAGIC   new_int_rt DOUBLE,
# MAGIC   act_endg_upb DOUBLE,
# MAGIC   loan_age INT,
# MAGIC   mths_remng INT,
# MAGIC   aj_mths_remng INT,
# MAGIC   dt_matr STRING,
# MAGIC   cd_msa INT,
# MAGIC   delq_sts STRING,
# MAGIC   flag_mod STRING,
# MAGIC   cd_zero_bal INT,
# MAGIC   dt_zero_bal STRING)
# MAGIC USING csv
# MAGIC LOCATION "/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex_csv";

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Query tables using SQL.
# MAGIC Use SQL to interactively query tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC --Parquet table
# MAGIC select * from fannie_mae.loan_performance where mths_remng < 12;

# COMMAND ----------

# MAGIC %sql
# MAGIC --CSV table
# MAGIC select * from fannie_mae.loan_performance_csv where mths_remng < 12;