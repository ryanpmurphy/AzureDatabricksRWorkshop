// Databricks notebook source
// MAGIC %md
// MAGIC # Connect to Blob Storage
// MAGIC 
// MAGIC Connect to a blob storage account you have access to, and mount data to Azure Databricks for fast and scalable data storage.
// MAGIC 
// MAGIC #####In this notebook we will:
// MAGIC 1. Create blob storage mount points.
// MAGIC 2. List mount points.
// MAGIC 3. Unmount blob storage.
// MAGIC 4. Refresh mount points.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create blob storage mount points
// MAGIC Substitute the placeholders with your container name, storage account name, and storage account key. 
// MAGIC 
// MAGIC *Note:* You will have to have a cluster running to execute this code.

// COMMAND ----------

//Mount blob storage of external data source
dbutils.fs.mount(
  source = "wasbs://workshop@adbbase.blob.core.windows.net/",
  mountPoint = "/mnt/data/source/",
  extraConfigs = Map("fs.azure.account.key.adbbase.blob.core.windows.net" -> 
                     "XwYaFpISJYnw8q7iaYPmAv/hHSlhzThXZVAj6X4j75MlN7/6gYJKq6yjgN7lE4t0YQ9vnnVK+vRYHPYDkWeHww=="))

// COMMAND ----------

// Mount your own blob storage container
dbutils.fs.mount(
  source = "wasbs://{YOUR CONTAINER NAME}@adbbase.blob.core.windows.net/",
  mountPoint = "/mnt/data/{YOUR CONTAINER NAME}/",
  extraConfigs = Map("fs.azure.account.key.adbbase.blob.core.windows.net" -> 
                     "XwYaFpISJYnw8q7iaYPmAv/hHSlhzThXZVAj6X4j75MlN7/6gYJKq6yjgN7lE4t0YQ9vnnVK+vRYHPYDkWeHww=="))


// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. List mount points.

// COMMAND ----------

display(dbutils.fs.ls("/mnt/data/"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Unmount blob storage
// MAGIC Substitute the placeholder with your container name.

// COMMAND ----------



// COMMAND ----------

 dbutils.fs.unmount("/mnt/data/source/")
 dbutils.fs.unmount("/mnt/data/{YOUR CONTAINER NAME}/")


// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Refresh mount points
// MAGIC Refresh mount cache on the cluster nodes.

// COMMAND ----------

dbutils.fs.refreshMounts()

// COMMAND ----------

