# Databricks notebook source
# MAGIC %md
# MAGIC # Machine Learning Batch for Scheduled Job
# MAGIC 
# MAGIC #####In this notebook we will:
# MAGIC 1. Create a data frame.
# MAGIC 2. Load a previously saved model.
# MAGIC 3. Run predictions.
# MAGIC 4. Combine predictions with input data.
# MAGIC 5. Get current timestamp for file name.
# MAGIC 5. Output results to Blob Storage.

# COMMAND ----------

# Create the R data.frame
rdf <- data.frame(iris)

# COMMAND ----------

# load a model from DBFS
load(file = "/dbfs/myirisrmodel.rda")

# COMMAND ----------

# Run predictions
rpredictions <- predict(rmodel, rdf)

# COMMAND ----------

# R returns only the predictions, so we need to combine them with the input data 
rdfWithPredictions <- cbind(rdf, rpredictions)

# COMMAND ----------

# Create a variable with current timestamp without separators
strTimestamp <- paste(
  substr(paste(Sys.Date(),format(Sys.time(), "%X"), sep=""), 1, 4), # year
  substr(paste(Sys.Date(),format(Sys.time(), "%X"), sep=""), 6, 7), # month
  substr(paste(Sys.Date(),format(Sys.time(), "%X"), sep=""), 9, 12), # day and hour
  substr(paste(Sys.Date(),format(Sys.time(), "%X"), sep=""), 14, 15), # minute
  substr(paste(Sys.Date(),format(Sys.time(), "%X"), sep=""), 17, 18), # second
  sep=""
)

# COMMAND ----------

# Output predictions with original data to Blob Storage
write.table(rdfWithPredictions, paste("/dbfs/mnt/data/{YOUR CONTAINER NAME}/rdfWithPredictions_", strTimestamp, ".csv", sep="")
            , sep=","
            , row.names=FALSE)

# COMMAND ----------

