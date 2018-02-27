# Databricks notebook source
# MAGIC %md
# MAGIC # Machine Learning with R and SparkR
# MAGIC 
# MAGIC #####In this notebook we will:
# MAGIC 1. Create a simple model with plain R.
# MAGIC 2. Persist this model for reuse later.
# MAGIC 3. Create the same model with SparkR for comparison.
# MAGIC 4. Run predictions with R and SparkR.
# MAGIC 5. Output predictions to Blob Storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create a simple model with plain R.
# MAGIC Create an R data frame based on the Iris dataset. Use `str()` to validate the structure of the data frame and `display()` to view sample data.
# MAGIC 
# MAGIC Then train the model using the R function `glm()` with `gaussian()` for linear regression.

# COMMAND ----------

# Create the R data.frame
rdf <- data.frame(iris)

# validate the data and structure of the R data.frame
# str(rdf)
display(rdf)

# COMMAND ----------

# Fit a linear model over the dataset.
rmodel <- glm(Sepal.Length ~ Sepal.Width + Species,data=rdf,family=gaussian())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Persist the model for reuse later.
# MAGIC Save the R model to DBFS with R function `save()`. Later, we can load the saved model to use in a batch scoring job.

# COMMAND ----------

save(rmodel, file = "/dbfs/myirisrmodel.rda")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create a simple model with SparkR for comparison.
# MAGIC Create a Spark data frame based on the Iris dataset. Use `str()` to validate the structure of the data frame and `display()` to view sample data.
# MAGIC 
# MAGIC Then train the model using the SparkR function `glm()` with `"gaussian"` for linear regression.

# COMMAND ----------

# Import SparkR library
library(SparkR)

# Create the SparkDataFrame
sdf <- createDataFrame(iris)

# validate the data and structure of the SparkDataFrame
# str(sdf)
display(sdf)

# COMMAND ----------

# Fit a linear model over the dataset.
smodel <- glm(Sepal_Length ~ Sepal_Width + Species, data = sdf, family = "gaussian")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Run predictions with R and SparkR.
# MAGIC Run predictions with R on the R data.frame, putting the results in a new R data.frame, and view the resulting data.
# MAGIC Then run predictions with SparkR on the Spark DataFrame, putting the results in a new Spark DataFrame, and view the resulting data.

# COMMAND ----------

# Run predictions with R model on R data.frame
rpredictions <- predict(rmodel, rdf)

# COMMAND ----------

# R returns only the predictions, so we need to combine them with the input data 
rdfWithPredictions <- cbind(rdf, rpredictions)

# view the R data.frame with combined input data and predictions
rdfWithPredictions

# COMMAND ----------

# Run predictions with SparkR model on SparkDataFrame
spredictions <- predict(smodel, sdf)

# COMMAND ----------

# SparkR automatically combines the label and predictions with the input data 
display(spredictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Output predictions to Blob Storage.
# MAGIC Write the R data.frame to a csv in Azure Blob Storage for further processing.
# MAGIC 
# MAGIC Substitute the placeholders with your container name.

# COMMAND ----------

# Output predictions with original data to Blob Storage
write.table(rdfWithPredictions, "/dbfs/mnt/data/{YOUR CONTAINER NAME}/rdfWithPredictions.csv"
            , sep=","
            , row.names=FALSE)

# COMMAND ----------

