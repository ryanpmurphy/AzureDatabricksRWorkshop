# Databricks notebook source
# MAGIC %md
# MAGIC # Get Started with Data in SparkR
# MAGIC 
# MAGIC Read, transform, aggregate, and output data using SparkR.
# MAGIC 
# MAGIC #####In this notebook we will:
# MAGIC 1. Load csv data
# MAGIC 2. Do basic data transformations (e.g., rename columns, change data types)
# MAGIC 3. Export to multiple file formats (parquet, csv)
# MAGIC 4. Aggregate and group data
# MAGIC 5. Collect a Spark DataFrame to an R data.frame
# MAGIC 
# MAGIC *Note:* This notebook borrows from the SparkR tutorials found here: https://github.com/UrbanInstitute/sparkr-tutorials.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load csv data

# COMMAND ----------

# import SparkR library
library(SparkR)

# COMMAND ----------

# MAGIC %md
# MAGIC Try out the filesystem magic `%fs` to help validate the presence of the source data.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/data/source/Performance_2000Q1.txt"

# COMMAND ----------

# MAGIC %md
# MAGIC Use the operation read.df to load in quarterly Fannie Mae single-family loan performance data from Azure Blob Storage as a Spark DataFrame (DF). Load a single quarter (2000, Q1) into SparkR, and save it as the DF perf. 
# MAGIC 
# MAGIC Then view the schema of the new DF.

# COMMAND ----------

# read file into spark dataframe
perf <- read.df("/mnt/data/source/Performance_2000Q1.txt", 
				header = "false", 
				delimiter = "|", 
				source = "csv", 
				inferSchema = "true", 
				na.strings = "")

# view the schema
str(perf)

# COMMAND ----------

# MAGIC %md
# MAGIC We can save the dimensions of the 'perf' DF through the following operations. Note that wrapping the computation with () forces SparkR/R to print the computed value:

# COMMAND ----------

# Check and save the number of rows
(n1 <- nrow(perf))

# COMMAND ----------

# Check and save the number of columns
(m1 <- ncol(perf))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Do basic data transformations
# MAGIC 
# MAGIC The select operation performs a by column subset of an existing DF. The columns to be returned in the new DF are specified as a list of column name strings in the select operation. Here, we create a new DF called perf_lim that includes only the first 14 columns in the perf DF, i.e. the DF perf_lim is a subset of perf:

# COMMAND ----------

# subset dataframe with select()
cols <- c("_C0","_C1","_C2","_C3","_C4","_C5","_C6","_C7","_C8","_C9","_C10","_C11","_C12","_C13")
perf_lim <- select(perf, col = cols)

# COMMAND ----------

# MAGIC %md
# MAGIC Using a for-loop and the SparkR operation withColumnRenamed, we rename the columns of perf_lim. The operation withColumnRenamed renames an existing column, or columns, in a DF and returns a new DF. By specifying the "new" DF name as perf_lim, however, we simply rename the columns of perf_lim (we *could* create an entirely separate DF with new column names by specifying a different DF name for withColumnRenamed).
# MAGIC 
# MAGIC 
# MAGIC Then view the schema of perf_lim.

# COMMAND ----------

# rename columns
old_colnames <- c("_C0","_C1","_C2","_C3","_C4","_C5","_C6","_C7","_C8","_C9","_C10","_C11","_C12","_C13")
new_colnames <- c("loan_id","period","servicer_name","new_int_rt","act_endg_upb","loan_age","mths_remng",
                  "aj_mths_remng","dt_matr","cd_msa","delq_sts","flag_mod","cd_zero_bal","dt_zero_bal")

for(i in 1:14){
  perf_lim <- withColumnRenamed(perf_lim, existingCol = old_colnames[i], newCol = new_colnames[i] )
}

# view the schema
str(perf_lim)

# COMMAND ----------

# multiple ways to view schema

dtypes(perf_lim)
# schema(perf_lim)
# printSchema(perf_lim)

# COMMAND ----------

# MAGIC %md
# MAGIC Data types can be changed after the DF has been created, using the cast operation. However, it is clearly more efficient to properly specify data types when creating the DF. A quick example of using the cast operation is given below.

# COMMAND ----------

# cast to change data type
perf_lim$loan_id <- cast(perf_lim$loan_id, dataType = "string")
printSchema(perf_lim)

# cast again to change it back
perf_lim$loan_id <- cast(perf_lim$loan_id, dataType = "long")
printSchema(perf_lim)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Export to multiple file formats
# MAGIC 
# MAGIC In order to use this DF later on, we must first export it to a location that can handle large data sizes and in a data structure that works with the SparkR environment. We'll save this example data to an Azure Blob Storage folder ("sparkr-tutorials") from which we'll access other example datasets. Below, we save perf_lim as a collection of parquet type files into the folder "hfpc_ex" using the write.df operation.
# MAGIC 
# MAGIC Substitute the placeholder with your container name.

# COMMAND ----------

# output to blob storage as parquet
write.df(perf_lim, path = "/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex", 
							source = "parquet", 
							mode = "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC When working with the DF perf_lim in the analysis above, we were really accessing data that was partitioned across our cluster. In order to export this partitioned data, we export each partition from its node (computer) and then collect them into the folder "hfpc_ex". This "file" of indiviudal, partitioned files should be treated like an indiviudal file when organizing a Blob Storage folder, i.e. do not attempt to save other DataFrames or files to this file. SparkR saves the DF in this partitioned structure to accomodate massive data.
# MAGIC 
# MAGIC Consider the conditions required for us to be able to save a DataFrame as a single .csv file: the given DF would need to be able to fit onto a single node of our cluster, i.e. it would need to be able to fit onto a single computer. Any data that would necessitate using SparkR in analysis will likely not fit onto a single computer. Note that we have specified mode = "overwrite", indicating that existing data in this folder is expected to be overwritten by the contents of this DF (additional mode specifications include "error", "ignore" and "append").
# MAGIC 
# MAGIC The partitioned nature of "hfpc_ex" does not affect our ability to load it back into SparkR and perform further analysis. Below, we use the read.df to read in the partitioned parquet file from Blob Storage as the DF dat.
# MAGIC 
# MAGIC Substitute the placeholder with your container name.

# COMMAND ----------

# read in the parquet data from blob storage
dat <- read.df("/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex", 
				header = "false", 
				inferSchema = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC Below, we confirm that the dimensions and column names of dat and perf_lim are equal. When comparing DFs, each with a large number of columns, the following if-else statement can be adapted to check equal dimensions and column names across DFs:

# COMMAND ----------

# compare dimensions of two DataFrames
dim1 <- dim(perf_lim)
dim2 <- dim(dat)
if (dim1[1]!=dim2[1] | dim1[2]!=dim2[2]) {
  "Error: dimension values not equal; DataFrame did not export correctly"
} else {
  "Dimension values are equal"
}

# COMMAND ----------

# MAGIC %md
# MAGIC We can also save the DF as a folder of partitioned .csv files with syntax similar to that which we used to export the DF as partitioned parquet files. Note, however, that this does not retain the column names like saving as partitioned parquet files does. The write.df expression for exporting the DF as a folder of partitioned .csv files is given below.
# MAGIC 
# MAGIC Substitute the placeholder with your container name.

# COMMAND ----------

# output to blob storage as csv
write.df(perf_lim, path = "/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex_csv", 
							source = "csv", 
							mode = "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC We can read in the .csv files as a DF with the following expression:

# COMMAND ----------

# read in the csv data from blob storage
dat2 <- read.df("/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex_csv", 
				source = "csv", 
				inferSchema = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Aggregate and group data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Aggregating
# MAGIC 
# MAGIC Computing aggregations across a dataset is a basic goal when working with tabular data and, because our data is distributed across nodes, we must explicitly direct SparkR to perform an aggregation if we want to compute and return a summary statistic. Both the agg and summarize operations achieve this by computing aggregations of DF entries based on a specified list of columns. For example, we can return the mean loan age for all rows in the DF df as below.
# MAGIC 
# MAGIC Substitute the placeholder with your container name.

# COMMAND ----------

# read in the data from blob storage
df <- read.df("/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex", 
				header = "false", 
				inferSchema = "true", 
				na.strings = "")

# create a new DataFrame with aggregated data 
df1 <- agg(df, loan_age_avg = avg(df$loan_age))

# view aggregated data
showDF(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Grouping
# MAGIC If we want to compute aggregations across the elements of a dataset that share a common identifier, we can achieve this embedding the groupBy operation in agg or summarize. For example, the following agg operation returns the mean loan age and the number of observations for each distinct "servicer_name" in the DataFrame df:

# COMMAND ----------

gb_sn <- groupBy(df, df$servicer_name)
df2 <- agg(gb_sn, loan_age_avg = avg(df$loan_age), count = n(df$loan_age))
head(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Collect a Spark DataFrame to an R data.frame
# MAGIC 
# MAGIC You may want to run R statistical or other functions that are not available in SparkR. If so, convert your Spark DataFrame to an R data.frame with `collect()`. Keep in mind you are pulling data from the distributed nodes to the single driver node when you do this. When you're dealing with large data volumes, you will need to sample or summarize your data before collecting to an R data.frame.

# COMMAND ----------

# MAGIC %md
# MAGIC Collect the aggregated loan performance data to an R data.frame.

# COMMAND ----------

# collect Spark DataFrame to R data.frame
rdfAggLoanPerf <- collect(df2)

# COMMAND ----------

# view the structure and sample data from your R data.frame to validate it was successfully collected (i.e., converted from Spark)
str(rdfAggLoanPerf)

# COMMAND ----------

