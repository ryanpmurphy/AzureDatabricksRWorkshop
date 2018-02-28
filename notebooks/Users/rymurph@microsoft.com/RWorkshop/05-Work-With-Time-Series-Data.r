# Databricks notebook source
# MAGIC %md
# MAGIC # Work with Time Series Data
# MAGIC 
# MAGIC Use datetime functions that come in handiest when working with time series data.
# MAGIC 
# MAGIC #####In this notebook we will:
# MAGIC 1. Load data
# MAGIC 2. Convert columns to date data type
# MAGIC 3. Add columns with relative date functions (e.g., nextday(), date_add()) 
# MAGIC 4. Add columns with relative measure of time functions (e.g., months_between(), datediff()) 
# MAGIC 5. Aggregate by calculated units of time
# MAGIC 
# MAGIC *Note:* This notebook borrows from the SparkR tutorials found here: https://github.com/UrbanInstitute/sparkr-tutorials.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load data
# MAGIC Read in initial data as DF. We will use the loan performance example dataset that we exported previously.
# MAGIC 
# MAGIC Substitute the placeholder with your container name.

# COMMAND ----------

# import SparkR library
library(SparkR)

# COMMAND ----------

# read in data from blob storage
df <- read.df("/mnt/data/{YOUR CONTAINER NAME}/sparkr-tutorials/hfpc_ex", 
				header = "false", 
				inferSchema = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Convert columns to date data type
# MAGIC There are several columns in our dataset that list dates which are helpful in determining loan performance. We will specifically consider the following columns:
# MAGIC 
# MAGIC - "period" (Monthly Reporting Period): The month and year that pertain to the servicer’s cut-off period for mortgage loan information
# MAGIC - "dt_matr"(Maturity Date): The month and year in which a mortgage loan is scheduled to be paid in full as defined in the mortgage loan documents
# MAGIC - "dt_zero_bal"(Zero Balance Effective Date): Date on which the mortgage loan balance was reduced to zero
# MAGIC 
# MAGIC Let's begin by reviewing the data types that read.df infers our date columns as. Note that each of our three (3) date columns were read in as strings:

# COMMAND ----------

# view the schema
str(df)

# COMMAND ----------

# MAGIC %md
# MAGIC While we could parse the date strings into separate year, month and day integer dtype columns, converting the columns to date dtype allows us to utilize the datetime functions available in SparkR.
# MAGIC 
# MAGIC We can convert "period", "matr_dt" and "dt_zero_bal" to date dtype with the following expressions:

# COMMAND ----------

# `period`
period_uts <- unix_timestamp(df$period, 'MM/dd/yyyy')	# 1. Gets current Unix timestamp in seconds
period_ts <- cast(period_uts, 'timestamp')	# 2. Casts Unix timestamp `period_uts` as timestamp
period_dt <- cast(period_ts, 'date')	# 3. Casts timestamp `period_ts` as date dtype
df <- withColumn(df, 'p_dt', period_dt)	# 4. Add date dtype column `period_dt` to `df`

# `dt_matr`
matr_uts <- unix_timestamp(df$dt_matr, 'MM/yyyy')
matr_ts <- cast(matr_uts, 'timestamp')
matr_dt <- cast(matr_ts, 'date')
df <- withColumn(df, 'mtr_dt', matr_dt)

# `dt_zero_bal`
zero_bal_uts <- unix_timestamp(df$dt_zero_bal, 'MM/yyyy')
zero_bal_ts <- cast(zero_bal_uts, 'timestamp')
zero_bal_dt <- cast(zero_bal_ts, 'date')
df <- withColumn(df, 'zb_dt', zero_bal_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC If the date string entries of these columns were in the default format, converting to date dtype would be straightforward. If "period" was in the format 'yyyy-mm-dd', for example, we would be able to append df with a date dtype column using a simple withColumn/cast expression: df <- withColumn(df, 'p_dt', cast(df$period, 'date')). We could also directly convert "period" to date dtype using the to_date operation: df$period <- to_date(df$period). Otherwise, the longer conversion process is required. 

# COMMAND ----------

# view the schema
str(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Add columns with relative date functions
# MAGIC As we mentioned earlier, converting date strings to date dtype allows us to utilize SparkR datetime operations. 
# MAGIC 
# MAGIC For convenience, we will review these operations using the df_dt DF, which includes only the date columns "p_dt" and "mtr_dt", which we created in the preceding section:

# COMMAND ----------

# subset the DataFrame with only the date columns of interest
cols_dt <- c("p_dt", "mtr_dt")
df_dt <- select(df, cols_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, we'll discuss SparkR operations that return date data type columns, which list dates relative to a preexisting date column in the DF.
# MAGIC 
# MAGIC - **last_day**: Returns the last day of the month which the given date belongs to (e.g. inputting "2013-07-27" returns "2013-07-31")
# MAGIC - **next_day**: Returns the first date which is later than the value of the date column that is on the specified day of the week
# MAGIC - **add_months**: Returns the date that is 'numMonths' after 'startDate'
# MAGIC - **date_add**: Returns the date that is 'days' days after 'start'
# MAGIC - **date_sub**: Returns the date that is 'days' days before 'start'
# MAGIC 
# MAGIC Below, we create relative date columns (defining "p_dt" as the input date) using each of these operations and withColumn:

# COMMAND ----------

# add relative date columns
df_dt1 <- withColumn(df_dt, 'p_ld', last_day(df_dt$p_dt))
df_dt1 <- withColumn(df_dt1, 'p_nd', next_day(df_dt$p_dt, "Sunday"))
df_dt1 <- withColumn(df_dt1, 'p_addm', add_months(df_dt$p_dt, 1)) # 'startDate'="pdt", 'numMonths'=1
df_dt1 <- withColumn(df_dt1, 'p_dtadd', date_add(df_dt$p_dt, 1)) # 'start'="pdt", 'days'=1
df_dt1 <- withColumn(df_dt1, 'p_dtsub', date_sub(df_dt$p_dt, 1)) # 'start'="pdt", 'days'=1

# view the schema
str(df_dt1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add columns with relative measure of time functions
# MAGIC 
# MAGIC In this section, we'll discuss SparkR operations that return integer or numerical data type columns, which list measures of time relative to a preexisting date column in the DF.
# MAGIC 
# MAGIC - **weekofyear**: Extracts the week number as an integer from a given date
# MAGIC - **dayofyear**: Extracts the day of the year as an integer from a given date
# MAGIC - **dayofmonth**: Extracts the day of the month as an integer from a given date
# MAGIC - **datediff**: Returns number of months between dates 'date1' and 'date2'
# MAGIC - **months_between**: Returns the number of days from 'start' to 'end'
# MAGIC 
# MAGIC Here, we use "p_dt" and "mtr_dt" as inputs in the above operations. We again use withColumn do append the new columns to a DF:

# COMMAND ----------

# add relative measure of time columns
df_dt2 <- withColumn(df_dt, 'p_woy', weekofyear(df_dt$p_dt))
df_dt2 <- withColumn(df_dt2, 'p_doy', dayofyear(df_dt$p_dt))
df_dt2 <- withColumn(df_dt2, 'p_dom', dayofmonth(df_dt$p_dt))
df_dt2 <- withColumn(df_dt2, 'mbtw_p.mtr', months_between(df_dt$mtr_dt, df_dt$p_dt)) # 'date1'=p_dt, 'date2'=mtr_dt
df_dt2 <- withColumn(df_dt2, 'dbtw_p.mtr', datediff(df_dt$mtr_dt, df_dt$p_dt)) # 'start'=p_dt, 'end'=mtr_dt

# view the schema
str(df_dt2)

# COMMAND ----------

# MAGIC %md
# MAGIC Extract components of a date dtype column as integer values
# MAGIC There are also datetime operations supported by SparkR that allow us to extract individual components of a date dtype column and return these as integers. Below, we use the year and month operations to create integer dtype columns for each of our date columns. Similar functions include hour, minute and second.

# COMMAND ----------

# add year and month columns

# Year and month values for `"period_dt"`
df <- withColumn(df, 'p_yr', year(df$p_dt))
df <- withColumn(df, "p_m", month(df$p_dt))

# Year and month values for `"matr_dt"`
df <- withColumn(df, 'mtr_yr', year(df$mtr_dt))
df <- withColumn(df, "mtr_m", month(df$mtr_dt))

# Year and month values for `"zero_bal_dt"`
df <- withColumn(df, 'zb_yr', year(df$zb_dt))
df <- withColumn(df, "zb_m", month(df$zb_dt))

# COMMAND ----------

# view the schema
str(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Aggregate by calculated units of time

# COMMAND ----------

# MAGIC %md
# MAGIC While we can resample the data over distinct values of any of the columns in dat, we will resample the loan-level data as aggregations of the DF columns by units of time since we are working with time series data. Below, we aggregate the columns of dat (taking the mean of the column entries) by "p_yr", and then by "p_yr" and "p_m":

# COMMAND ----------

# Resample, aggregating by "period_yr" column
df1 <- agg(groupBy(df, df$p_yr), p_m = mean(df$p_m), mtr_yr = mean(df$mtr_yr), zb_yr = mean(df$zb_yr), 
            new_int_rt = mean(df$new_int_rt), act_endg_upb = mean(df$act_endg_upb), loan_age = mean(df$loan_age), 
            mths_remng = mean(df$mths_remng), aj_mths_remng = mean(df$aj_mths_remng))
head(df1)

# COMMAND ----------

