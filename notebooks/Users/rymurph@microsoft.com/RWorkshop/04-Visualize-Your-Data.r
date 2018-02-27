# Databricks notebook source
# MAGIC %md
# MAGIC # Visualize Your Data
# MAGIC 
# MAGIC #####In this notebook we will:
# MAGIC 1. Visualize data with R library ggplot2.
# MAGIC 2. Use built-in Databricks notebook visualizations.
# MAGIC 2. Create a dashboard with Databricks.
# MAGIC 3. Use PowerBI to view your data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Visualize data with ggplot.
# MAGIC Using the Iris dataset, import R library ggplot2 and try various visualizations using it.

# COMMAND ----------

# First view the data
iris

# COMMAND ----------

# Import library ggplot2
library(ggplot2)

# Try a basic visualization with ggplot2
ggplot(data=iris, aes(x=Sepal.Length, y=Sepal.Width)) + geom_point()

# COMMAND ----------

# Extend your ggplot2 visualization with color
ggplot(data=iris, aes(x=Sepal.Length, y=Sepal.Width, color=Species)) + geom_point(size=3)

# COMMAND ----------

# Now pruce up your visualization with a linear fit
ggplot(iris, aes(x=Sepal.Length, y=Sepal.Width, color=Species)) + geom_point() + stat_smooth(method="lm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Use built-in Databricks notebook visualizations.
# MAGIC Create and display a Spark DataFrame and experiment with built-in notebook visualization options.

# COMMAND ----------

# Import SparkR library
library(SparkR)

# Create Spark DataFrame with Iris dataset
sdfIris <- createDataFrame(iris)

# COMMAND ----------

# Visualize Iris with Databricks notebook visualizations. Try customizing the plot, and toggling between a raw table and visual plot.
display(sdfIris)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create a dashboard with Databricks.
# MAGIC Use the **View:** dropdown in the notebook menu to select **+ New Dashboard**. Customize and share your dashboard with data and visualizations from this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Use PowerBI to view your data.
# MAGIC PowerBI can read global Databricks tables with the help of a Spark ODBC driver. 
# MAGIC 
# MAGIC Data registered in Tables, as in previously exercises, is accessible from PowerBI.
# MAGIC 
# MAGIC Follow the instructions in the documentation to connect PowerBI to Azure Databricks data: https://docs.azuredatabricks.net/user-guide/bi/power-bi.html#install-power-bi-desktop-and-create-a-report.

# COMMAND ----------

