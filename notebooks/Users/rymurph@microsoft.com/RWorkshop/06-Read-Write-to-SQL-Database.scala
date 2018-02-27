// Databricks notebook source
// MAGIC %md
// MAGIC # Read and Write to a SQL Database
// MAGIC 
// MAGIC Connect to a SQL Database and create corresponding Azure Databricks tables for easy external database integration.
// MAGIC 
// MAGIC #####In this notebook we will:
// MAGIC 1. Create a database.
// MAGIC 2. Create tables based on a SQL Database.
// MAGIC 3. Read data from a SQL Database.
// MAGIC 4. Write data to a SQL Database.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create a database
// MAGIC Try out the sql magic `%sql` to simplify the command to create a database. Then validate that it was created with a spark command to list all existing databases.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS awdb;

// COMMAND ----------

// validate database created
// try spark command:
spark.catalog.listDatabases.show()

// also view Data in UI

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Create tables based on a SQL Database
// MAGIC Use the sql magic `%sql` to simplify the command to create tables.
// MAGIC 
// MAGIC We can also see the database and tables, along with sample data, in the Data sidebar.
// MAGIC 
// MAGIC Substitute the placeholders with your Azure SQL Database name.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS awdb;
// MAGIC USE awdb;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS customer;
// MAGIC CREATE TABLE customer
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://comressqlserver.database.windows.net:1433;database={YOUR AZURE SQL DATABASE NAME};',
// MAGIC   dbtable 'saleslt.customer',
// MAGIC   user 'comres',
// MAGIC   password "P@ssword1234"
// MAGIC );

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS awdb;
// MAGIC USE awdb;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS salesorderheader;
// MAGIC CREATE TABLE salesorderheader
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://comressqlserver.database.windows.net:1433;database={YOUR AZURE SQL DATABASE NAME};',
// MAGIC   dbtable 'saleslt.salesorderheader',
// MAGIC   user 'comres',
// MAGIC   password "P@ssword1234"
// MAGIC );

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS awdb;
// MAGIC USE awdb;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS salesorderdetail;
// MAGIC CREATE TABLE salesorderdetail
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://comressqlserver.database.windows.net:1433;database={YOUR AZURE SQL DATABASE NAME};',
// MAGIC   dbtable 'saleslt.salesorderdetail',
// MAGIC   user 'comres',
// MAGIC   password "P@ssword1234"
// MAGIC );

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Read data from a SQL Database.
// MAGIC Now we can use simple or more complex SQL queries to read data in the SQL Database. Try reading from multiple tables independently, and then try joining them in a query. 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from awdb.customer;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from awdb.salesorderheader;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from awdb.salesorderdetail;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from awdb.customer c
// MAGIC inner join awdb.salesorderheader oh on c.customerid = oh.customerid
// MAGIC inner join awdb.salesorderdetail od on oh.salesorderid = od.salesorderid;

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Write data to a SQL Database.
// MAGIC We could write to one of the existing tables that we worked with above, but a common requirement is writing to a new table. So first we'll create the new table in the SQL Database; then we'll create the corresponding Databricks table based on the SQL Database table; finally, we'll write records to the new table.

// COMMAND ----------

// MAGIC %md
// MAGIC First, create the table in the SQL Database. You can use the Query editor (preview) within your SQL Database from the Azure Portal and run the below `CREATE TABLE` statement.
// MAGIC 
// MAGIC ```
// MAGIC --DROP TABLE saleslt.allorders;
// MAGIC CREATE TABLE saleslt.allorders  
// MAGIC     (salesorderid int NOT NULL,
// MAGIC     customerid int NOT NULL,
// MAGIC     orderdate datetime,
// MAGIC     salesorderdetailid int,
// MAGIC     productid int,
// MAGIC     orderqty smallint,
// MAGIC     unitprice money
// MAGIC PRIMARY KEY (salesorderid, salesorderdetailid));```
// MAGIC 
// MAGIC *Note:* If you need to re-create the table, uncomment the `DROP TABLE` line and run the whole block.

// COMMAND ----------

// MAGIC %md
// MAGIC Then create the corresponding table in Azure Databricks based on the SQL Database table you just created.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS awdb;
// MAGIC USE awdb;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS allorders;
// MAGIC CREATE TABLE allorders
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://comressqlserver.database.windows.net:1433;database={YOUR AZURE SQL DATABASE NAME};',
// MAGIC   dbtable 'saleslt.allorders',
// MAGIC   user 'comres',
// MAGIC   password "P@ssword1234"
// MAGIC );

// COMMAND ----------

// MAGIC %md
// MAGIC Now write data to your new table. Using `INSERT INTO... SELECT` syntax, you can insert results from another query into your new table.

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into allorders
// MAGIC select oh.salesorderid, --int
// MAGIC     oh.customerid, --int
// MAGIC     oh.orderdate, --datetime
// MAGIC     od.salesorderdetailid, --int
// MAGIC     od.productid, --int
// MAGIC     od.orderqty, --int
// MAGIC     od.unitprice --decimal
// MAGIC from salesorderheader oh
// MAGIC inner join salesorderdetail od on oh.salesorderid = od.salesorderid;

// COMMAND ----------

// MAGIC %md
// MAGIC Query your new table to validate the data was written successfully to the SQL Database based on your custom query.

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from allorders

// COMMAND ----------

