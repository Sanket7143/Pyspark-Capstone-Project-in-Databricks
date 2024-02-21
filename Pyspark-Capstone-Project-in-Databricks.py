# Databricks notebook source
# MAGIC %md
# MAGIC #### 1.Create a DF(airlines_1987_to_2008) from this path

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df.display()

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.Create a PySpark Datatypes schema for the above DF

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

user_schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("DepTime", DoubleType(), True),
    StructField("CRSDepTime", DoubleType(), True),
    StructField("ArrTime", DoubleType(), True),
    StructField("CRSArrTime", DoubleType(), True),
    StructField("UniqueCarrier", StringType(), True),
    StructField("FlightNum", IntegerType(), True),
    StructField("TailNum", StringType(), True),
    StructField("ActualElapsedTime", DoubleType(), True),
    StructField("CRSElapsedTime", DoubleType(), True),
    StructField("AirTime", DoubleType(), True),
    StructField("ArrDelay", DoubleType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("Origin", StringType(), True),
    StructField("Dest", StringType(), True),
    StructField("Distance", IntegerType(), True),
    StructField("TaxiIn", DoubleType(), True),
    StructField("TaxiOut", DoubleType(), True),
    StructField("Cancelled", IntegerType(), True),
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", IntegerType(), True),
    StructField("CarrierDelay", DoubleType(), True),
    StructField("WeatherDelay", DoubleType(), True),
    StructField("NASDelay", DoubleType(), True),
    StructField("SecurityDelay", DoubleType(), True),
    StructField("LateAircraftDelay", DoubleType(), True)
])

# COMMAND ----------

df=spark.read.option("header",True).schema(user_schema).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.View the dataframe

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.Return count of records in dataframe
# MAGIC

# COMMAND ----------

record_count = df.count()
print("Record Count:", record_count)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.Select the columns - Origin, Dest and Distance
# MAGIC

# COMMAND ----------

df.select("Origin","Origin","Distance").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.Filtering data with 'where' method, where Year = 2001

# COMMAND ----------

df.where("Year=2001").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.Create a new dataframe (airlines_1987_to_2008_drop_DayofMonth) exluding dropped column (“DayofMonth”) 

# COMMAND ----------

df.drop("DayofMonth").display()

# COMMAND ----------

df1=df.drop("DayofMonth")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.Display new DataFrame

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9.Create column 'Weekend' and a new dataframe(AddNewColumn) and display

# COMMAND ----------

AddNewColumn=df1.withColumn("Weekend",when(col("DayOfWeek").isin(6, 7), lit("Yes")).otherwise(lit("No")))

# COMMAND ----------

AddNewColumn.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####10.Cast ActualElapsedTime column to integer and use printschema to verify

# COMMAND ----------

df_cast = AddNewColumn.withColumn("ActualElapsedTime", col("ActualElapsedTime").cast(IntegerType()))

# COMMAND ----------

df_cast.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 11.Rename 'DepTime' to 'DepartureTime'

# COMMAND ----------

newdf=df_cast.withColumnRenamed("DepTime","DepartureTime")

# COMMAND ----------

newdf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 12.Drop duplicate rows based on Year and Month and Create new df (Drop Rows)
# MAGIC

# COMMAND ----------

DropRows = newdf.dropDuplicates(['Year','Month'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####13.Sort by descending order for Year Column using sort()

# COMMAND ----------

sort_df = DropRows.sort("Year", ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 14.Group data according to Origin and returning count

# COMMAND ----------

origin_count = DropRows.groupBy("Origin").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####15.Group data according to dest and finding maximum value for each 'Dest'

# COMMAND ----------

max_dest_values = DropRows.groupBy("Dest").max("Distance")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 16.Write data in Delta format

# COMMAND ----------

DropRows.write.format("delta").save("dbfs:/FileStore/assignment")
