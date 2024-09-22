'''
Problem Statement:
Write a code to remove the Special characters(! @ # $ % ^ & * ( ) - _ = + \ | { } ; : / ? . >) from 
the column value.

Input:
+-----------+
|column_name|
+-----------+
|   val!ue@1|
|   va#lue$2|
|   val%ue^3|
+-----------+

Output:
+-----------+
|column_name|
+-----------+
|     value1|
|     value2|
|     value3|
+-----------+

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Initialize a Spark session
spark = SparkSession.builder.appName("RemoveSpecialCharacters").getOrCreate()

# Sample data
data = [("val!ue@1",), ("va#lue$2",), ("val%ue^3",)]
columns = ["column_name"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)
df.show()
# Remove special characters
df_cleaned = df.withColumn("column_name", regexp_replace("column_name", "[^A-Za-z0-9 ]", ""))

# Show the result
df_cleaned.show()

