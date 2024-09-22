'''
Problem Statement:

Write a code to find the second highest salary achiever from all employee.

Input:
+---+-------+------+
| id|   name|salary|
+---+-------+------+
|  1|  Alice|  5000|
|  2|    Bob|  4000|
|  3|Charlie|  3000|
|  4|  David|  4500|
|  5|    Eve|  6000|
+---+-------+------+


Output:

+---+-----+------+----+
| id| name|salary|rank|
+---+-----+------+----+
|  1|Alice|  5000|   2|
+---+-----+------+----+

'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create Spark session
spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()

# Sample data
data = [
    (1, "Alice", 5000),
    (2, "Bob", 4000),
    (3, "Charlie", 3000),
    (4, "David", 4500),
    (5, "Eve", 6000)
]

# Create DataFrame
columns = ["id", "name", "salary"]
df = spark.createDataFrame(data, columns)

import pyspark.sql.window as w
ww = w.Window.orderBy(df['salary'].desc())


df2 = df.withColumn("rank", F.row_number().over(ww))
df2.filter(F.col('rank')== 2).show()