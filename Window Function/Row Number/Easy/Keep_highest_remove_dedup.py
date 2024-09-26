"""
Question ❓: Given a DataFrame with columns ID, Name, and Value, write a PySpark code to remove duplicate rows based on the ID column, keeping only the row with the highest Value

Sample Input

+---+-----+-----+
| ID| Name|Value|
+---+-----+-----+
| 1|Alice| 100|
| 1| Alice| 200|
| 2| Bob| 150|
| 2| Bob| 120|
+---+-----+-----+


Expected Output
+---+-----+-----+
| ID| Name|Value|
+---+-----+-----+
| 1|Alice| 200|
| 2| Bob| 150|
+---+-----+-----+

data = [
    (1, 'Alice', 100),
    (1, 'Alice', 200),
    (2, 'Bob', 150),
    (2, 'Bob', 120)
]

columns = ["ID", "Name", "Value"]

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

data = [
    (1, 'Alice', 100),
    (1, 'Alice', 200),
    (2, 'Bob', 150),
    (2, 'Bob', 120)
]

columns = ["ID", "Name", "Value"]

df = ss.createDataFrame(data,columns)

df.show()

import pyspark.sql.window as W
ww = W.Window.partitionBy(F.col("ID")).orderBy(F.col("Value").desc())

res_df = df.withColumn("rank", F.row_number().over(ww))
res_df.filter(F.col("rank")== 1).select("ID","Name","Value").show()