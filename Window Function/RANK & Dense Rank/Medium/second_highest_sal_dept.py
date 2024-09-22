'''
Problem Statement:

Write a code to find the second highest salary achiever department wise from all employee.

Input:
+---+-----------------+-------+----------+
| id|             name| salary|      Dept|
+---+-----------------+-------+----------+
|  1|         John Doe|50000.0|        HR|
|  2|       Jane Smith|60000.0|   Finance|
|  3|        Sam Brown|55000.0|        IT|
|  4|      Emily Davis|75000.0| Marketing|
|  5|   Michael Wilson|80000.0|     Sales|
|  6|     Linda Taylor|67000.0|Operations|
|  7|   David Anderson|72000.0|        HR|
|  8|      Susan Clark|58000.0|   Finance|
|  9|    James Johnson|69000.0|        IT|
| 10|Patricia Martinez|63000.0| Marketing|
+---+-----------------+-------+----------+


Output:

+---+-----------------+-------+---------+----+
| id|             name| salary|     Dept|Rank|
+---+-----------------+-------+---------+----+
|  8|      Susan Clark|58000.0|  Finance|   2|
|  1|         John Doe|50000.0|       HR|   2|
|  3|        Sam Brown|55000.0|       IT|   2|
| 10|Patricia Martinez|63000.0|Marketing|   2|
+---+-----------------+-------+---------+----+

'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as w

data = [
    (1, "John Doe", 50000.0, "HR"),
    (2, "Jane Smith", 60000.0, "Finance"),
    (3, "Sam Brown", 55000.0, "IT"),
    (4, "Emily Davis", 75000.0, "Marketing"),
    (5, "Michael Wilson", 80000.0, "Sales"),
    (6, "Linda Taylor", 67000.0, "Operations"),
    (7, "David Anderson", 72000.0, "HR"),
    (8, "Susan Clark", 58000.0, "Finance"),
    (9, "James Johnson", 69000.0, "IT"),
    (10, "Patricia Martinez", 63000.0, "Marketing")
]


columns = ['id','name','salary','Dept']

df = ss.createDataFrame(data,columns)

import pyspark.sql.window as w
ww = w.Window.partitionBy(F.col('Dept')).orderBy(F.col('salary').desc())


res_df = df.withColumn("Rank", F.dense_rank().over(ww))

res_df.show()