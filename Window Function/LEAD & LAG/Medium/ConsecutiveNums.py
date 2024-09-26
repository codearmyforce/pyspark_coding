"""

Table: Logs

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| num         | varchar |
+-------------+---------+
In SQL, id is the primary key for this table.
id is an autoincrement column starting from 1.
 

Find all numbers that appear at least three times consecutively.


Input: 
Logs table:
+----+-----+
| id | num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+
Output: 
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+

data = [
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 1),
    (6, 2),
    (7, 2)
]

# Define the schema
columns = ["id", "num"]

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

ss = SparkSession.builder.appName("LAG and Lead").getOrCreate()

data = [
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 1),
    (6, 2),
    (7, 2)
]

# Define the schema
columns = ["id", "num"]

log_df = ss.createDataFrame(data,columns)
log_df.show()

import pyspark.sql.window as W
ww = W.Window.orderBy(F.col("id"))

df_1 = (log_df.withColumn("pre_num", F.lag(F.col("num")).over(ww))
        .withColumn("Next_num", F.lead(F.col("num")).over(ww))
)
df_1.show()

#Find all numbers that appear at least three times consecutively.
(df_1.filter((F.col("pre_num") == F.col("num")) & (F.col("num") == F.col("next_num")))
 .select("num").show())