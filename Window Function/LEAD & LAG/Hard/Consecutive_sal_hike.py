"""
Q.
find emp_id having at least 3 Consecutive hike in salary and year should be Consecutive

employee_ID    salary    year
1         80000    2020
1         70000    2019
1         60000    2018
2         65000    2020
2         65000    2019
2         60000    2018
3         65000    2019
3         60000    2018
3         50000    2015


# Data to be added to the DataFrame
data = [
    (1, 80000, 2020),
    (1, 70000, 2019),
    (1, 60000, 2018),
    (2, 65000, 2020),
    (2, 65000, 2019),
    (2, 60000, 2018),
    (3, 65000, 2019),
    (3, 60000, 2018),
    (3, 50000, 2015)
]

# Define the schema
columns = ["employee_ID", "salary", "year"]

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

ss = SparkSession.builder.appName("LAG and Lead").getOrCreate()

# Data to be added to the DataFrame
data = [
    (1, 80000, 2020),
    (1, 70000, 2019),
    (1, 60000, 2018),
    (2, 65000, 2020),
    (2, 65000, 2019),
    (2, 60000, 2018),
    (3, 65000, 2019),
    (3, 60000, 2018),
    (3, 50000, 2015)
]

# Define the schema
columns = ["employee_ID", "salary", "year"]

hike_df = ss.createDataFrame(data,columns)
hike_df.show()

import pyspark.sql.window as W
ww = W.Window.partitionBy(F.col("employee_ID")).orderBy(F.col("year"),F.col("salary"))

df_1 = (hike_df.withColumn("pre_sal", F.lag(F.col("salary")).over(ww))
        .withColumn("next_sal", F.lead(F.col("salary")).over(ww))
)
df_1.show()

df_2 =(df_1.withColumn("pre_yr", F.lag(F.col("year")).over(ww))
        .withColumn("next_yr", F.lead(F.col("year")).over(ww))
)
df_2.show()

#consecutively hike in salary.
(df_2.filter(((F.col("pre_sal") < F.col("salary")) & (F.col("salary") < F.col("next_sal")))
             & (((F.col("year") - F.col("pre_yr") == 1)) & ((F.col("next_yr") - F.col("year")) == 1 ))
)
 .select("employee_ID").show())