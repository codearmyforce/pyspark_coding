'''
Problem Statement:
Write a pyspark to get the words count from below article.

Input:
Article:
PySpark is the Python API for Spark. PySpark allows you to leverage the 
parallel computing capabilities of Spark using Python. With PySpark, you can 
process data at scale, perform machine learning, and much more.

Output:
Total number of words: 34

'''
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("TotalWordCountExample").getOrCreate()

article = """PySpark is the Python API for Spark. PySpark allows you to leverage the 
             parallel computing capabilities of Spark using Python. With PySpark, you can 
             process data at scale, perform machine learning, and much more."""

# Create a DataFrame
data = [(article,)]
columns = ["article"]

df = spark.createDataFrame(data, columns)
df.show(truncate = True)

from pyspark.sql.functions import split, explode, col

# Split the article into words
df_words = df.withColumn("word", explode(split(col("article"), "\\s+")))

# Show the DataFrame with words
df_words.show(truncate=False)

from pyspark.sql.functions import regexp_replace

# Remove punctuation from words
df_cleaned_words = df_words.withColumn("word", regexp_replace(col("word"), "[^A-Za-z0-9]", ""))

# Show the cleaned DataFrame with words
df_cleaned_words.select('word', 'article').show(truncate=False)


# Count the total number of words
total_word_count = df_cleaned_words.count()

print(f"Total number of words: {total_word_count}")
