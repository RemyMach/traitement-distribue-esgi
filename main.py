from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from time import sleep
from pyspark.sql import functions as F

import time
start_time = time.time()

# Initialisation de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-ui") \
    .config("spark.default.parallelism", "10") \
    .config("spark.executor.cores", "1") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("/app/data/full.csv", inferSchema=True, header=True)
print(df.printSchema())

# 1

df_non_null = df.filter(df.repo.isNotNull())
df_grouped = df_non_null.groupBy("repo").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count")).limit(10)

df_grouped.show(truncate=100)

# 2
df_spark = df_non_null.filter(df_non_null.repo == 'apache/spark')
# df_spark.cache()
df_spark_grouped = df_spark.groupBy("author").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count"))
df_spark_grouped_top = df_spark_grouped.limit(1)

df_spark_grouped_top.show(truncate=100)

from pyspark.sql.functions import regexp_extract,to_timestamp, expr, col, concat_ws, slice, split

# 3
# Mon Apr 19 20:38:03 2021 +0100
date_format = "MMM d HH:mm:ss yyyy Z"

#df_spark.show(truncate=False)
#df_spark_copy = df_spark.limit(20)
#df_spark.show(truncate=False)
#df_spark_extract = df_spark.withColumn("date", regexp_extract(df_spark["date"], "(\\w{3} \\d{1,2} \\d{2}:\\d{2}:\\d{2} \\d{4} (\\+|\\-)\\d{4})", 1))
df_spark_extract = df_spark.withColumn("date", concat_ws(" ", slice(split(df_spark["date"], " "), 2, int(1e9))))


df_spark_extract.show(truncate=False)
# Convertir la colonne 'date' en timestamp
df_convert = df_spark_extract.withColumn("date", to_timestamp(col("date"), date_format))
#df_convert.show(truncate=False)
df_spark_four_year = df_convert.filter((df_convert.date >= expr("date_sub(current_date(), 4 * 365)")))
df_spark_four_year.orderBy(F.asc("date")).show(truncate=False)

df_spark_four_year_grouped = df_spark_four_year.groupBy("author").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count"))
 
#print(df_time.printSchema())
df_spark_four_year_grouped.show(truncate=False)

print("--- %s seconds ---" % (time.time() - start_time))

# 4
from pyspark.ml.feature import StopWordsRemover

stop_word_remover = StopWordsRemover()
stop_word_remover.setInputCol("words_split")
stop_word_remover.setOutputCol("no_stop_words")

df_non_null = df.filter(df.repo.isNotNull()).filter(df.message.isNotNull())
df_grouped = df_non_null.withColumn('words_split', F.split(df_non_null.message, " "))
df_grouped = stop_word_remover.transform(df_grouped)
df_grouped = df_grouped.withColumn('word', F.explode(df_grouped.no_stop_words))
df_grouped = df_grouped.filter(df_grouped.word != '')
df_grouped = df_grouped.groupBy("word").agg(F.count("word").alias("word_count")).orderBy(F.desc("word_count")).limit(10)
df_grouped.show(truncate=100)

sleep(1000)