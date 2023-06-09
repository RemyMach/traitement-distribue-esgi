from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from time import sleep
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import to_timestamp, expr, col, concat_ws, slice, split
import time
start_time = time.time()

# Initialisation de la SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-ui") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.default.parallelism", "8") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/app/data/full.parquet", inferSchema=True, header=True, multiLine=True)

print("--- Load %s seconds ---" % (time.time() - start_time))

# Exercice 1

df_non_null = df.filter(df.repo.isNotNull())
df_grouped = df_non_null.groupBy("repo").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count")).limit(10)
df_grouped.show(truncate=False)

print("--- 1- %s seconds ---" % (time.time() - start_time))

# Exercice 2
df_spark = df_non_null.filter(df_non_null.repo == 'apache/spark')
df_spark_grouped = df_spark.groupBy("author").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count"))
df_spark_grouped_top = df_spark_grouped.limit(1)
df_spark_grouped_top.show(truncate=False)

print("--- 2- %s seconds ---" % (time.time() - start_time))

# Exercice 3
date_format = "MMM d HH:mm:ss yyyy Z"

df_spark_extract = df_spark.withColumn("date", concat_ws(" ", slice(split(df_spark["date"], " "), 2, int(1e9))))
print("---3 extract %s seconds ---" % (time.time() - start_time))

df_convert = df_spark_extract.withColumn("date", to_timestamp(col("date"), date_format))
print("---3 timestamp %s seconds ---" % (time.time() - start_time))
df_spark_four_year = df_convert.filter((df_convert.date >= expr("date_sub(current_date(), 4 * 365)")))
print("---3 filter by date %s seconds ---" % (time.time() - start_time))

df_spark_four_year_grouped = df_spark_four_year.groupBy("author").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count"))
print("---3 group by %s seconds ---" % (time.time() - start_time))
 
df_spark_four_year_grouped.show(truncate=False)

print("---3 final %s seconds ---" % (time.time() - start_time))

# Exercice 4

stop_word_remover = StopWordsRemover()
stop_word_remover.setInputCol("words_split")
stop_word_remover.setOutputCol("no_stop_words")

df_non_null_2 = df_non_null.filter(df_non_null.message.isNotNull())
df_grouped = df_non_null_2.withColumn('words_split', F.split(df_non_null_2.message, " "))

df_grouped = stop_word_remover.transform(df_grouped)
df_grouped = df_grouped.withColumn('word', F.explode(df_grouped.no_stop_words))
df_grouped = df_grouped.filter(df_grouped.word != '').filter(df_grouped.word != '\n').filter(df_grouped.word != '*').filter(df_grouped.word != '-')
df_grouped = df_grouped.groupBy("word").agg(F.count("word").alias("word_count")).orderBy(F.desc("word_count")).limit(10)
df_grouped.show(truncate=False)

print("---4 %s seconds ---" % (time.time() - start_time))

sleep(1000)

