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
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.default.parallelism", "8") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("/app/data/full.csv", inferSchema=True, header=True)
df.coalesce(8)
print(df.printSchema())

print("--- Load %s seconds ---" % (time.time() - start_time))

# 1

df_non_null = df.filter(df.repo.isNotNull())
# Repartitionnement pour équilibrer les données entre les partitions
df_repartitioned = df_non_null.repartition("repo")

# Mise en cache du DataFrame pour éviter de recalculer les mêmes données
#df_repartitioned.cache()

df_grouped = df_repartitioned.groupBy("repo").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count")).limit(10)

df_grouped.show(truncate=100)

print("--- 1- %s seconds ---" % (time.time() - start_time))

# 2
df_spark = df_repartitioned.filter(df_repartitioned.repo == 'apache/spark')
# df_spark.cache()
df_spark_grouped = df_spark.groupBy("author").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count"))
df_spark_grouped_top = df_spark_grouped.limit(1)

df_spark_grouped_top.show(truncate=100)

from pyspark.sql.functions import regexp_extract,to_timestamp, expr, col, concat_ws, slice, split
print("--- 2- %s seconds ---" % (time.time() - start_time))

# 3
# Mon Apr 19 20:38:03 2021 +0100
date_format = "MMM d HH:mm:ss yyyy Z"

#df_spark.show(truncate=False)
#df_spark_copy = df_spark.limit(20)
#df_spark.show(truncate=False)
#df_spark_extract = df_spark.withColumn("date", regexp_extract(df_spark["date"], "(\\w{3} \\d{1,2} \\d{2}:\\d{2}:\\d{2} \\d{4} (\\+|\\-)\\d{4})", 1))
df_spark_extract = df_spark.withColumn("date", concat_ws(" ", slice(split(df_spark["date"], " "), 2, int(1e9))))
print("---3 extract %s seconds ---" % (time.time() - start_time))

#df_spark_extract.show(truncate=False)
# Convertir la colonne 'date' en timestamp
df_convert = df_spark_extract.withColumn("date", to_timestamp(col("date"), date_format))
print("---3 timestamp %s seconds ---" % (time.time() - start_time))
#df_convert.show(truncate=False)
df_spark_four_year = df_convert.filter((df_convert.date >= expr("date_sub(current_date(), 4 * 365)")))
print("---3 filter by date %s seconds ---" % (time.time() - start_time))
#df_spark_four_year.orderBy(F.asc("date")).show(truncate=False)

df_spark_four_year_grouped = df_spark_four_year.groupBy("author").agg(F.count("*").alias("commit_count")).orderBy(F.desc("commit_count"))
print("---3 group by %s seconds ---" % (time.time() - start_time))
 
#print(df_time.printSchema())
df_spark_four_year_grouped.show(truncate=False)

print("---3 final %s seconds ---" % (time.time() - start_time))
sleep(1000)

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

