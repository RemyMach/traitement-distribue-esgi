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

df = spark.read.csv("/app/data/full.csv", inferSchema=True, header=True, multiLine=True)

df.write.parquet("/app/data/full.parquet")

print('PArquet generated')

