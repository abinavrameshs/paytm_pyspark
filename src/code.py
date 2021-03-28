from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Paytm") \
    .getOrCreate()

test = spark.read.format("csv").option("header", "true").load("../data/test.csv")

test.show(15)



