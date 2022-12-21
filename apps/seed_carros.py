from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("local", "carros_mongodb")

spark = SparkSession(sc)

df = spark.read.options(header='True', inferSchema='True',
                        delimiter=',').csv("cars.csv")

df.write.format("mongodb").mode("overwrite").option(
    "database", "venda").option("collection", "carros").save()

print("seed completo")