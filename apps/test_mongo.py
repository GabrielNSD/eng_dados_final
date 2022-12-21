from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os

mongo_uri = os.getenv('MONGO_URI')

def init_spark():
    #password = os.environ["MONGODB_PASSWORD"]
    #user = os.environ["MONGODB_USER"]
    #host = os.environ["MONGODB_HOST"]
    #db_auth = os.environ["MONGODB_DB_AUTH"]
    #mongo_conn = f"mongodb://{user}:{password}@{host}:27017/{db_auth}"
    mongo_conn = "mongodb://root:example@172.20.0.3:27017"

    conf = SparkConf()

    # Download mongo-spark-connector and its dependencies.
    # This will download all the necessary jars and put them in your $HOME/.ivy2/jars, no need to manually download them :
    conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.5")
    conf.set("spark.jars.packages", "org.mongodb:mongo-java-driver:3.12.11")

    print(mongo_uri)

    # Set up read connection :
    conf.set("spark.mongodb.read.connection.uri", mongo_conn)
    print('set read connection')
    conf.set("spark.mongodb.read.database", "testDatabase")
    print('set read database')
    conf.set("spark.mongodb.read.collection", "testCollection")
    print('set read collection')

    # Set up write connection
    conf.set("spark.mongodb.write.connection.uri", mongo_conn)
    conf.set("spark.mongodb.write.database", "testDatabase")
    conf.set("spark.mongodb.write.collection", "testCollection")
    # If you need to update instead of inserting :
    #conf.set("spark.mongodb.write.operationType", "update")

    SparkContext(conf=conf)

    return SparkSession \
        .builder \
        .appName('MyApp') \
        .getOrCreate()

spark = init_spark()
df = spark.read.format("mongodb").load()

people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77), ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])

people.write.format("mongodb").mode("append").save()

people.show()

