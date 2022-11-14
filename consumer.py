from allsecrets import *
import json
import requests
from json import dumps, loads
# dumps converts python object to json string
# loads converts json string to python object
from kafka import KafkaConsumer


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('OMDB')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")\
    .getOrCreate()


consumer = KafkaConsumer('OMDB', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8')))

# Setting parameters for the Spark session to read from Kafka
bootstrap_servers = "localhost:9092"
topic = "OMDB"

# Reading from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# # Converting the value column from binary to string inroder to read the data
df = df.selectExpr("CAST(value AS STRING)")



schema = "Actors STRING, Awards STRING, BoxOffice STRING, Country STRING, DVD STRING, Director STRING, Genre STRING, Language STRING, Metascore STRING, Plot STRING, Poster STRING, Production STRING, Rated STRING, Ratings STRING, Released STRING, Response STRING, Runtime STRING, Title STRING, Type STRING, Website STRING, Writer STRING, Year STRING, imdbID STRING, imdbRating STRING, imdbVotes STRING"

# explode to columns from json
df = df.select(F.from_json(F.col("value"), schema).alias(
    "data")).select("data.*")


# Writing the stream to the console
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
