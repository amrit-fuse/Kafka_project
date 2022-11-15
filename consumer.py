from allsecrets import *
import json
import requests
from json import dumps, loads
# dumps converts python object to json string
# loads converts json string to python object
from kafka import KafkaConsumer


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('OMDB+weather')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")\
    .getOrCreate()


consumer_OMDB = KafkaConsumer('OMDB', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                              enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8')))


# Reading from Kafka
df_OMDB = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "OMDB") \
    .option("startingOffsets", "earliest") \
    .load()

# # Converting the value column from binary to string inroder to read the data
df_OMDB = df_OMDB.selectExpr("CAST(value AS STRING)")


schema_OMDB = "Actors STRING, Awards STRING, BoxOffice STRING, Country STRING, DVD STRING, Director STRING, Genre STRING, Language STRING, Metascore STRING, Plot STRING, Poster STRING, Production STRING, Rated STRING, Ratings STRING, Released STRING, Response STRING, Runtime STRING, Title STRING, Type STRING, Website STRING, Writer STRING, Year STRING, imdbID STRING, imdbRating STRING, imdbVotes STRING"

# explode to columns from json
df_OMDB = df_OMDB.select(F.from_json(F.col("value"), schema_OMDB).alias(
    "data")).select("data.*")


# Writing the stream to the console
query = df_OMDB \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()


# Consumer_weather = KafkaConsumer('weather', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
#                                  enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8')))


# # Reading from Kafka
# df_weather = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "weather") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # # Converting the value column from binary to string inroder to read the data
# df_weather = df_weather.selectExpr("CAST(value AS STRING)")


# schema_weather = "city STRING, cod STRING, message STRING, cnt STRING, list STRING"

# df_weather = df_weather.select(F.from_json(
#     F.col("value"), schema_weather).alias("data")).select("data.*")

# # Writing the stream to the console
# query = df_weather \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


# query.awaitTermination()
