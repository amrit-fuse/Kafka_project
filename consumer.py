from allsecrets import *
import json
import requests
from json import dumps, loads
# dumps converts python object to json string
# loads converts json string to python object
from kafka import KafkaConsumer


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('weather')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")\
    .getOrCreate()


Consumer = KafkaConsumer('weather', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8'), key_deserializer=lambda x: loads(x.decode('utf-8'))))


# Reading from Kafka
df_weather = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather") \
    .option("startingOffsets", "earliest") \
    .load()


# # Converting the value column from binary to string inroder to read the data
df_weather = df_weather.selectExpr("CAST(value AS STRING)")


#  load  schema_weather  from .json file
with open('schema_weather.json', 'r') as f:
    schema_weather = F.StructType.fromJson(json.load(f))

# select all columns from data column and create a new dataframe
df_weather = df_weather.select(F.from_json(
    F.col("value"), schema_weather).alias("data")).select("data.*")

# df_weather.printSchema()

# expanding the  city  column
df_weather = df_weather.select("*", F.col("city.*"))
# df_weather.printSchema()

# explode list colmun
df_weather = df_weather.select(
    "*", F.explode("list").alias(" every 3 hours weather"))
# df_weather.printSchema()


# # extract coord, country, id, name, population, sunrise, sunset, timezone from city column
# df_weather = df_weather.select( F.col("city.coord.lat").alias("lat"), F.col("city.coord.lon").alias("lon"), F.col("city.country").alias("country"), F.col("city.id").alias("id"), F.col("city.name").alias("name"), F.col("city.population").alias("population"), F.col("city.sunrise").alias("sunrise"), F.col("city.sunset").alias("sunset"), F.col("city.timezone").alias("timezone"), F.col("every 3 hours weather.*"))


# #  with the help of schema_weather we can extract the data from the json file
# df_weather = df_weather.select(F.col("city.coord.lat"), F.col("city.coord.lon"), F.col("city.country"), F.col("city.id"), F.col(
#     "city.name"), F.col("city.population"), F.col("city.sunrise"), F.col("city.sunset"), F.col("city.timezone"), F.col("list"))

# # df_weather.printSchema()


# # explode list column
# df_weather = df_weather.select(
#     "*", F.explode("list").alias("every 3 hours weather"))

# # df_weather.printSchema()

# # expand every 3 hours weather column
# df_weather = df_weather.select(F.col("lat"), F.col("lon"), F.col("country"), F.col("id"), F.col("name"), F.col(
#     "population"), F.col("sunrise"), F.col("sunset"), F.col("timezone"), F.col("every 3 hours weather.*"))

# df_weather.printSchema()


# Writing the stream to the console
query = df_weather \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()
