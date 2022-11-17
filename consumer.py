from allsecrets import *
import json
import requests
from json import dumps, loads
# dumps converts python object to json string
# loads converts json string to python object
from kafka import KafkaConsumer
import kafkaconnect


from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('weather')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")\
    .getOrCreate()


Consumer = KafkaConsumer('weather', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8'), key_deserializer=lambda x: loads(x.decode('utf-8'))))


# sink connector to write data to postgres


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

# expanding the  city  column  and drop city column
df_weather = df_weather.select("*", F.col("city.*")) \
    .drop("city")
# df_weather.printSchema()

# for lat lon
df_weather = df_weather.select("*", F.col("coord.*")) \
    .drop("coord")

# explode list colmun  and drop list column
df_weather = df_weather.select(
    "*", F.explode("list").alias("every_3_hr")) \
    .drop("list")

# for every_3_hr column
df_weather = df_weather.select("*", F.col("every_3_hr.*")) \
    .drop("every_3_hr")

# for clouds column
df_weather = df_weather.select("*", F.col("clouds.*")) \
    .drop("clouds")

# rename all column to Cloudiness
df_weather = df_weather.withColumnRenamed("all", "Cloudiness")

# for main column
df_weather = df_weather.select("*", F.col("main.*")) \
    .drop("main")

# for  rain column
df_weather = df_weather.select("*", F.col("rain.*")) \
    .drop("rain")

# for sys column
df_weather = df_weather.select("*", F.col("sys.*")) \
    .drop("sys")

#   "weather": [
#     {
#         "description": "scattered clouds",
#         "icon": "03n",
#         "id": 802,
#         "main": "Clouds"
#     }
# ],
# for weather column
df_weather = df_weather.select("*", F.col("weather[0].*")) \
    .drop("weather")

# for wind column
df_weather = df_weather.select("*", F.col("wind.*")) \
    .drop("wind")

df_weather.printSchema()

#################### NOT working for streaming data ############################
# for loop to expand  every nested column elements  and drop the nested column
# for col in df_weather.columns:
#     if type(df_weather.select(col).take(1)[0][0]) == dict:
#         df_weather = df_weather.select(
#             "*", F.col(col+".*")).drop(col)


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
