from allsecrets import *
from titles import *
from coordinates import *
import json
import requests
from json import dumps, loads
# dumps converts python object to json string
# loads converts json string to python object


from kafka import KafkaProducer

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('OMDB')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
    .getOrCreate()


# zookeeper and kafka broker should be running in the background
producer = KafkaProducer(bootstrap_servers=[
                         'localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'), key_serializer=lambda x: dumps(x).encode('utf-8'))
# value_serializer=lambda x: dumps(x).encode('utf-8') is used to convert the data into json format


# for lat, lon in lat_lon.items():
#     parameters = {'lat': lat, "lon": lon, "appid": open_weather_api}
#     weather_response = requests.get(
#         "http://api.openweathermap.org/data/2.5/forecast", params=parameters)

#     # producer.send('weather', value=weather_response.json())
#     # producer.flush()  # flush the data to the kafka broker ( topic) and  make sure data  is sent to the kafka broker and  not lost in the buffer



for lat, lon in lat_lon.items():
    parameters = {'lat': lat, "lon": lon, "appid": open_weather_api}
    weather_response = requests.get(
        "http://api.openweathermap.org/data/2.5/forecast", params=parameters)

    producer.send('weather', value=weather_response.json())
producer.flush()  # flush the data to the kafka broker ( topic) and  make sure data  is sent to the kafka broker and  not lost in the buffer