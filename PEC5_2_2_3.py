import findspark
findspark.init()

from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import sys
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col, window
from time import sleep

conf = SparkConf()
conf.setMaster("local[*]")
sc = SparkContext(conf=conf)
print(sc.version)

ssc = StreamingContext(sc, 1)

# Introducid el nombre de la app PEC5_ seguido de vuestro nombre de usuario
spark = SparkSession \
    .builder \
    .appName("PEC5_fmoralesh") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.metricsEnabled", "true")


# Creamos el DataFrame representando el streaming de las lineas que nos entran por host:port
linesDF = spark\
    .readStream\
    .format('rate').option("rowsPerSecond", 1).load()

# Separamos las lineas en palabras en un nuevo DF
#las funciones explode y split estan explicadas en
#https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html
wordsDF = linesDF

# Generamos el word count en tiempo de ejecución

wordCountsDF = wordsDF.withColumn(
    "window",
    window(
         "timestamp", 
         windowDuration="10 seconds"
    )
).groupBy("window", "value").count().select("window", "value", "count")


# Iniciamos la consuta que muestra por consola o almacena en memoria el word count. 
# Trabajamos a partir del DataFrame que contiene la agrupación de las palabras y el numero de repeticiones
# Utilizamos el formato memory para poder mostrarlo en Notebook, 
#si ejecutamos en consola debemos poner el formato console


query = wordCountsDF\
    .writeStream\
    .outputMode('update')\
    .format("console") \
    .option('truncate', 'false')\
    .queryName("palabras") \
    .trigger(processingTime="5 second")\
    .start()


while query.isActive:
    print("\n")
    print(query.status)
    print(query.lastProgress)
    sleep(5)

query.awaitTermination()