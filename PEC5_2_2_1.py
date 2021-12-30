import findspark
findspark.init()

from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, length
from pyspark.sql.functions import split, size, col, window
from IPython.display import display, clear_output
from time import sleep

conf = SparkConf()
conf.setMaster("local[*]")
sc = SparkContext(conf=conf)
print(sc.version)
ssc = StreamingContext(sc, 1)

spark = SparkSession \
    .builder \
    .appName("PEC5_fmoralesh") \
    .getOrCreate()

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

query.awaitTermination()