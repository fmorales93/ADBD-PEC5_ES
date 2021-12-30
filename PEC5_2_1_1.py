import findspark
findspark.init()
import sys
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from IPython.display import display, clear_output
from time import sleep

conf = SparkConf()
conf.setMaster("local[1]")
sc = SparkContext(conf=conf)
print(sc.version)

# Introducid el nombre de la app PEC5_ seguido de vuestro nombre de usuario
spark = SparkSession \
    .builder \
    .appName("PEC5_fmoralesh") \
    .getOrCreate()

# Creamos el DataFrame representando el streaming de las lineas que nos entran por host:port
linesDF = spark\
    .readStream\
    .format('socket')\
    .option('host', sys.argv[1])\
    .option('port', sys.argv[2])\
    .load()

# Separamos las lineas en palabras en un nuevo DF
#las funciones explode y split estan explicadas en
#https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html
wordsDF = linesDF.select(
    explode(
        split(linesDF.value, ' ')
    ).alias('palabra')
)

# Generamos el word count en tiempo de ejecución
wordCountsDF = wordsDF.groupBy('palabra').count()

# Iniciamos la consuta que muestra por consola o almacena en memoria el word count. 
# Trabajamos a partir del DataFrame que contiene la agrupación de las palabras y el numero de repeticiones
# Utilizamos el formato memory para poder mostrarlo en Notebook, 
#si ejecutamos en consola debemos poner el formato console
query = wordCountsDF\
    .writeStream\
    .outputMode('complete')\
    .format("memory") \
    .queryName("palabras") \
    .start()

#en una ejecución desde el terminal de sistema, necesitamos evitar que el programa finalice mientras 
#se está ejecutando la consulta en un Thread separado y en segundo plano. 
#query.awaitTermination() 

while True:
    clear_output(wait=True)
    display(query.status)
    display(spark.sql('SELECT * FROM palabras WHERE palabra LIKE "A%" AND count > 5').show())
    sleep(5)