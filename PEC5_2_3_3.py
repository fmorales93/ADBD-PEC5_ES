import findspark
findspark.init()

import sys
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructField, StructType
from pyspark.sql.functions import explode, from_json, col
from pyspark.sql.functions import split
from IPython.display import display, clear_output
from time import sleep

conf = SparkConf()
conf.setMaster("local[1]")
sc = SparkContext(conf=conf)
print(sc.version)

spark = SparkSession \
    .builder \
    .appName("PEC5_fmoralesh") \
    .getOrCreate()

linesDF = spark\
    .readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 20036)\
    .load()

schema = StructType([
            StructField('country', StringType(), True),
            StructField('callsign', StringType(), True),
            StructField('longitude', FloatType(), True),
            StructField('latitude', FloatType(), True),
            StructField('velocity', FloatType(), True),
            StructField('vertical_rate', FloatType(), True)
])


vuelosDF = linesDF.select(
    explode(
        split(linesDF.value, '\n')
    ).alias('vuelos')
)

vuelosDF = vuelosDF.withColumn("json",from_json(col('vuelos'),schema)) \
                    .select('json.*')\
                    .filter(col("vuelos").rlike("^(?!.*null.*).*$"))

query = vuelosDF\
    .writeStream\
    .outputMode('update')\
    .format("console") \
    .queryName("vuelos") \
    .option('truncate', 'false')\
    .start()


while query.isActive:
    print("\n")
    vuelosDF.printSchema()
    sleep(5)

query.awaitTermination()