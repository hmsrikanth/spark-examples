from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

file_1 = {"version": 1, "stats": {"hits": 20}}

file_2 = {"version": 2, "stats": [{"hour": 1, "hits": 10}, {"hour": 2, "hits": 12}]}

schema = spark.read.json(sc.parallelize([file_2])).withColumn('stats', explode('stats')).schema

spark.read.schema(schema).json(sc.parallelize([file_1, file_2])).printSchema()