from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType,StructField,StringType
spark = SparkSession.builder.getOrCreate()

schema = StructType([
  StructField("a", StringType(), True),
  StructField("b", StringType(),  True),
  StructField("json", StringType(), True)
])

data = [("a","b",'{"msg_id":"123","msg":"test"}'),("c","d",'{"msg_id":"456","column1":"test"}')]

df = spark.createDataFrame(data,schema)
json_schema = spark.read.json(df.rdd.map(lambda row: row.json)).schema
df2 = df.withColumn('parsed', from_json(col('json'), json_schema))
df2.createOrReplaceTempView("test")
spark.sql("select a,b,parsed.msg_id from test").show()


