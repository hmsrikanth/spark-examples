from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.json("test.json")
df.createOrReplaceTempView("test")

spark.sql("select article.adultLanguage, company.* from test lateral view explode(articles) as article lateral view explode(article.companies) as company ").show(10,False)