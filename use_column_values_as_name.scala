// Databricks notebook source
val df = Seq(
    ("A", 14, 26, 32),
    ("C", 13, 17, 96),
    ("B", 23, 19, 42),
).toDF("score_col", "A", "B", "C")

// COMMAND ----------

import org.apache.spark.sql.functions._

val scoreMap = map(
  df.columns
    .flatMap(c => Seq(lit(c), col(c))): _*
)

val df2 = df.withColumn("score", scoreMap(col("score_col")))

// COMMAND ----------

df2.show(false)
