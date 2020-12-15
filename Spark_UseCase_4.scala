// Databricks notebook source
val julyrdd = sc.textFile("/FileStore/shared_uploads/malleshtiie@gmail.com/nasa_19950701.tsv")
val augustrdd = sc.textFile("/FileStore/shared_uploads/malleshtiie@gmail.com/nasa_19950801.tsv")

// COMMAND ----------

val julyRdd = julyrdd.map(line => line.split("\t")(0))
val augustRdd = augustrdd.map(line => line.split("\t")(0))
julyRdd.collect()
augustRdd.collect()

// COMMAND ----------

val interRdd = julyRdd.intersection(augustRdd)
interRdd.count()
