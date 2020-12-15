// Databricks notebook source
val airport = sc.textFile("dbfs:/FileStore/shared_uploads/malleshtiie@gmail.com/airports.text")
airport.collect()

// COMMAND ----------

val country = airport.filter(x=>x.split(",")(3) == "\"United States\"").collect().take(3)

// COMMAND ----------

val task1 = airport.map(x=>(x.split(",")(9).toFloat, x.split(",")(11)))
 
task1.take(20)

// COMMAND ----------

val task2 = task1.filter(x=> x._1 % 2.0 == 0)
 task2.take(20)

// COMMAND ----------

task2.countByValue().take(20)

// COMMAND ----------

val task3 = airport.map(x=>(x.split(",")(3), x.split(",")(7).toDouble))
task3.take(10)

// COMMAND ----------

task3.filter(x=> x._1 == "\"Iceland\"" && x._2 > -16.0 ).collect()
