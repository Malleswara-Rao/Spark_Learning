// Databricks notebook source
val data = List(1,2,3,4,5)

// COMMAND ----------

//creationRDD command is used to create an RDD
//sc is spark context -> type of objects in databricks used to work with cluster
//parallelize method is used to create rdd and is lazy in nature
 
val creationRDD = sc.parallelize(data)

// COMMAND ----------

// to get result of rdd -> action should be performed on rdd
 
creationRDD.collect()

// COMMAND ----------

// to get toatl partitions for our data
 
creationRDD.partitions.size

// COMMAND ----------

// to give our own partition
 
val rddpartition = sc.parallelize(List(1,2,3,4),2)

// COMMAND ----------

rddpartition.partitions.size

// COMMAND ----------

//To get the number of values of element
rddpartition.count()

// COMMAND ----------

// map -> transformation
// using map to create new rdd from an existing rdd(rddpartition)
val maprdd = rddpartition.map(x => x * x * x)

// COMMAND ----------

maprdd.filter(x => x % 2 == 0).collect()

// COMMAND ----------

val mainrdd = sc.parallelize(List("hey","Mallesh"))
mainrdd.collect()

// COMMAND ----------

// creating rdd for keyvalue pair
 
val rdd0 = sc.parallelize(Array("one","two","three","one","two"))

// COMMAND ----------

val keyrdd = rdd0.map(x => (x,1))
keyrdd.collect()

// COMMAND ----------

keyrdd.reduceByKey(_+_).collect()
