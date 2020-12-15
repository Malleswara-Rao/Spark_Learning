// Databricks notebook source
//importing data 
val julyrdd = sc.textFile("dbfs:/FileStore/shared_uploads/malleshtiie@gmail.com/nasa_19950701.tsv")
val augustrdd = sc.textFile("dbfs:/FileStore/shared_uploads/malleshtiie@gmail.com/nasa_19950801.tsv")

// COMMAND ----------

val unionrdd = julyrdd.union(augustrdd)
unionrdd.take(2)

// COMMAND ----------

// DBTITLE 1,Setting 1st Row As HEADER
val header = unionrdd.first

// COMMAND ----------

// DBTITLE 1,Displaying Data Without HEADER
unionrdd.filter(line => line != header).take(3)

// COMMAND ----------

// DBTITLE 1,Alternate Method to Remove Header --> Using Function
def headerRemover(line: String): Boolean = !(line.startsWith("host"))

// COMMAND ----------

val finalrdd = unionrdd.filter( x => headerRemover(x))
finalrdd.take(2)

// COMMAND ----------

// DBTITLE 1,Data Sampling
finalrdd.sample(withReplacement = true, fraction = 0.20).collect()
