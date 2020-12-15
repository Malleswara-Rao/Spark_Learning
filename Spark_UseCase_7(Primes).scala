// Databricks notebook source
val data = sc.textFile("dbfs:/FileStore/shared_uploads/malleshtiie@gmail.com/numberData.csv")
data.collect()

// COMMAND ----------

def headerRemover(line: String): Boolean = !(line.startsWith("Number"))

// COMMAND ----------

val finalrdd = data.filter( x => headerRemover(x))
finalrdd.take(2)

// COMMAND ----------

// DBTITLE 1,Function to Extract Prime Numbers
def isPrime2(i :Int) : Boolean = 
{
  if (i <= 1)
      false
  else if (i == 2)
      true
  else
      !(2 to (i-1)).exists(x => i % x == 0)
 }

// COMMAND ----------

val numberRdd = finalrdd.map(x => x.toInt)
numberRdd.take(10)

// COMMAND ----------

numberRdd.filter(i => (isPrime2(i))).sum()
