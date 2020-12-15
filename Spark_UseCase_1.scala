// Databricks notebook source
//creating an rdd from external dataset
val data = sc.textFile("dbfs:/FileStore/shared_uploads/malleshtiie@gmail.com/ratings.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val ratingsdata = data.map(x => x.split(",")(2))

ratingsdata.collect()

// COMMAND ----------

ratingsdata.count()

// COMMAND ----------

ratingsdata.countByValue()

// COMMAND ----------

val airportrdd = sc.textFile("dbfs:/FileStore/shared_uploads/malleshtiie@gmail.com/airports.text")

// COMMAND ----------

airportrdd.collect()

// COMMAND ----------

airportrdd.map(x => x.split(",")).collect()

// COMMAND ----------

airportrdd.map(x => x.split(",")(3)).collect()

// COMMAND ----------

// DBTITLE 1,METHOD - 1
val usardd = airportrdd.filter(x => x.split(",")(3) == "\"United States\"")
usardd.collect()

// COMMAND ----------

//Using function
 
def splitInput(line:String) = 
{
  val dataSplit = line.split(",")
  val airportid = dataSplit(1)
  val cityname = dataSplit(2)

  (airportid,cityname)
}

usardd.map(splitInput).take(2)

// COMMAND ----------

// DBTITLE 1,METHOD - 2
usardd.map( line => { val splitdata = line.split(",") 
                      splitdata(1)+" "+splitdata(2)}).take(2)
