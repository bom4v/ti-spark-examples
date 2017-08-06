package org.bom4v.ti

object Demonstrator extends App {

  // Spark 2.x way, with a SparkSession
  // https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("SparkSessionForBom4VDemonstrator")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  // CSV data file, from the local file-system
  val dataFilepath = "data/cdr/CDR-sample.csv"
  // CSV data file, from HDFS
  // (check the fs.defaultFS property in the $HADOOP_CONF_DIR/core-site.xml file)
  // val dataFilepath = "hdfs://localhost:8020/data/bom4v/CDR-sample.csv"

  // Spark 2x
  val cdrDF = spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true")       // Use first line of all files as header
    .option("inferSchema", "true")  // Automatically infer data types
    .option("delimiter", "^")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .load(dataFilepath)

  // Print the schema of this input
  println ("cdrDF:")
  cdrDF.printSchema()
  
  // Sample 3 records along with headers
  cdrDF.show (3)
  
  // Show all the records along with headers 
  cdrDF.show ()  
  
  // Sample the first 5 records
  cdrDF.head(5).foreach (println)
  
  // Alias of head
  cdrDF.take(5).foreach (println)
    
  // Select just the emitter MSISDN to a different DataFrame
  val emitterDF:org.apache.spark.sql.DataFrame = cdrDF.select ("callingNumber")
  println ("emitterDF:")
  emitterDF.show(3)
  
  // Select more than one column and create a different DataFrame
  val emitterReceiverDF = cdrDF.select ("recEntityId", "callingNumber")
  println ("emitterReceiver:")
  emitterReceiverDF.show(3)
  
  // Print the first 5 records, which have a time-stamp greater than some specific one
  cdrDF.filter (cdrDF("callEventStartTimeStamp").gt("2017-04-26 21:02:51")).show(7)
  
  // Records with no receiver number
  cdrDF.filter ("recEntityId =''").show(7)
  
  // Show all records, for which receiver numbers are empty or null
  cdrDF.filter ("recEntityId ='' OR recEntityId = 'NULL'").show(7)
  
  // Get all records, for which numbers start with the digits '3104'
  cdrDF.filter ("SUBSTR(recEntityId, 0, 4) ='3104'").show(7)
  
  // The real power of DataFrames lies in the way we could treat it
  // like a relational table and use SQL to query
  // Step 1. Register the CDR DataFrame as a table
  cdrDF.createOrReplaceTempView ("cdr")

  // Step 2. Query it away
  val dfFilteredBySQL = spark.sql ("select * from cdr where recEntityId != '' order by callingNumber desc")
  println ("dfFilteredBySQL:")
  dfFilteredBySQL.show(7)
  
  // You could also optionally order the DataFrame by column
  // without registering it as a table.
  // Order by descending order
  cdrDF.sort (cdrDF ("recEntityId").desc).show(10)
  
  // Order by a list of column names - without using SQL
  cdrDF.sort ("recEntityId", "callEventStartTimeStamp").show(10)
  
  // Now, let's save the modified DataFrame with a new name
  val options = Map ("header" -> "true", "path" -> "altCDR.csv")
  
  // Modify DataFrame - pick 'recEntityId' and 'callingNumber' columns,
  // change 'recEntityId' column name to just 'number'
  val copyOfCDRDF = cdrDF
    .select (cdrDF ("recEntityId").as("number"), cdrDF ("callingNumber"))
  println ("copyOfCDRDF:")
  copyOfCDRDF.show()

  // Save this new dataframe with headers
  // and with file name "altCDR.csv"
  copyOfCDRDF.write
    .format ("com.databricks.spark.csv")
    .mode (org.apache.spark.sql.SaveMode.Overwrite)
    .options (options).save
  
  // Load the saved data and verify the schema and list some records
  // Instead of using the csvFile, you could do a 'load' 
  val newCDRDF = spark.read
    .format ("com.databricks.spark.csv")
    .options (options).load
  //
  println ("newCDRDF:")
  newCDRDF.printSchema()
  newCDRDF.show()  

  // Stop Spark
  spark.stop()
}

