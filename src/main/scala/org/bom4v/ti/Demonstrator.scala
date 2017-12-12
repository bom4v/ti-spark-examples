package org.bom4v.ti

//import org.apache.spark._
import org.apache.spark.sql.functions.not
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType}
//import org.apache.spark.sql._
//import org.apache.spark.sql.Dataset
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.feature.VectorAssembler
// Bom4V
import org.bom4v.ti.models.customers.CustomerAccount.AccountModelForChurn
import org.bom4v.ti.models.calls.CallsModel.CallEvent
import org.bom4v.ti.serializers.customers.CustomerAccount._
import org.bom4v.ti.serializers.calls.CallsModel._
//data preparation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType


object Demonstrator extends App {

  // Spark 2.x way, with a SparkSession
  // https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html
  val spark = org.apache.spark.sql.SparkSession
    .builder()
    .appName("SparkSessionForBom4VDemonstrator")
    .config("spark.master", "local")
    .getOrCreate()

  // CSV data file, from the local file-system
  val cdrDataFilepath = "data/cdr/CDR-sample.csv"
  // CSV data file, from HDFS
  // (check the fs.defaultFS property in the $HADOOP_CONF_DIR/core-site.xml file)
  // val cdrDataFilepath = "hdfs://localhost:8020/data/bom4v/CDR-sample.csv"

  //
  import spark.implicits._

  // CDR data set
  val cdr: org.apache.spark.sql.Dataset[CallEvent] = spark.read
    .schema(callEventSchema)
    .option("inferSchema", "false")
    .option("header", "true")
    .option("delimiter", "^")
    .csv(cdrDataFilepath)
    .as[CallEvent]

  //
  cdr.printSchema()
  cdr.show()

  // CSV data file, from the local file-system
  val churn20DataFilepath = "data/churn/churn-bigml-20.csv"
  val churn80DataFilepath = "data/churn/churn-bigml-80.csv"
  // CSV data file, from HDFS
  // (check the fs.defaultFS property in the $HADOOP_CONF_DIR/core-site.xml file)
  // val churn20DataFilepath = "hdfs://localhost:8020/data/churn/churn-bigml-20.csv"
  // val churn80DataFilepath = "hdfs://localhost:8020/data/churn/churn-bigml-80.csv"

  // Training data set
  val train: org.apache.spark.sql.Dataset[AccountModelForChurn] = spark.read
    .option("inferSchema", "false")
    .schema(customerAccountSchema)
    .option("header", "true")
    .csv(churn80DataFilepath)
    .as[AccountModelForChurn]

  //
  train.take(1)
  train.cache

  //
  println(train.count)

  // Test data set
  val test: org.apache.spark.sql.Dataset[AccountModelForChurn] = spark.read
    .option("inferSchema", "false")
    .schema(customerAccountSchema)
    .option("header", "true")
    .csv(churn20DataFilepath)
    .as[AccountModelForChurn]

  //
  test.take(2)
  test.cache

  //
  println(test.count)

  //
  train.printSchema()
  train.show()
  train.createOrReplaceTempView("account")
  spark.catalog.cacheTable("account")

  train.groupBy("churn").count.show()

  val fractions = Map("False" -> .17, "True" -> 1.0)
  //Here we're keeping all instances of the Churn=True class, but downsampling the Churn=False class to a fraction of 388/2278.
  val strain = train.stat.sampleBy("churn", fractions, 36L)

  strain.groupBy("churn").count.show()
  val ntrain = strain.drop("state").drop("acode").drop("vplan").drop("tdcharge").drop("techarge")
  println(ntrain.count)
  ntrain.show()

  val ipindexer = new StringIndexer()
    .setInputCol("intlplan")
    .setOutputCol("iplanIndex")
  val labelindexer = new StringIndexer()
    .setInputCol("churn")
    .setOutputCol("label")
  val featureCols = Array("len", "iplanIndex", "numvmail", "tdmins", "tdcalls", "temins", "tecalls", "tnmins", "tncalls", "timins", "ticalls", "numcs")

  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")

  val dTree = new DecisionTreeClassifier().setLabelCol("label")
    .setFeaturesCol("features")

  // Chain indexers and tree in a Pipeline.
  val pipeline = new Pipeline()
    .setStages(Array(ipindexer, labelindexer, assembler, dTree))
  // Search through decision tree's maxDepth parameter for best model
  val paramGrid = new ParamGridBuilder().addGrid(dTree.maxDepth, Array(2, 3, 4, 5, 6, 7)).build()

  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("prediction")

  // Set up 3-fold cross validation
  val crossval = new CrossValidator().setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid).setNumFolds(3)

  val cvModel = crossval.fit(ntrain)

  val bestModel = cvModel.bestModel
  println("The Best Model and Parameters:\n--------------------")
  println(bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3))
  bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
    .stages(3)
    .extractParamMap

  val treeModel = bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3).asInstanceOf[DecisionTreeClassificationModel]
  println("Learned classification tree model:\n" + treeModel.toDebugString)

  val predictions = cvModel.transform(test)
  val accuracy = evaluator.evaluate(predictions)
  evaluator.explainParams()

  val predictionAndLabels = predictions.select("prediction", "label").rdd.map(x =>
    (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
  val metrics = new BinaryClassificationMetrics(predictionAndLabels)
  println("area under the precision-recall curve: " + metrics.areaUnderPR)
  println("area under the receiver operating characteristic (ROC) curve : " + metrics.areaUnderROC)

  println(metrics.fMeasureByThreshold())

  val result = predictions.select("label", "prediction", "probability")
  result.show()

  val lp = predictions.select("label", "prediction")
  val counttotal = predictions.count()
  val correct = lp.filter($"label" === $"prediction").count()
  val wrong = lp.filter(not($"label" === $"prediction")).count()
  val ratioWrong = wrong.toDouble / counttotal.toDouble
  val ratioCorrect = correct.toDouble / counttotal.toDouble
  val truep = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count() / counttotal.toDouble
  val truen = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count() / counttotal.toDouble
  val falsep = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble
  val falsen = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble

  println("counttotal : " + counttotal)
  println("correct : " + correct)
  println("wrong: " + wrong)
  println("ratio wrong: " + ratioWrong)
  println("ratio correct: " + ratioCorrect)
  println("ratio true positive : " + truep)
  println("ratio false positive : " + falsep)
  println("ratio true negative : " + truen)
  println("ratio false negative : " + falsen)

  println("wrong: " + wrong)

  val equalp = predictions.selectExpr(
    "double(round(prediction)) as prediction", "label",
    """CASE double(round(prediction)) = label WHEN true then 1 ELSE 0 END as equal"""
  )
  equalp.show()

  //data preparation
  val cdr_data_t = spark
    .read
    .option("header","true")
    .option("inferSchema","true")
    .option("delimiter", "^")
    .csv("data/cdr/cdr_example.csv")

  val cdr_data_temp = cdr_data_t
    .selectExpr (
    "specificationVersionNumber",
 	"releaseVersionNumber",
 	"fileName",
 	"fileAvailableTimeStamp",
 	"fileUtcTimeOffset",
 	"sender",
 	"recipient",
 	"sequenceNumber",
 	"cast(callEventsCount as Int) callEventsCount",
 	"eventType",
 	"cast(imsi as String) imsi",
 	"cast(imei as String) imei",
 	"callEventStartTimeStamp",
 	"utcTimeOffset",
 	"callEventDuration",
 	"causeForTermination",
 	"accessPointNameNI",
 	"accessPointNameOI",
 	"cast(dataVolumeIncoming as Int) dataVolumeIncoming",
 	"cast(dataVolumeOutgoing as Int) dataVolumeOutgoing",
 	"sgsnAddress",
 	"ggsnAddress",
 	"chargingId",
 	"cast(chargeAmount as Int) chargeAmount",
 	"teleServiceCode",
 	"bearerServiceCode",
 	"supplementaryServiceCode",
 	"cast(dialledDigits as String) dialledDigits",
 	"cast(connectedNumber as String) connectedNumber",
 	"cast(thirdPartyNumber as String) thirdPartyNumber",
 	"cast(callingNumber as String) callingNumber",
 	"recEntityId",
 	"callReference",
 	"locationArea",
 	"cellId",
 	"cast(msisdn as String) msisdn",
 	"servingNetwork")

  val cdr_data = cdr_data_temp
    .withColumn (
    "callEventStartDay",
      cdr_data_temp ("callEventStartTimeStamp")
        .cast(DateType))

  val callEventDuration = cdr_data
    .groupBy ("imsi")
    .agg(mean ("callEventDuration"), min("callEventDuration"),
	  max("callEventDuration"), sum("callEventDuration"))

  var prefix = "callEventDuration_"
  var renamedColumns = callEventDuration
    .columns
    .map (c => callEventDuration(c).as(s"$prefix$c"))
  val callEventDurationRename = callEventDuration.select(renamedColumns: _*)

  val dataVolumeIncoming = cdr_data
    .groupBy("imsi").agg(mean("dataVolumeIncoming"), min("dataVolumeIncoming"),
	max("dataVolumeIncoming"), sum("dataVolumeIncoming"))

  prefix = "dataVolumeIncoming_"
  renamedColumns = dataVolumeIncoming.columns.map(c=> dataVolumeIncoming(c).as(s"$prefix$c"))
  val dataVolumeIncomingRename = dataVolumeIncoming.select(renamedColumns: _*)

  val dataVolumeOutgoing = cdr_data.groupBy("imsi").agg(mean("dataVolumeOutgoing"), min("dataVolumeOutgoing"),
	max("dataVolumeOutgoing"), sum("dataVolumeOutgoing"))

  prefix = "dataVolumeOutgoing_"
  renamedColumns = dataVolumeOutgoing.columns.map(c=> dataVolumeOutgoing(c).as(s"$prefix$c"))
  val dataVolumeOutgoingRename = dataVolumeOutgoing.select(renamedColumns: _*)

  val chargeAmount=cdr_data.groupBy("imsi").agg(mean("chargeAmount"), min("chargeAmount"),
	max("chargeAmount"), sum("chargeAmount"))

  prefix = "chargeAmount_"
  renamedColumns = chargeAmount.columns.map(c=> chargeAmount(c).as(s"$prefix$c"))
  val chargeAmountRename = chargeAmount.select(renamedColumns: _*)

  val callingNumberN = cdr_data.groupBy("imsi").agg(countDistinct("callingNumber"))
  val locationAreaN = cdr_data.groupBy("imsi").agg(countDistinct("locationArea"))
  val cellIdN = cdr_data.groupBy("imsi").agg(countDistinct("cellId"))
  val activeDaysN = cdr_data.groupBy("imsi").agg(countDistinct("callEventStartDay"))

  val callingNumbersTechN = cdr_data.groupBy("imsi","callingNumber").count()
  val locationAreasTechN = cdr_data.groupBy("imsi","locationArea").count()
  val cellIdsTechN = cdr_data.groupBy("imsi","cellId").count()
  val activeDaysTechN = cdr_data.groupBy("imsi","callEventStartDay").count()

  val locationAreasTech = cdr_data
    .groupBy("imsi","locationArea")
    .pivot("locationArea")
    .agg(mean("callEventDuration"), min("callEventDuration"),
	  max("callEventDuration"), sum("callEventDuration"))
  val cellIdsTech = cdr_data
    .groupBy("imsi","cellId")
    .pivot("cellId")
    .agg(mean("callEventDuration"), min("callEventDuration"),
	  max("callEventDuration"), sum("callEventDuration"))
  val activeDaysTech = cdr_data
    .groupBy("imsi","callEventStartDay")
    .pivot("callEventStartDay")
    .agg(mean("callEventDuration"), min("callEventDuration"),
	  max("callEventDuration"), sum("callEventDuration"))

  // join
  val cdr_data_start = callEventDurationRename
    .join (dataVolumeIncomingRename,
      dataVolumeIncomingRename.col("dataVolumeIncoming_imsi") === callEventDurationRename.col("callEventDuration_imsi"))

  val cdr_data_start1 = cdr_data_start
    .drop ("dataVolumeIncoming_imsi")
    .withColumnRenamed ("callEventDuration_imsi", "imsi_id")

  val cdr_data_trans = cdr_data_start1
    .join (dataVolumeOutgoingRename, dataVolumeOutgoingRename.col("dataVolumeOutgoing_imsi") === cdr_data_start1.col("imsi_id"))
    .join(chargeAmountRename, chargeAmountRename.col("chargeAmount_imsi") === cdr_data_start1.col("imsi_id"))
    .join(callingNumberN, callingNumberN.col("imsi") === cdr_data_start1.col("imsi_id"))
    .join(locationAreaN, locationAreaN.col("imsi") === cdr_data_start1.col("imsi_id"))
    .join(cellIdN, cellIdN.col("imsi") === cdr_data_start1.col("imsi_id"))
    .join(activeDaysN, activeDaysN.col("imsi") === cdr_data_start1.col("imsi_id"))
    .join(callingNumbersTechN, callingNumbersTechN.col("imsi") === cdr_data_start1.col("imsi_id"))
    .join(locationAreasTechN, locationAreasTechN.col("imsi") === cdr_data_start1.col("imsi_id"))
    .join(cellIdsTechN, cellIdsTechN.col("imsi") === cdr_data_start1.col("imsi_id"))
    .join(activeDaysTechN, activeDaysTechN.col("imsi") === cdr_data_start1.col("imsi_id"))

  val cdr_data_transponed = cdr_data_trans
    .drop("dataVolumeOutgoing_imsi", "chargeAmount_imsi", "imsi")

  // Stop Spark
  spark.stop()
}
