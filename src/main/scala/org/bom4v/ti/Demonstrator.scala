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

  // Stop Spark
  spark.stop()
}
