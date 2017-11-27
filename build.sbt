name := "ti-spark-examples"

organization := "org.bom4v.ti"

version := "0.0.1"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.11.6", "2.11.11")

checksums in update := Nil

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.9.4" % "test"

libraryDependencies += "org.bom4v.ti" %% "ti-models-customers" % "0.0.1"

libraryDependencies += "org.bom4v.ti" %% "ti-models-calls" % "0.0.1"

libraryDependencies += "org.bom4v.ti" %% "ti-serializers-customers" % "0.0.1"

libraryDependencies += "org.bom4v.ti" %% "ti-serializers-calls" % "0.0.1"

// Hadoop
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.8.1"
//libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.8.1"
//libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % "2.8.1"

// Spark
val sparkVersion = "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snaspshots"),
  "Local repository"     at "http://localhost/mavenrepo/",
  Resolver.mavenLocal
)


javacOptions in Compile ++= Seq("-source", "1.8",  "-target", "1.8")

scalacOptions ++= Seq("-deprecation", "-feature")

publishTo := Some("TI Maven Repo" at "http://localhost/mavenrepo/")

cleanKeepFiles += target.value / "test-reports"

