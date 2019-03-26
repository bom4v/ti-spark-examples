name := "ti-spark-examples"

organization := "org.bom4v.ti"

organizationName := "Business Object Models for Verticals (BOM4V)"

organizationHomepage := Some(url("http://github.com/bom4v"))

version := "0.0.1-spark2.3"

homepage := Some(url("https://github.com/bom4v/ti-spark-examples"))

startYear := Some(2019)

description := "Sample/demonstration project for the Spark layer of BOM for Verticals"

licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")

scmInfo := Some(
  ScmInfo(
    url("https://github.com/bom4v/ti-spark-examples"),
    "https://github.com/bom4v/ti-spark-examples.git"
  )
)

developers := List(
  Developer(
    id    = "denis.arnaud",
    name  = "Denis Arnaud",
    email = "denis.arnaud_ossrh@m4x.org",
    url   = url("https://github.com/denisarnaud")
  )
)

//useGpg := true

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8", "2.11.12")

checksums in update := Nil

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"

libraryDependencies += "org.specs2" %% "specs2-core" % "4.4.1" % "test"

libraryDependencies += "com.github.hirofumi" %% "xgboost4j-spark" % "0.7.1-p1"

libraryDependencies += "org.bom4v.ti" %% "ti-models-customers" % "0.0.1"

libraryDependencies += "org.bom4v.ti" %% "ti-models-calls" % "0.0.1"

libraryDependencies ++= (version.value match {
    case v if v.contains("spark2.3") => Seq(
      "org.bom4v.ti" %% "ti-spark-all" % "0.0.1-spark2.3"
    )
    case v if v.contains("spark2.2") => Seq(
      "org.bom4v.ti" %% "ti-spark-all" % "0.0.1-spark2.2"
    )
  }
)  

// Hadoop
//val hadoopVersion = "3.1.1"
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
//libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
//libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion

// Spark
libraryDependencies ++= (version.value match {
    case v if v.contains("spark2.3") => Seq(
        "org.apache.spark" %% "spark-core" % "2.3.2",
        "org.apache.spark" %% "spark-sql" % "2.3.2",
        "org.apache.spark" %% "spark-mllib" % "2.3.2",
        "org.apache.spark" %% "spark-hive" % "2.3.2"
    )
    case v if v.contains("spark2.2") => Seq(
        "org.apache.spark" %% "spark-core" % "2.2.0",
        "org.apache.spark" %% "spark-sql" % "2.2.0",
        "org.apache.spark" %% "spark-mllib" % "2.2.0",
        "org.apache.spark" %% "spark-hive" % "2.2.0"
    )
  }
)

javacOptions in Compile ++= Seq("-source", "1.8",  "-target", "1.8")

scalacOptions ++= Seq("-deprecation", "-feature")

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

cleanKeepFiles += target.value / "test-reports"

