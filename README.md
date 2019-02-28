Spark Layer of the BOM for Verticals
====================================

# References
* [This GitHub repository](https://github.com/bom4v/ti-spark-examples)
  is a component of the [BOM4V project](https://github.com/bom4v/metamodels),
  aiming at demonstrating end-to-end Spark-based examples
  of Machine Learning (ML) pipelines, for instance
  churn detection in telecoms and transport industries.
* [Central Maven repository with BOM4V Jar artefacts](https://repo1.maven.org/maven2/org/bom4v/ti/)
* [Docker cloud with ready-to-use images](https://cloud.docker.com/u/bigdatadevelopment/repository/docker/bigdatadevelopment/base)

## Machine Learning (ML)
* [Churn Prediction with Apache Spark Machine Learning](https://mapr.com/blog/churn-prediction-sparkml/),
  by [Carol McDonald](https://mapr.com/blog/author/carol-mcdonald/) on MapR blog, 5 June 2017
* [Realtime prediction using Spark Structured Streaming, XGBoost and Scala](https://towardsdatascience.com/realtime-prediction-using-spark-structured-streaming-xgboost-and-scala-d4869a9a4c66),
  by [Bogdan Cojocar](https://towardsdatascience.com/@bogdan.cojocar) on Medium, 24 June 2018
  + [Source code on GitHub](https://github.com/BogdanCojocar/medium-articles/tree/master/titanic_spark)

# Installation

## Short version
Just add the dependency on `ti-spark-examples` in the SBT project
configuration (typically, `build.sbt` in the project root directory):
```scala
libraryDependencies += "org.bom4v.ti" %% "ti-spark-examples" % "0.0.1-spark2.3"
```

# Run the demonstrator
```bash
$ mkdir -p ~/dev/ti
$ cd ~/dev/ti
$ git clone https://github.com/bom4v/metamodels.git
$ cd metamodels
$ rake clone && rake checkout
$ rake offline=true deliver
$ cd workspace/src/ti-spark-examples
$ ./fillLocalDataDir.sh
$ sbt run
[info] Loading global plugins from ~/.sbt/1.0/plugins
[info] Loading project definition from ~/dev/ti/metamodels/workspace/src/ti-spark-examples/project
[info] Set current project to ti-spark-examples (in build file:~/dev/ti/metamodels/workspace/src/ti-spark-examples/)
[info] Compiling 1 Scala source to ~/dev/ti/metamodels/workspace/src/ti-spark-examples/target/scala-2.11/classes...
[info] Running org.bom4v.ti.Demonstrator 
17/08/06 18:04:26 INFO DataNucleus.Persistence: Property hive.metastore.integral.jdo.pushdown unknown - will be ignored
17/08/06 18:04:26 INFO DataNucleus.Persistence: Property datanucleus.cache.level2 unknown - will be ignored
17/08/06 18:04:28 INFO DataNucleus.Datastore: The class "org.apache.hadoop.hive.metastore.model.MFieldSchema" is tagged as "embedded-only" so does not have its own datastore table.
17/08/06 18:04:28 INFO DataNucleus.Datastore: The class "org.apache.hadoop.hive.metastore.model.MOrder" is tagged as "embedded-only" so does not have its own datastore table.
17/08/06 18:04:28 INFO DataNucleus.Datastore: The class "org.apache.hadoop.hive.metastore.model.MFieldSchema" is tagged as "embedded-only" so does not have its own datastore table.
17/08/06 18:04:28 INFO DataNucleus.Datastore: The class "org.apache.hadoop.hive.metastore.model.MOrder" is tagged as "embedded-only" so does not have its own datastore table.
17/08/06 18:04:28 INFO DataNucleus.Query: Reading in results for query "org.datanucleus.store.rdbms.query.SQLQuery@0" since the connection used is closing
17/08/06 18:04:29 INFO DataNucleus.Datastore: The class "org.apache.hadoop.hive.metastore.model.MResourceUri" is tagged as "embedded-only" so does not have its own datastore table.
cdrDF:
root
 |-- specificationVersionNumber: integer (nullable = true)
 |-- releaseVersionNumber: integer (nullable = true)
 |-- fileName: string (nullable = true)
 |-- fileAvailableTimeStamp: timestamp (nullable = true)
 |-- fileUtcTimeOffset: integer (nullable = true)
 |-- sender: string (nullable = true)
 |-- recipient: string (nullable = true)
 |-- sequenceNumber: integer (nullable = true)
 |-- callEventsCount: string (nullable = true)
 |-- eventType: string (nullable = true)
 |-- imsi: long (nullable = true)
 |-- imei: long (nullable = true)
 |-- callEventStartTimeStamp: timestamp (nullable = true)
 |-- utcTimeOffset: integer (nullable = true)
 |-- callEventDuration: integer (nullable = true)
 |-- causeForTermination: integer (nullable = true)
 |-- accessPointNameNI: string (nullable = true)
 |-- accessPointNameOI: string (nullable = true)
 |-- dataVolumeIncoming: string (nullable = true)
 |-- dataVolumeOutgoing: string (nullable = true)
 |-- sgsnAddress: string (nullable = true)
 |-- ggsnAddress: string (nullable = true)
 |-- chargingId: string (nullable = true)
 |-- chargeAmount: integer (nullable = true)
 |-- teleServiceCode: integer (nullable = true)
 |-- bearerServiceCode: string (nullable = true)
 |-- supplementaryServiceCode: string (nullable = true)
 |-- dialledDigits: string (nullable = true)
 |-- connectedNumber: string (nullable = true)
 |-- thirdPartyNumber: string (nullable = true)
 |-- callingNumber: long (nullable = true)
 |-- recEntityId: long (nullable = true)
 |-- callReference: string (nullable = true)
 |-- locationArea: string (nullable = true)
 |-- cellId: string (nullable = true)
 |-- msisdn: string (nullable = true)
 |-- servingNetwork: string (nullable = true)

+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|imsi|imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
		  +--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
		  |                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:55|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
		  |                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:10|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
		  |                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:14|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
		  |                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:39|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
		  |                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:46|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
		  |                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:51|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
		  |                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:05:08|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
		  +--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
		  only showing top 7 rows

+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|imsi|imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+

+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|imsi|imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+

+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|imsi|imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+

dfFilteredBySQL:
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|imsi|imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+----+----+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+

+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|           imsi|           imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:01:54|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:09|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:19|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:24|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:28|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:51|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:55|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:10|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:14|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:39|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
only showing top 10 rows

+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|specificationVersionNumber|releaseVersionNumber|fileName|fileAvailableTimeStamp|fileUtcTimeOffset|sender|recipient|sequenceNumber|callEventsCount|eventType|           imsi|           imei|callEventStartTimeStamp|utcTimeOffset|callEventDuration|causeForTermination|accessPointNameNI|accessPointNameOI|dataVolumeIncoming|dataVolumeOutgoing|sgsnAddress|ggsnAddress|chargingId|chargeAmount|teleServiceCode|bearerServiceCode|supplementaryServiceCode|dialledDigits|connectedNumber|thirdPartyNumber|callingNumber|recEntityId|callReference|locationArea|cellId|msisdn|servingNetwork|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:01:54|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:09|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:19|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:24|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:28|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:51|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:02:55|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:10|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:14|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
|                         2|                   1|    null|   2017-04-26 14:11:29|             -400| FRAKS|    ITAUT|        304561|           null|      mtc|250209890003854|355587045959660|    2017-04-26 21:04:39|          300|                0|                  0|             null|             null|              null|              null|       null|       null|      null|           0|             21|             null|                    null|         null|           null|            null|  39043490004|33672054372|         null|        null|  null|  null|          null|
+--------------------------+--------------------+--------+----------------------+-----------------+------+---------+--------------+---------------+---------+---------------+---------------+-----------------------+-------------+-----------------+-------------------+-----------------+-----------------+------------------+------------------+-----------+-----------+----------+------------+---------------+-----------------+------------------------+-------------+---------------+----------------+-------------+-----------+-------------+------------+------+------+--------------+
only showing top 10 rows

copyOfCDRDF:
+-----------+-------------+
|     number|callingNumber|
+-----------+-------------+
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
+-----------+-------------+

newCDRDF:
root
 |-- number: string (nullable = true)
  |-- callingNumber: string (nullable = true)

+-----------+-------------+
|     number|callingNumber|
+-----------+-------------+
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
|33672054372|  39043490004|
+-----------+-------------+

[success] Total time: 17 s, completed Aug 6, 2017 6:04:35 PM
```

# Launching in local stand-alone Spark
## Copy the data onto HDFS
```bash
$ export HDFS_URL=hdfs://127.0.0.1:9000
$ alias hdfsfs='hdfs dfs -Dfs.defaultFS=$HDFS_URL'
$ export HDFS_USR_DIR=/user/<user>
$ hdfsfs -mkdir -p $HDFS_USR_DIR/data/cdr
$ hdfsfs -put data/cdr/CDR-sample.csv $HDFS_USR_DIR/data/cdr
$ hdfsfs -cat $HDFS_USR_DIR/data/cdr/CDR-sample.csv|head -3
```

## Spark
```bash
$ export MVN_CHD_REPO=~/.m2/repository
$ $SPARK_HOME/bin/spark-submit \
  --class org.bom4v.ti.Demonstrator \
  --master local[4] --deploy-mode client \
  --queue default \
  target/scala-2.11/ti-spark-examples_2.11-0.0.1.jar
```

# Launching in Yarn client mode

```bash
$ $SPARK_HOME/bin/spark-submit \
  --class org.bom4v.ti.Demonstrator \
  --master yarn --deploy-mode client \
  --queue default \
  target/scala-2.11/ti-spark-examples_2.11-0.0.1.jar
```

# Launching in Yarn server mode
If the jobs are to be launched from a remote machine, you may want to map the local HDFS port
to the HDFS port of the remote machine. For instance, from an independent terminal window
on the local machine:
```bash
$ The -N option allows to not launch any command (eg, bash)
$ ssh <user>@<remote-machine> -N -L 9000:127.0.0.1:9000
```

Then, the following commands will work:
* remotely if the above SSH port forwarding has been set up
* locally if the above SSH port forwarding has not been set up
```bash
$ export HDFS_URL=hdfs://127.0.0.1:9000
$ alias hdfsfs='hdfs dfs -Dfs.defaultFS=$HDFS_URL'
$ export ATF_USR_DIR=/user/darnaud/artefacts
$ export ATF_USR_URL=$HDFS_URL$ATF_USR_DIR
$ hdfsfs -mkdir -p $ATF_USR_DIR
$ hdfsfs -put -f target/scala-2.11/ti-spark-examples_2.11-0.0.1.jar $ATF_USR_DIR
```

```bash
$ $SPARK_HOME/bin/spark-submit \
  --class org.bom4v.ti.Demonstrator \
  --master yarn --deploy-mode cluster \
  --queue default \
  --jars file:$MVN_CHD_REPO/com/databricks/spark-csv_2.11/1.5.0/spark-csv_2.10-1.5.0.jar,\
         file:$MVN_CHD_REPO/org/apache/commons/commons-csv/1.1/commons-csv-1.1.jar \
  target/scala-2.11/ti-spark-examples_2.11-0.0.1.jar
```

