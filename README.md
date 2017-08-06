Spark Layer of the BOM for Verticals
====================================

# Build and deployment
See http://github.com/bom4v/metamodels for the way to build, package and deploy
all the Telecoms Intelligence (TI) Models components, including that project.

# Run the demonstrator
```bash
$ mkdir -p ~/dev/ti
$ cd ~/dev/ti
$ git clone https://github.com/bom4v/metamodels.git
$ cd metamodels
$ rake clone && rake checkout
$ rake offline=true deliver
$ cd workspace/src/ti-spark-examples
$ ./mkLocalDir.sh
$ sbt run
[info] Loading global plugins from ~/.sbt/0.13/plugins
[info] Loading project definition from ~/dev/ti/metamodels/workspace/src/ti-spark-examples/project
[info] Set current project to ti-spark-examples (in build file:~/dev/ti/metamodels/workspace/src/ti-spark-examples/)
[info] Compiling 1 Scala source to ~/dev/ti/metamodels/workspace/src/ti-spark-examples/target/scala-2.10/classes...
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


