#
# GWCDR Process into Hive
#

from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import *
from pyspark.sql.types import *


if __name__ == "__main__":
    sc = SparkContext(appName="PythonSQL")
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition","true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext.setConf("spark.sql.orc.filterPushdown", "true")

    ## Create a DataFrame from the file(s) pointed to by path
    #gwcdr =hiveContext.read.json("/user/wgovea/GWCDR_RAW_DB_EVENTS_2016_08_31_08.gz")
    gwcdr =hiveContext.read.json("/user/wgovea/people.json.gz")


    # The inferred schema can be visualized using the printSchema() method.
    gwcdr.printSchema()

    # Register this DataFrame as a table.
    gwcdr.registerTempTable("gwcdr_tmp")

    data = hiveContext.sql("SELECT name,age,country,ts from gwcdr_tmp").write.format("orc").partitionBy("ts").mode("append").insertInto("people")

    sc.stop()
