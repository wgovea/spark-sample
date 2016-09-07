# spark-sample

To run this application

1.  Create the hive table using the DDL provided ( people.hsql)
2.  hdfs dfs -put people.json.gz /path/in/hdfs
3.  submit spark job
        /usr/hdp/2.3.2.0-2950/spark/bin/spark-submit --master local[8] jsonToHiveOrc.py 
