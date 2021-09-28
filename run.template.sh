#!/bin/bash

# mode read/write
if [[ $1 == 'read' ]]; then
  PYFILE=bench_read.py
elif [[ $1 == 'write' ]]; then
  PYFILE=bench_write.py
else
  echo "You need to specify read or write as argument."
  exit
fi

# filesystem and filename
if [[ $2 == s3a://* ]]; then
  echo 'Running on S3'
elif [[ $2 == hdfs://* ]]; then
  echo 'Running on HDFS'
else
  echo 'The path to file is not valid. It should start with s3a:// or hdfs://'
fi


PACK="org.apache.hadoop:hadoop-aws:2.7.7,com.amazonaws:aws-java-sdk:1.7.4"
spark-submit \
    --master \
    --packages $PACK \
    --driver-memory 4g --executor-memory 3g --conf spark.cores.max=68 --conf spark.executor.cores=2 \
    --conf spark.mesos.principal= \
    --conf spark.mesos.secret= \
    --conf spark.mesos.role= \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.endpoint= \
    --conf spark.hadoop.fs.s3a.access.key= \
    --conf spark.hadoop.fs.s3a.secret.key= \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.fast.upload=true \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.executorEnv.HOME= \
    $PYFILE $2
