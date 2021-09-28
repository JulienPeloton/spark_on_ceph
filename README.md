# Spark on CEPH

First copy the runner, and fill the missing parameters according to your setup:

```bash
cp run_read.template.sh run_read.sh
```

Specify the URL of your master:

```bash
--master
```

You will have to specify the Mesos parameters:

```bash
--conf spark.mesos.principal=
--conf spark.mesos.secret=
--conf spark.mesos.role=
```

and some Hadoop properties to access S3:

```bash
--conf spark.hadoop.fs.s3a.endpoint=
--conf spark.hadoop.fs.s3a.access.key=
--conf spark.hadoop.fs.s3a.secret.key=
```

and do not forget to specify your `HOME` directory (Mesos does not know it):

```bash
--conf spark.executorEnv.HOME=
```

Finally, launch a job using:

```bash
# reading file on s3
./run.sh read s3a://SPARKJULIEN/xyz_v1.1.4_mass_mock_native.parquet

# writing file on s3
./run.sh write s3a://SPARKJULIEN/xyz_v1.1.4_mass_mock_native.parquet

# reading file on hdfs
./run.sh read hdfs:///user/julien.peloton/xyz_v1.1.4_mass_mock_native.parquet

# writing file on hdfs
./run.sh write hdfs:///user/julien.peloton/xyz_v1.1.4_mass_mock_native.parquet
```
