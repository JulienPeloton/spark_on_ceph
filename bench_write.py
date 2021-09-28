from pyspark.sql import SparkSession
import time
import sys
import pandas as pd

def quiet_logs(sc, log_level="ERROR"):
    """ Set the level of log in Apache Spark.

    Parameters
    ----------
    sc : SparkContext
        The SparkContext for the session
    log_level : String [optional]
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.

    Examples
    ----------
    Display only ERROR messages (ignore INFO, WARN, etc.)
    >>> quiet_logs(spark.sparkContext, "ERROR")
    """
    ## Get the logger
    logger = sc._jvm.org.apache.log4j

    ## Set the level
    level = getattr(logger.Level, log_level, "INFO")

    logger.LogManager.getLogger("org"). setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)

def main():
    # Grab the running Spark Session,
    # otherwise create it.
    spark = SparkSession \
        .builder \
        .appName("Spark on CEPH -- write") \
        .getOrCreate()

    sc = spark.sparkContext
    quiet_logs(sc)

    filename = sys.argv[1]
    filename_template = filename.split('.parquet')[0] + '{}.parquet'

    nloop = 5

    for count in range(nloop):
        df = spark.read.format('parquet').load(filename)

        start = time.time()

        df.write.parquet(filename_template.format(count))
        elapsed = time.time() - start
        print("{} seconds".format(elapsed))


if __name__ == "__main__":
    """ Execute the test suite """
    main()
