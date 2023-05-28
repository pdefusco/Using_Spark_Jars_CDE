#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
import configparser
import os
from sparkmeasure import StageMetrics

## CDE PROPERTIES
config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

CDE_RESOURCE_NAME = "SPARKGEN_FILES"

spark = SparkSession.builder.appName('ENRICH')\
            .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
            .config("spark.sql.catalog.spark_catalog.type", "hive")\
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
            .config("spark.jars","/app/mount/{}/spark-measure_2.13-0.23.jar".format(CDE_RESOURCE_NAME))\
            .config("spark.driver.extraClassPath","/app/mount/{}/spark-measure_2.13-0.23.jar".format(CDE_RESOURCE_NAME))\
            .getOrCreate()

print("PYSPARK HOME: ")
print(os.environ["SPARK_HOME"])

print("LIST PYSPARK HOME CONTENTS")
print(os.listdir(os.environ["SPARK_HOME"]+"/jars"))

print("ALL SPARK CONFIGS IN CDE: ")
print(spark.sparkContext.getConf().getAll())

print("PIP3 FREEZE")
os.system("pip3 freeze")

print("LIST PROPERTIES")
print(os.system("ls /opt/spark/conf/spark.properties"))

#os.system("cp /app/mount/{0}/spark-measure_2.13-0.23.jar {1}".format(CDE_RESOURCE_NAME, "/opt/spark/jars"))

#print("LIST PYSPARK HOME CONTENTS")
#print(os.listdir(os.environ["SPARK_HOME"]+"/jars"))

#os.system("pip3 install pyarrow")
#os.system("pip3 install sparkmeasure")

stagemetrics = StageMetrics(spark)


print("FIND SPARK")
import findspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# Find Spark Locally
location = findspark.find()
print(location)
print(findspark.init(location, edit_rc=True))
