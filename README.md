# Using_Spark_Jars_CDE

* Jars cannot be added from the pyspark or scala code while creating the SparkSession i.e. SparkSession.builder.config('spark.jars', '<comma separated list of jars>') will not work. This is because the jar needs to be added to spark's classpath before the JVM is started. If you do it in the code, then the JVM has already started; thus, we cannot append jars to the classpath.

* The jars should be specified using --jars arg during spark-submit and in CDE's case the jars should be added to the CDE job definition. When the jar is uploaded as a resource the official way to specify it is:

```
spark.jars=/app/mount/<resource name>/<name of the uploaded jar>.jar
spark.driver.extraClassPath=/app/mount/<resource name>/<name of the uploaded jar>.jar
```
