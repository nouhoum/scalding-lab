package com.nouhoum

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SimpleApp extends App {
  val SPARK_HOME = "/Users/nouhoum/tools/spark-0.9.1"
  val logFile = s"$SPARK_HOME/README.md"
  val sc = new SparkContext("local", "Simple App", SPARK_HOME, List("target/scala-2.10/scalding-lab_2.10-1.0.jar"))
  val logData: RDD[String] = sc.textFile(logFile, 2).cache()
  val numAs = logData.filter(_.contains("a")).count()
  val numBs = logData.filter(_.contains("b")).count()

  println(s"Lines with a: $numAs, lines with b: $numBs")
}
