package com.oracle.demo.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val conf = new SparkConf().setAppName("WordCountJobAppScala").setMaster("local[*]");
  val sc = new SparkContext(conf)

  val initialRDD = sc.textFile("hdfs://localhost:9000/graalvm-demos/large.txt")
  val rdd = initialRDD.flatMap(v => v.split(" ")).
    filter(v => v.length > 2).
    map(w => (w, 1)).
    reduceByKey((v1, v2) => v1 + v2).
    map(u => (u._2, u._1)).
    sortByKey(false);

   val topTenRDD = rdd.take(10)
  println("Running with Java:" + System.getProperty("java.home"))
  topTenRDD.foreach(System.out.println)

}
