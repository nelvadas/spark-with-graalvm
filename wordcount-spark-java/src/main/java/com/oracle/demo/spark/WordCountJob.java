package com.oracle.demo.spark;


import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.internal.config.Python;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class WordCountJob {


    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("WordCountJobApp");
        //Uncomment if running from IDE
        // conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);


        System.out.println(String.format("Running with Java : %s",System.getProperty("java.home")));



                JavaRDD < String > initialRDD = sc.textFile("hdfs://localhost:9000/graalvm-demos/large.txt");
        JavaPairRDD rdd = initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(w -> w.length() > 2)
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(t -> new Tuple2(t._2, t._1))
                .sortByKey(false);
        List result = rdd.take(10);


        result.forEach(System.out::println);
        /*Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

         */
        sc.close();

    }


}
