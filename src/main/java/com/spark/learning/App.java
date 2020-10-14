package com.spark.learning;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class App {

 public static final String DELIMITER = " ";
 public static void main(String... args) {
  try {
   if (args.length < 1)
    throw new Exception();
  
   SparkConf conf = new SparkConf()
    .setAppName("JAVA_SPARK_WORD_COUNT");

   JavaSparkContext ctx = new JavaSparkContext(conf);
   JavaRDD<String> lines = ctx.textFile(args[0], 1);
   JavaRDD<String> strings = lines.flatMap(s -> Arrays.asList(s.split(DELIMITER)).iterator());
   JavaPairRDD<String, Integer> items = strings.mapToPair(word -> new Tuple2<>(word, 1));
   JavaPairRDD<String, Integer> counts = items.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

   for (Tuple2<?, ?> tuple: counts.collect())
    System.out.println(tuple._1() + " : " + tuple._2());

   ctx.close();
  } catch (Exception exc) {
   System.exit(1);
  }
 }
}
