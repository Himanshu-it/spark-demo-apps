package com.hmdemo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCounterApp implements SparkApp {
    private SparkConf sparkConf;
    private JavaSparkContext sparkContext;
    private String path = "../sparkapps/main/java/resources/text-file.txt";

    public WordCounterApp() {
        sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter App");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @Override
    public void process() {
        JavaRDD<String> content  = sparkContext.textFile(path);
        JavaRDD<String> words = content.flatMap(str -> Arrays.asList(str.split(" ")));
        JavaPairRDD<String, Integer> wordCountPair = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);
        wordCountPair.saveAsTextFile("../sparkapps/main/java/resources/word-count-result");
    }

}
