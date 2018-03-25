package spark.workshop.wordcount;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import spark.workshop.util.SparkHelper;
import spark.workshop.util.Util;

public class WordcountRdd {
    public static void main(String[] args) {
        SparkHelper spark = new SparkHelper();
        JavaRDD<String> rdd = spark.readText("data/plain-text.txt").javaRDD();

        JavaPairRDD<String, Integer> outputRdd = rdd
            .flatMap(Util::tokenize)
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((count1, count2) -> count1 + count2);

        outputRdd.collect().forEach(System.out::println);
    }
}
