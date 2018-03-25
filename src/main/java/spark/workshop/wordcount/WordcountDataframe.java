package spark.workshop.wordcount;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import spark.workshop.util.SparkHelper;
import spark.workshop.util.Util;

import static org.apache.spark.sql.functions.count;

public class WordcountDataframe {
    public static void main(String[] args) {
        SparkHelper spark = new SparkHelper();
        Dataset<String> dataset = spark.readText("data/plain-text.txt");

        Dataset<Row> outputDataset = dataset.flatMap((FlatMapFunction<String, String>) Util::tokenize, Encoders.STRING())
            .groupBy("value")
            .agg(count("value"));

        outputDataset.show();
    }
}
