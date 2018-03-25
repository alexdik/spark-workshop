package spark.workshop.wordcount;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import spark.workshop.util.SparkHelper;
import spark.workshop.util.Util;

public class WordcountDataset {
    public static void main(String[] args) {
        SparkHelper spark = new SparkHelper();
        Dataset<String> dataset =  spark.readText("data/plain-text.txt");

        Encoder<String> stringEncoder = Encoders.STRING();
        Encoder<Tuple2<String, Long>> tupleEncoder = Encoders.tuple(Encoders.STRING(), Encoders.LONG());

        Dataset<Tuple2<String, Tuple2<String, Long>>> outputDataset = dataset
            .flatMap((FlatMapFunction<String, String>) Util::tokenize, stringEncoder)
            .map((MapFunction<String, Tuple2<String, Long>>) word -> new Tuple2<>(word, 1L), tupleEncoder)
            .groupByKey((MapFunction<Tuple2<String, Long>, String>) row -> row._1, stringEncoder)
            .reduceGroups((ReduceFunction<Tuple2<String, Long>>) (tuple1, tuple2) -> new Tuple2<>(tuple1._1, tuple1._2 + tuple2._2));

        outputDataset.show();
    }
}
