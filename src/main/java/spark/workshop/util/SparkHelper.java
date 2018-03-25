package spark.workshop.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHelper {
    public final SparkSession session;
    public final SparkContext context;

    public SparkHelper() {
        SparkConf conf = new SparkConf()
            .setAppName("spark-workshop")
            .setMaster("local[4]");
        this.session = SparkSession.builder().config(conf).getOrCreate();
        this.context = this.session.sparkContext();
    }

    public Dataset<String> readText(String path) {
        return session.read().textFile(path);
    }

    public Dataset<Row> readJson(String path) {
        return session.read().json(path);
    }
}
