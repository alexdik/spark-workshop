package spark.workshop.task1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import spark.workshop.util.SparkHelper;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.desc;

public class CostReport {
    public static void main(String[] args) {
        SparkHelper spark = new SparkHelper();
        Dataset<Row> src = spark.readJson("data/ad-response.json");
        src.show(false);
        src.printSchema();

        /*
            TODO: This report should provide total sum paid by each of 3 advertisers (ids: 8, 9, 10).

            Expected output:
            +------------+----+
            |advertiserId| sum|
            +------------+----+
            |           8|2637|
            |          10|2248|
            |           9| 840|
            +------------+----+

            Hints:
                1) filter dataset for specific advertisers
                2) group dataset by advertiserId column
                3) aggregate total sum
                4) sort by total sum descending

         */

        src
            .show();
    }
}
