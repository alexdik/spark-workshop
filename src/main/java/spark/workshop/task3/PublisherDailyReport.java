package spark.workshop.task3;

import org.apache.spark.broadcast.Broadcast;
import static scala.reflect.ClassManifestFactory.fromClass;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import spark.workshop.util.Registry;
import spark.workshop.util.SparkHelper;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.count;

public class PublisherDailyReport {
    public static void main(String[] args) {
        SparkHelper spark = new SparkHelper();
        Broadcast<Registry> registryBroadcast = spark.context.broadcast(new Registry(), fromClass(Registry.class));
        UDF1<Long, String> getAdvertiserName = registryBroadcast.value()::getPublisherName;
        spark.session.udf().register("getAdvName", getAdvertiserName, DataTypes.StringType);

        Dataset<Row> reqDs = spark.readJson("data/ad-request.json");
        Dataset<Row> rspDs = spark.readJson("data/ad-response.json");
        Dataset<Row> impDs = spark.readJson("data/impression.json");

        /*
            TODO: Show daily statistics per Publisher including request/response/impression counters

            Expected output:
            +---------------+----------+--------+---------+-----------+
            |      publisher|       day|requests|responses|impressions|
            +---------------+----------+--------+---------+-----------+
            |    Bertelsmann|2018-03-11|       2|        2|          0|
            |    Bertelsmann|2018-03-12|       1|        1|          0|
            |    Bertelsmann|2018-03-13|       1|        0|          0|
            |  Grupo Planeta|2018-03-11|       3|        3|          2|
            |  Grupo Planeta|2018-03-13|       1|        1|          1|
            | Hachette Livre|2018-03-11|       1|        1|          0|
            | Hachette Livre|2018-03-12|       1|        0|          0|
            | Hachette Livre|2018-03-13|       3|        1|          0|
            |    McGraw-Hill|2018-03-12|       1|        1|          1|
            |    McGraw-Hill|2018-03-13|       1|        1|          0|
            |        Pearson|2018-03-11|       2|        2|          1|
            |        Pearson|2018-03-12|       2|        0|          0|
            |        Pearson|2018-03-13|       2|        1|          0|
            |     RELX Group|2018-03-12|       2|        2|          1|
            |     RELX Group|2018-03-13|       1|        1|          1|
            |Springer Nature|2018-03-12|       2|        2|          1|
            |Springer Nature|2018-03-13|       1|        0|          0|
            | ThomsonReuters|2018-03-11|       6|        4|          4|
            | ThomsonReuters|2018-03-12|       6|        2|          1|
            | ThomsonReuters|2018-03-13|       2|        0|          0|
            |          Wiley|2018-03-12|       3|        2|          2|
            |          Wiley|2018-03-13|       1|        1|          0|
            | Wolters Kluwer|2018-03-11|       3|        0|          0|
            | Wolters Kluwer|2018-03-12|       2|        2|          0|
            +---------------+----------+--------+---------+-----------+

            Hints:
                1) join 3 datasets reqDs, rspDs, impDs via auctionId column
                2) add day column to dataset populated as substring from date column
                3) group dataset by publisherId and day
                4) calculate total count number of records for each dataset
                5) map publisherId to publisher name and sort

         */

        reqDs
            .show(50);
    }
}
