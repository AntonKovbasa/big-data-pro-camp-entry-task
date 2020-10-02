package gl.pro.camp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class MetricsCalculator {

    private final static String INPUT_FILE_PATH = "src/main/resources/data/test-task_dataset_summer_products.csv";
    private final static String OUTPUT_PATH = "src/main/resources/";

    private final static String PRICE = "price";
    private final static String ORIGIN_COUNTRY = "origin_country";
    private final static String RATING_COUNT = "rating_count";
    private final static String RATING_FIVE_COUNT = "rating_five_count";

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("MetricsCalculator")
                .getOrCreate();

        Dataset<Row> inputDataset = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(INPUT_FILE_PATH);

        Dataset<Row> castedFields = inputDataset.withColumn(PRICE, col(PRICE).cast(DataTypes.DoubleType)).
                withColumn(RATING_COUNT, col(RATING_COUNT).cast(DataTypes.DoubleType)).
                withColumn(RATING_FIVE_COUNT, col(RATING_FIVE_COUNT).cast(DataTypes.DoubleType));

        Dataset<Row> dirtyData = castedFields.filter(col(PRICE).isNull()
                .or(col(ORIGIN_COUNTRY).isNull())
                .or(col(RATING_FIVE_COUNT).isNull())
                .or(col(RATING_COUNT).isNull())
                .or(col(RATING_COUNT).$less(col(RATING_FIVE_COUNT))
                )
        );

        Dataset<Row> cleanData = castedFields.filter(col(PRICE).isNotNull()
                .and(col(ORIGIN_COUNTRY).isNotNull())
                .and(col(RATING_FIVE_COUNT).isNotNull())
                .and(col(RATING_COUNT).isNotNull())
                .and(col(RATING_FIVE_COUNT).$less(col(RATING_COUNT))
                )
        );

        Dataset<Row> result = cleanData.groupBy(ORIGIN_COUNTRY)
                .agg(avg(PRICE).as("average_price_of_products"),
                        sum(RATING_FIVE_COUNT)
                                .divide(sum(RATING_COUNT))
                                .multiply(100)
                                .as("share_of_five_star_products"))
                .orderBy(ORIGIN_COUNTRY);

        result.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).csv(OUTPUT_PATH + "result");
        dirtyData.write().mode(SaveMode.Overwrite).option("header", true).csv(OUTPUT_PATH + "dirty_data");


    }

}
