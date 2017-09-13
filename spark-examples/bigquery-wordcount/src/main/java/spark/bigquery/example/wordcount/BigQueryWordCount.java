package spark.bigquery.example.wordcount;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

/**
 * A simple work count example that both reads its input from and writes its output to BigQuery.
 */
public class BigQueryWordCount {

    private static final String FS_GS_PROJECT_ID = "fs.gs.project.id";
    private static final String FS_GS_SYSTEM_BUCKET = "fs.gs.system.bucket";
    private static final String MAPRED_OUTPUT_FORMAT_CLASS = "mapreduce.job.outputformat.class";

    private static final String OUTPUT_DATASET_ID = "wordcount_dataset";
    private static final String OUTPUT_TABLE_ID = "wordcount_output";
    private static final String OUTPUT_TABLE_SCHEMA =
            "[{'name': 'word', 'type': 'STRING'}, {'name': 'word_count', 'type': 'INTEGER'}]";

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: BigQueryWordCount <input table id>");
            System.exit(1);
        }

        // Note that the default constructor loads settings from system properties (
        // for instance, when launching with ./bin/spark-submit).
        JavaSparkContext javaSparkContext = new JavaSparkContext();
        Configuration conf = javaSparkContext.hadoopConfiguration();

        String projectId = conf.get(FS_GS_PROJECT_ID);
        String systemBucket = conf.get(FS_GS_SYSTEM_BUCKET);
        conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
        conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);

        // Input configuration.
        BigQueryConfiguration.configureBigQueryInput(conf, args[0]);

        String inputTmpDir = String.format("gs://%s/hadoop/tmp/bigquery/wordcount", systemBucket);
        conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, inputTmpDir);

        // Output configuration.
        BigQueryConfiguration.configureBigQueryOutput(
                conf, projectId, OUTPUT_DATASET_ID, OUTPUT_TABLE_ID, OUTPUT_TABLE_SCHEMA);
        conf.set(MAPRED_OUTPUT_FORMAT_CLASS, BigQueryOutputFormat.class.getName());

        JavaPairRDD<LongWritable, JsonObject> tableData = javaSparkContext.newAPIHadoopRDD(
                conf,
                GsonBigQueryInputFormat.class,
                LongWritable.class,
                JsonObject.class).cache();
        JavaPairRDD<String, Long> wordCount = tableData
                .map(entry -> toTuple(entry._2))
                .keyBy(tuple -> tuple._1)
                .mapValues(tuple -> tuple._2)
                .reduceByKey((count1, count2) -> count1 + count2);
        wordCount
                .map(tuple -> Tuple2.apply(null, toJson(tuple)))
                .keyBy(tuple -> tuple._1)
                .mapValues(tuple -> tuple._2)
                .saveAsNewAPIHadoopDataset(conf);

        Path inputTmpDirPath = new Path(inputTmpDir);
        if (!inputTmpDirPath.getFileSystem(conf).delete(inputTmpDirPath, true)) {
            System.err.println("Failed to delete " + inputTmpDirPath);
        }
    }

    private static Tuple2<String, Long> toTuple(JsonObject jsonObject) {
        String word = jsonObject.get("word").getAsString().toLowerCase();
        long count = jsonObject.get("word_count").getAsLong();
        return new Tuple2<>(word, count);
    }

    private static JsonObject toJson(Tuple2<String, Long> tuple) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("word", tuple._1);
        jsonObject.addProperty("word_count", tuple._2);
        return jsonObject;
    }
}
