package spark.bigquery.example.wordcount;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkContext;
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
    private static final String FS_GS_WORKING_DIR = "fs.gs.working.dir";
    private static final String MAPRED_OUTPUT_FORMAT_CLASS = "mapreduce.job.outputformat.class";

    private static final String OUTPUT_DATASET_ID = "wordcount_dataset";
    private static final String OUTPUT_TABLE_ID = "wordcount_output";
    private static final String WORD_COLUMN = "word";
    private static final String WORD_COUNT_COLUMN = "word_count";
    private static final String OUTPUT_TABLE_SCHEMA =
            "[{'name': 'word', 'type': 'STRING'}, {'name': 'word_count', 'type': 'INTEGER'}]";

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: BigQueryWordCount <input table id>");
            System.exit(1);
        }

        Configuration conf = null;
        Path tmpDirPath = null;
        try (JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate())) {
            conf = configure(javaSparkContext.hadoopConfiguration(), args);
            tmpDirPath = new Path(conf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
            deleteTmpDir(tmpDirPath, conf);
            Path gcsOutputPath =
                    new Path(String.format("gs://%s/hadoop/output/wordcounts", conf.get(FS_GS_SYSTEM_BUCKET)));
            FileSystem gcsFs = gcsOutputPath.getFileSystem(conf);
            if (gcsFs.exists(gcsOutputPath) && !gcsFs.delete(gcsOutputPath, true)) {
                System.err.println("Failed to delete the output directory: " + gcsOutputPath);
            }
            compute(javaSparkContext, conf, gcsOutputPath);
        } finally {
            if (conf != null && tmpDirPath != null) {
                deleteTmpDir(tmpDirPath, conf);
            }
        }
    }

    private static Configuration configure(Configuration conf, String[] args) throws IOException {
        String inputTableId = args[0];
        Preconditions.checkArgument(!Strings.isNullOrEmpty(inputTableId),
                "Input BigQuery table (fully-qualified) ID must not be null or empty");
        String projectId = conf.get(FS_GS_PROJECT_ID);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId),
                "GCP project ID must not be null or empty");
        String systemBucket = conf.get(FS_GS_SYSTEM_BUCKET);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(systemBucket),
                "GCS system bucket must not be null or empty");

        // Basic BigQuery connector configuration.
        conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
        conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
        conf.set(FS_GS_WORKING_DIR, "/");

        // Use service account for authentication. The service account key file is located at the path
        // specified by the configuration property google.cloud.auth.service.account.json.keyfile.
        conf.set(EntriesCredentialConfiguration.BASE_KEY_PREFIX +
                        EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
                "true");

        // Input configuration.
        BigQueryConfiguration.configureBigQueryInput(conf, args[0]);
        String inputTmpDir = String.format("gs://%s/hadoop/tmp/bigquery/wordcount", systemBucket);
        conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, inputTmpDir);

        // Output configuration.
        BigQueryConfiguration.configureBigQueryOutput(
                conf, projectId, OUTPUT_DATASET_ID, OUTPUT_TABLE_ID, OUTPUT_TABLE_SCHEMA);
        conf.set(MAPRED_OUTPUT_FORMAT_CLASS, BigQueryOutputFormat.class.getName());

        return conf;
    }

    private static void compute(JavaSparkContext javaSparkContext, Configuration conf, Path gcsOutputPath) {
        JavaPairRDD<LongWritable, JsonObject> tableData = javaSparkContext.newAPIHadoopRDD(
                conf,
                GsonBigQueryInputFormat.class,
                LongWritable.class,
                JsonObject.class).cache();
        JavaPairRDD<String, Long> wordCounts = tableData
                .map(entry -> toTuple(entry._2))
                .keyBy(tuple -> tuple._1)
                .mapValues(tuple -> tuple._2)
                .reduceByKey((count1, count2) -> count1 + count2);

        // First write to GCS.
        wordCounts.
                mapToPair(tuple -> new Tuple2<>(new Text(tuple._1), new LongWritable(tuple._2))).
                saveAsNewAPIHadoopFile(
                        gcsOutputPath.toString(), Text.class, LongWritable.class, TextOutputFormat.class);

        // Then write to a BigQuery table.
        wordCounts
                .map(tuple -> Tuple2.apply(null, toJson(tuple)))
                .keyBy(tuple -> tuple._1)
                .mapValues(tuple -> tuple._2)
                .saveAsNewAPIHadoopDataset(conf);
    }

    private static void deleteTmpDir(Path tmpDirPath, Configuration conf) throws IOException {
        FileSystem fs = tmpDirPath.getFileSystem(conf);
        if (fs.exists(tmpDirPath) && !fs.delete(tmpDirPath, true)) {
            System.err.println("Failed to delete temporary directory: " + tmpDirPath);
        }
    }

    private static Tuple2<String, Long> toTuple(JsonObject jsonObject) {
        String word = jsonObject.get(WORD_COLUMN).getAsString().toLowerCase();
        long count = jsonObject.get(WORD_COUNT_COLUMN).getAsLong();
        return new Tuple2<>(word, count);
    }

    private static JsonObject toJson(Tuple2<String, Long> tuple) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(WORD_COLUMN, tuple._1);
        jsonObject.addProperty(WORD_COUNT_COLUMN, tuple._2);
        return jsonObject;
    }
}
