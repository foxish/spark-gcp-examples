package spark.bigquery.example.sparksql;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.spotify.spark.bigquery.BigQueryDataFrame;
import com.spotify.spark.bigquery.BigQuerySQLContext;
import com.spotify.spark.bigquery.CreateDisposition;
import com.spotify.spark.bigquery.WriteDisposition;
import java.io.File;
import java.io.IOException;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * This is a simple ETL Spark job that runs a user-given SQL query stored in a file using the Spark
 * SQL API and writes the query result ino a BigQuery table.
 */
public class BigQuerySparkSQL {

  private static final String APPLICATION_CREDENTIALS_ENV = "GOOGLE_APPLICATION_CREDENTIALS";

  public static void main(String[] args) throws IOException {
    if (args.length != 4) {
      System.err.println("Usage: BigQuerySparkSQL <GCP project ID> "
          + "<GCS bucket for temporary data> <BigQuery SQL query file> "
          + "<output BigQuery table ID>");
      System.exit(1);
    }

    BigQuerySQLContext bigQuerySQLContext = createBigQuerySQLContext(args);

    String sqlQueryFilePath = args[2];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(sqlQueryFilePath),
        "Input BigQuery SQL query must not be empty");
    String outputTableId = args[3];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(outputTableId),
        "Output BigQuery table ID must not be empty");
    runQueryAndLoadResult(bigQuerySQLContext, sqlQueryFilePath, outputTableId);
  }

  private static BigQuerySQLContext createBigQuerySQLContext(String[] args) {
    String projectId = args[0];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId),
        "GCP project ID must not be empty");
    String gcsBucket = args[1];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(gcsBucket),
        "GCS bucket must not be empty");

    String serviceAccountJsonKeyFilePath = System.getenv(APPLICATION_CREDENTIALS_ENV);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(serviceAccountJsonKeyFilePath),
        APPLICATION_CREDENTIALS_ENV + " must be set");

    SQLContext sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate());
    BigQuerySQLContext bigQuerySQLContext = new BigQuerySQLContext(sqlContext);
    bigQuerySQLContext.setBigQueryProjectId(projectId);
    bigQuerySQLContext.setBigQueryGcsBucket(gcsBucket);
    bigQuerySQLContext.setGcpJsonKeyFile(serviceAccountJsonKeyFilePath);

    return bigQuerySQLContext;
  }

  private static void runQueryAndLoadResult(BigQuerySQLContext bigQuerySQLContext,
      String sqlQueryFilePath,
      String outputTableId) throws IOException {
    String sqlQuery = Files.asCharSource(new File(sqlQueryFilePath), Charsets.UTF_8).read();
    Dataset<Row> dataset = bigQuerySQLContext.bigQuerySelect(sqlQuery);
    BigQueryDataFrame bigQueryDataFrame = new BigQueryDataFrame(dataset);
    bigQueryDataFrame.saveAsBigQueryTable(outputTableId, CreateDisposition.CREATE_IF_NEEDED(),
        WriteDisposition.WRITE_EMPTY());
  }
}
