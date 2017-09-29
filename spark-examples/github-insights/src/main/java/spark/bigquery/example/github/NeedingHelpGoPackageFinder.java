package spark.bigquery.example.github;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.spotify.spark.bigquery.BigQueryDataFrame;
import com.spotify.spark.bigquery.BigQuerySQLContext;
import com.spotify.spark.bigquery.CreateDisposition;
import com.spotify.spark.bigquery.WriteDisposition;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * This is an example Spark pipeline that finds popular Golang projects that need help from
 * contributors for bug fixes, feature implementations, or documentation improvements, etc.
 */
public class NeedingHelpGoPackageFinder {

  private static final String APPLICATION_CREDENTIALS_ENV = "GOOGLE_APPLICATION_CREDENTIALS";

  private static final String GO_FILES_QUERY =
      "SELECT * "
          + "FROM [bigquery-public-data:github_repos.sample_files] "
          + "WHERE RIGHT(path, 3) = '.go'";

  private static final String GO_FILES_TABLE = "go_files";

  private static final String GO_CONTENTS_QUERY_TEMPLATE =
      "SELECT * "
          + "FROM [bigquery-public-data:github_repos.sample_contents] "
          + "WHERE id IN (SELECT id FROM %s.%s)";

  private static final String GO_CONTENTS_TABLE = "go_contents";

  private static final String GO_PACKAGE_KEYWORD = "package";

  private static final Pattern GO_SINGLE_IMPORT_PATTERN = Pattern
      .compile("(?s).*import\\s*\"(.*)\".*");

  private static final Pattern GO_BLOCK_IMPORT_PATTERN = Pattern
      .compile("(?s).*import \\(([^)])\\).*");

  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: NeedingHelpGoPackageFinder <GCP project ID> "
          + "<BigQuery dataset> <GCS bucket for temporary data>");
      System.exit(1);
    }

    String projectId = args[0];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId),
        "GCP project ID must not be empty");
    String bigQueryDataset = args[1];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(bigQueryDataset),
        "BigQuery dataset name must not be empty");
    String gcsBucket = args[2];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(gcsBucket),
        "GCS bucket must not be empty");

    BigQuerySQLContext bigQuerySQLContext = createBigQuerySQLContext(projectId, gcsBucket);
    String goFilesTableId = String.format("%s:%s.%s", projectId, bigQueryDataset, GO_FILES_TABLE);
    queryAndOutputGoFilesTable(bigQuerySQLContext, goFilesTableId);
    String goContentsTableId = String
        .format("%s:%s.%s", projectId, bigQueryDataset, GO_CONTENTS_TABLE);
    queryAndOutputGoContentsTable(bigQuerySQLContext, bigQueryDataset, goContentsTableId);

    Dataset<Row> goContentsDataset = loadGoContentsTable(bigQuerySQLContext, goContentsTableId);
    JavaPairRDD<Tuple2<String, String>, Integer> packagesByRepoNames =
        getNeedHelpPackagesByRepoNames(goContentsDataset);
    packagesByRepoNames
        .take(10)
        .forEach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));

    getImportedPackages(goContentsDataset)
        .take(10)
        .forEach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));
  }

  private static BigQuerySQLContext createBigQuerySQLContext(String projectId, String gcsBucket) {
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

  private static void queryAndOutputGoFilesTable(BigQuerySQLContext bigQuerySQLContext,
      String goFilesTableId) {
    Dataset<Row> dataset = bigQuerySQLContext.bigQuerySelect(GO_FILES_QUERY);
    BigQueryDataFrame bigQueryDataFrame = new BigQueryDataFrame(dataset);
    bigQueryDataFrame.saveAsBigQueryTable(goFilesTableId, CreateDisposition.CREATE_IF_NEEDED(),
        WriteDisposition.WRITE_EMPTY());
  }

  private static void queryAndOutputGoContentsTable(BigQuerySQLContext bigQuerySQLContext,
      String bigQueryDataset, String goContentsTableId) {
    Dataset<Row> dataset = bigQuerySQLContext
        .bigQuerySelect(String.format(GO_CONTENTS_QUERY_TEMPLATE, bigQueryDataset, GO_FILES_TABLE));
    BigQueryDataFrame bigQueryDataFrame = new BigQueryDataFrame(dataset);
    bigQueryDataFrame.saveAsBigQueryTable(goContentsTableId, CreateDisposition.CREATE_IF_NEEDED(),
        WriteDisposition.WRITE_EMPTY());
  }

  private static Dataset<Row> loadGoContentsTable(BigQuerySQLContext bigQuerySQLContext,
      String goContentsTableId) {
    return bigQuerySQLContext.bigQueryTable(goContentsTableId)
        .persist(StorageLevel.MEMORY_AND_DISK());
  }

  private static JavaPairRDD<String, String> getContentsByRepoNames(
      Dataset<Row> goContentsDataset) {
    return goContentsDataset.select("sample_repo_name", "content").toJavaRDD()
        .mapToPair(row -> new Tuple2<>(row.getString(0), row.getString(1)))
        .filter(tuple -> tuple._2() != null);
  }

  /**
   * This function takes the sample contents dataset and produces a JavaPairRDD with keys in the
   * form of (repo_name, package) and values being a count a particular repo_name/package
   * combination needs help. The result is sorted in descending order by the counts.
   */
  private static JavaPairRDD<Tuple2<String, String>, Integer> getNeedHelpPackagesByRepoNames(
      Dataset<Row> goContentsDataset) {
    return getContentsByRepoNames(goContentsDataset)
        .filter(tuple -> tuple._2().contains("TODO") || tuple._2().contains("FIXME"))
        .flatMapValues(content -> Splitter.on('\n').split(content))
        .filter(tuple -> tuple._2().startsWith(GO_PACKAGE_KEYWORD))
        .mapValues(line -> line.substring(GO_PACKAGE_KEYWORD.length() + 1).trim())
        .mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._1(), tuple._2()), 1))
        .reduceByKey((left, right) -> left + right)
        .mapToPair(Tuple2::swap)
        .sortByKey(false)
        .mapToPair(Tuple2::swap);
  }

  private static JavaPairRDD<String, String> getImportedPackages(Dataset<Row> goContentsDataset) {
    return getContentsByRepoNames(goContentsDataset)
        .flatMapValues(content -> {
          ImmutableList.Builder<String> builder = ImmutableList.builder();
          Matcher matcher = GO_BLOCK_IMPORT_PATTERN.matcher(content);
          if (matcher.matches()) {
            builder.add(matcher.group(1));
          }
          matcher = GO_SINGLE_IMPORT_PATTERN.matcher(content);
          if (matcher.matches()) {
            builder.add(matcher.group(1));
          }
          return builder.build();
        })
        .flatMapValues(imports -> Splitter.on('\n').split(imports))
        .filter(tuple -> !tuple._2().isEmpty());
  }
}
