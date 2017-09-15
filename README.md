# Accessing GCP Storage Products in Spark

This repository is for examples showing how to access Google Cloud Platform storage products such as [Google Cloud Storage](https://cloud.google.com/storage/) (GCS) and [BigQuery](https://cloud.google.com/bigquery/) as data sources or sinks in [Spark](https://github.com/apache-spark-on-k8s/spark) applications.

The best way of accessing GCS and BigQuery in a Spark application is to use the official [BigQuery](https://cloud.google.com/dataproc/docs/connectors/bigquery) and [GCS](https://cloud.google.com/dataproc/docs/connectors/cloud-storage) connectors that offer both Apache Hadoop and Apache Spark direct access to data on GCS and BigQuery.

Accessing GCS or BigQuery from a Spark application using the BigQuery and GCS connectors requires a GCP service account Json key file for authenticating with the GCS or BigQuery service. The service account Json key file must be placed in the driver and/or executor containers and accessible to the connector. This requires injecting the service account Json key file into the driver and/or containers. Please refer to [Authenticating to Cloud Platform with Service Accounts](https://cloud-dot-devsite.googleplex.com/container-engine/docs/tutorials/authenticating-to-cloud-platform) for detailed information on how to get a service account Json key file and how to create a secret out of it. The service account must have the appropriate roles and permissions to read and write GCS buckets and objects and to create, read, and write BigQuery datasets and tables. As the example commands above show, users can request the secret to be mounted into the driver and executor containers using the following Spark configuration properties:

```
--conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
--conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
``` 

The BigQuery and GCS connectors are special in how they use the service account Json key file to authenticate with the BigQuery and GCS services. Specifically, the following two Spark configuration properties must be set:

```
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<Path to GCS service account Json key file>
``` 

Both connectors also require that the user specifies a GCP project ID for billing and a GCS bucket name for temporary input from data exported from the input BigQuery table and output, which can be done using the following Spark configuration properties.

```
--conf spark.hadoop.fs.gs.project.id=<GCP project ID>
--conf spark.hadoop.fs.gs.system.bucket=<Root GCS bucket to use for temporary working and output directories>
```
