# Accessing GCP Storage Products in Spark

This repository is for examples showing how to access Google Cloud Platform storage products such as [Google Cloud Storage](https://cloud.google.com/storage/) (GCS) and [BigQuery](https://cloud.google.com/bigquery/) as data sources or sinks in [Spark](https://github.com/apache-spark-on-k8s/spark) applications.

## Using the GCS/BigQuery Connector

The best way of accessing GCS and BigQuery in a Spark application is to use the official [GCS/BigQuery connector](https://cloud.google.com/dataproc/docs/connectors/cloud-storage) that offers both Apache Hadoop and Apache Spark direct access to data on GCS and BigQuery.

## GCP Service Account

Accessing GCS or BigQuery from a Spark application using the GCS/BigQuery connector requires a GCP service account Json key file for authenticating with the GCS or BigQuery service. The service account Json key file must be placed in the driver and/or executor containers and accessible to the connector. This requires injecting the service account Json key file into the driver and/or containers. The best way to do this is to create a Kubernetes secret from the Json key file and mount it into the driver and/or executor containers using the following configuration properties in the `spark-submit` script:

```
--conf spark.kubernetes.driver.secrets.<service account secret name>=<mount path of service account secret>
--conf spark.kubernetes.executor.secrets.<service account secret name>=<mount path of service account secret>
```   

