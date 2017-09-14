# Spark Word Count using BigQuery and Google Cloud Storage

This package contains a Spark word count example that reads input data from a BigQuery table such as `publicdata:samples.shakespeare` and writes its output to both a GCS bucket and a BigQuery table. The example requires a GCP service account with the IAM roles of.

## Build

To build the example, run the following command:

```
mvn clean package
```

This will create a single jar named `bigquery-wordcount-<version>-jar-with-dependencies.jar` with the necessary dependencies under `target/`. This is the jar to be used as the `<application-jar>` in `spark-submit`. The example takes the full-qualified input BigQuery table ID as the only input argument.

## Run

There are two ways of running this example on [Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark), depending on how the example jar is shipped.

### Staging The Example Jar using the Resource Staging Server

[Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark) ships with a [Resource Staging Server](https://apache-spark-on-k8s.github.io/userdocs/running-on-kubernetes.html#dependency-management) that can be used to stage resources such as jars and files local to the submission machine. The Spark submission client uploads the resources to the Resource Staging Server, from where they are downloaded by the init-container into the Spark driver and executor Pods so they can be used by the driver and executors. To use it, the Resource Staging Server needs to be deployed to the Kubernetes cluster and the Spark configuration property `spark.kubernetes.resourceStagingServer.uri` needs to be set accordingly. Please refer to the [documentation](https://apache-spark-on-k8s.github.io/userdocs/running-on-kubernetes.html#dependency-management) for more details on how to deploy and use the Resource Staging Server. An example `spark-submit` command when using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.bigquery.example.wordcount.BigQueryWordCount \
  --conf spark.executor.instances=1 \
  --conf spark.app.name=bigquery-wordcount \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.initcontainer.docker.image=<init-container image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.hadoop.fs.gs.project.id=<GCP project ID> \
  --conf spark.hadoop.fs.gs.system.bucket=<Root GCS bucket to use for temporary working and output directories>  \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<Path to GCS service account Json key file> \
  --conf spark.kubernetes.resourceStagingServer.uri=<resource staging server URI> \ 
  local:///opt/spark/examples/jars/bigquery-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar publicdata:samples.shakespeare
```

### Putting The Example Jar into Custom Spark Driver and Executor Images

For users who prefer not using the Resource Staging Server, an alternative way is to put the example jar into custom built Spark driver and executor Docker images. Typically the jar gets copy into the `examples/jars` directory of a unzipped Spark distribution, from where the Docker images are to be built. The entire `examples/jars` directory get copied into the driver and executor images. When using this option, the `<application-jar>` is in the form of `local:///opt/spark/examples/jars/bigquery-wordcount-<version>-jar-with-dependencies.jar`, where the `local://` scheme is needed and it means the jar is locally in the driver and executor containers. An example `spark-submit` command when using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.bigquery.example.wordcount.BigQueryWordCount \
  --conf spark.executor.instances=1 \
  --conf spark.app.name=bigquery-wordcount \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.initcontainer.docker.image=<init-container image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.hadoop.fs.gs.project.id=<GCP project ID> \
  --conf spark.hadoop.fs.gs.system.bucket=<Root GCS bucket to use for temporary working and output directories>  \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<Path to GCS service account Json key file> \ local:///opt/spark/examples/jars/bigquery-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar publicdata:samples.shakespeare
```    

## Trouble Shooting
