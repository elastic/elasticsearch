---
navigation_title: "Google Cloud Storage"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-google-cloud.html
---

# Google Cloud Storage Connector [es-connectors-google-cloud]


The *Elastic Google Cloud Storage connector* is a [connector](/reference/search-connectors/index.md) for [Google Cloud Storage](https://cloud.google.com/storage) data sources.

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-google-cloud-connector-client-reference]

### Availability and prerequisites [es-connectors-google-cloud-client-availability-prerequisites]

This connector is available as a self-managed connector. This self-managed connector is compatible with Elastic versions **8.6.0+**. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Usage [es-connectors-google-cloud-client-usage]

The Google Cloud Storage service account must have (at least) the following scopes and roles:

* `resourcemanager.projects.get`
* `serviceusage.services.use`
* `storage.buckets.list`
* `storage.objects.list`
* `storage.objects.get`

Google Cloud Storage service account credentials are stored in a JSON file.


### Configuration [es-connectors-google-cloud-client-configuration]




The following configuration fields are required to set up the connector:

`buckets`
:   List of buckets to index. `*` will index all buckets.

`service_account_credentials`
:   The service account credentials generated from Google Cloud Storage (JSON string). Refer to the [Google Cloud documentation](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) for more information.

`retry_count`
:   The number of retry attempts after a failed call to Google Cloud Storage. Default value is `3`.


### Deployment using Docker [es-connectors-google-cloud-client-docker]

You can deploy the Google Cloud Storage connector as a self-managed connector using Docker. Follow these instructions.

::::{dropdown} Step 1: Download sample configuration file
Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example --output ~/connectors-config/config.yml
```
% NOTCONSOLE

Remember to update the `--output` argument value if your directory name is different, or you want to use a different config file name.

::::


::::{dropdown} Step 2: Update the configuration file for your self-managed connector
Update the configuration file with the following settings to match your environment:

* `elasticsearch.host`
* `elasticsearch.api_key`
* `connectors`

If you’re running the connector service against a Dockerized version of Elasticsearch and Kibana, your config file will look like this:

```yaml
# When connecting to your cloud deployment you should edit the host value
elasticsearch.host: http://host.docker.internal:9200
elasticsearch.api_key: <ELASTICSEARCH_API_KEY>

connectors:
  -
    connector_id: <CONNECTOR_ID_FROM_KIBANA>
    service_type: google_cloud_storage
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

Using the `elasticsearch.api_key` is the recommended authentication method. However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

Note: You can change other default configurations by simply uncommenting specific settings in the configuration file and modifying their values.

::::


::::{dropdown} Step 3: Run the Docker image
Run the Docker image with the Connector Service using the following command:

```sh
docker run \
-v ~/connectors-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors:9.0.0 \
/app/bin/elastic-ingest \
-c /config/config.yml
```

::::


Refer to [`DOCKER.md`](https://github.com/elastic/connectors/tree/main/docs/DOCKER.md) in the `elastic/connectors` repo for more details.

Find all available Docker images in the [official registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).

::::{tip}
We also have a quickstart self-managed option using Docker Compose, so you can spin up all required services at once: Elasticsearch, Kibana, and the connectors service. Refer to this [README](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.

::::



### Documents and syncs [es-connectors-google-cloud-client-documents-syncs]

The connector will fetch all buckets and paths the service account has access to.

The `Owner` field is not fetched as `read_only` scope doesn’t allow the connector to fetch IAM information.

::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permission are not synced. All documents indexed to an Elastic deployment will be visible to all users with access to that Elastic Deployment.

::::



#### Sync types [es-connectors-google-cloud-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-google-cloud-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently filtering is controlled by ingest pipelines.


### Content extraction [es-connectors-google-cloud-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### End-to-end testing [es-connectors-google-cloud-client-client-operations-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Google Cloud Storage connector, run the following command:

```shell
$ make ftest NAME=google_cloud_storage
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=google_cloud_storage DATA_SIZE=small
```


### Known issues [es-connectors-google-cloud-client-known-issues]

There are currently no known issues for this connector.


### Troubleshooting [es-connectors-google-cloud-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-google-cloud-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Framework and source [es-connectors-google-cloud-client-source]

This connector is built with the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/google_cloud_storage.py) (branch *main*, compatible with Elastic *9.0*).



