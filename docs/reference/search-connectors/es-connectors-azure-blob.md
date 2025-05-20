---
navigation_title: "Azure Blob Storage"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-azure-blob.html
---

# Elastic Azure Blob Storage connector reference [es-connectors-azure-blob]


The *Elastic Azure Blob Storage connector* is a [connector](/reference/search-connectors/index.md) for [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).

This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/azure_blob_storage.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-azure-blob-connector-client-reference]

### Availability and prerequisites [es-connectors-azure-blob-client-availability-prerequisites]

This connector is available as a self-managed connector. This self-managed connector is compatible with Elastic versions **8.6.0+**. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Compatibility [es-connectors-azure-blob-client-compatability]

This connector has not been tested with Azure Government. Therefore we cannot guarantee that it will work with Azure Government endpoints. For more information on Azure Government compared to Global Azure, refer to the [official Microsoft documentation](https://learn.microsoft.com/en-us/azure/azure-government/compare-azure-government-global-azure).


### Create Azure Blob Storage connector [es-connectors-azure_blob_storage-create-connector-client]


#### Use the UI [es-connectors-azure_blob_storage-client-create-use-the-ui]

To create a new Azure Blob Storage connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Azure Blob Storage** self-managed connector.


#### Use the API [es-connectors-azure_blob_storage-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Azure Blob Storage self-managed connector.

For example:

```console
PUT _connector/my-azure_blob_storage-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Azure Blob Storage",
  "service_type": "azure_blob_storage"
}
```

:::::{dropdown} You’ll also need to create an API key for the connector to use.
::::{note}
The user needs the cluster privileges `manage_api_key`, `manage_connector` and `write_connector_secrets` to generate API keys programmatically.

::::


To create an API key for the connector:

1. Run the following command, replacing values where indicated. Note the `encoded` return values from the response:

    ```console
    POST /_security/api_key
    {
      "name": "connector_name-connector-api-key",
      "role_descriptors": {
        "connector_name-connector-role": {
          "cluster": [
            "monitor",
            "manage_connector"
          ],
          "indices": [
            {
              "names": [
                "index_name",
                ".search-acl-filter-index_name",
                ".elastic-connectors*"
              ],
              "privileges": [
                "all"
              ],
              "allow_restricted_indices": false
            }
          ]
        }
      }
    }
    ```

2. Update your `config.yml` file with the API key `encoded` value.

:::::


Refer to the [{{es}} API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) for details of all available Connector APIs.


### Usage [es-connectors-azure-blob-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Configuration [es-connectors-azure-blob-client-configuration]




The following configuration fields are required to set up the connector:

`account_name`
:   Name of Azure Blob Storage account.

`account_key`
:   [Account key](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal) for the Azure Blob Storage account.

`blob_endpoint`
:   Endpoint for the Blob Service.

`containers`
:   List of containers to index. `*` will index all containers.

`retry_count`
:   Number of retry attempts after a failed call. Default value is `3`.

`concurrent_downloads`
:   Number of concurrent downloads for fetching content. Default value is `100`.

`use_text_extraction_service`
:   Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that ingest pipeline settings disable text extraction. Default value is `False`.


### Deployment using Docker [es-connectors-azure-blob-client-docker]

You can deploy the Azure Blob Storage connector as a self-managed connector using Docker. Follow these instructions.

::::{dropdown} Step 1: Download sample configuration file
Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example --output ~/connectors-config/config.yml
```

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
    service_type: azure_blob_storage
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



### Documents and syncs [es-connectors-azure-blob-client-documents-syncs]

The connector will fetch all data available in the container.

::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-azure-blob-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-azure-blob-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently filtering is controlled via ingest pipelines.


### Content extraction [es-connectors-azure-blob-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### End-to-end testing [es-connectors-azure-blob-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Azure Blob Storage connector, run the following command:

```shell
$ make ftest NAME=azure_blob_storage
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=azure_blob_storage DATA_SIZE=small
```


### Known issues [es-connectors-azure-blob-client-known-issues]

This connector has the following known issues:

* **`lease data` and `tier` fields are not updated in Elasticsearch indices**

    This is because the blob timestamp is not updated. Refer to [Github issue](https://github.com/elastic/connectors/issues/289).



### Troubleshooting [es-connectors-azure-blob-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-azure-blob-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).