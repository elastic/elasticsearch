---
navigation_title: "Sandfly Security"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-sandfly.html
---

# Elastic Sandfly Security connector reference [es-connectors-sandfly]


The *Elastic Sandfly Security connector* is a [connector](/reference/search-connectors/index.md) for [Sandfly Security](https://www.sandflysecurity.com).
This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/sandfly) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available.
All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

::::{note}
This connector is a community contribution and is not supported by Elastic.
Support for this connector is provided by the community.
Please refer to the connector's source code repository for issues and support requests.
::::


## **Self-managed connector** [es-connectors-sandfly-connector-client-reference]

### Availability and prerequisites [es-connectors-sandfly-client-availability-prerequisites]

This connector is available as a self-managed connector.

This self-managed connector is compatible with Elastic versions **9.1.0+**.

To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a Sandfly Security connector [es-connectors-sandfly-create-connector-client]


#### Use the UI [es-connectors-sandfly-client-create-use-the-ui]

To create a new Sandfly Security connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Sandfly Security** self-managed connector.


#### Use the API [es-connectors-sandfly-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Sandfly Security connector.

For example:

```console
PUT _connector/my-sandfly-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Sandfly Security",
  "service_type": "sandfly"
}
```

:::::{dropdown} You'll also need to create an API key for the connector to use.
::::{note}
The user needs the cluster privileges `manage_api_key`, `manage_connector` and `write_connector_secrets` to generate API keys programmatically.

::::


To create an API key for the connector:

1. Run the following command, replacing values where indicated.
Note the `encoded` return values from the response:

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


### Usage [es-connectors-sandfly-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md)
For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


#### Sandfly Security credentials [es-connectors-sandfly-client-credentials]

Configure Sandfly Security credentials to fetch data from your Sandfly Security server.

You'll need to provide:

* **Server URL**: The URL of your Sandfly Security server, including the API version (v4).
For example: `https://your-sandfly-server.com/v4`
* **Username**: A valid username for the Sandfly Security server
* **Password**: The password for the specified username



### Compatibility [es-connectors-sandfly-client-compatibility]

This connector is compatible with Sandfly Security servers that support API version v4.


### Configuration [es-connectors-sandfly-client-configuration]

The following configuration fields are required:

`server_url`
:   Sandfly Server URL including the API version (v4).
For example: `https://server-name/v4`

`username`
:   Sandfly Server Username for authentication.

`password`
:   Sandfly Server Password for authentication.

`enable_pass`
:   Toggle to enable indexing of "pass" results.
When disabled (default), only Alert and Error results are indexed.
Default value is `False`.

`verify_ssl`
:   Toggle to verify the Sandfly Server SSL certificate.
Disable to allow self-signed certificates.
Default value is `True`.

`fetch_days`
:   Number of days of results history to fetch during a Full Content Sync.
Default value is `30`.


### Deployment using Docker [es-connectors-sandfly-client-docker]

You can deploy the Sandfly Security connector as a self-managed connector using Docker.
Follow these instructions.

::::{dropdown} Step 1: Download sample configuration file
Download the sample configuration file.
You can either download it manually or run the following command:

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

If you're running the connector service against a Dockerized version of Elasticsearch and Kibana, your config file will look like this:

```yaml
# When connecting to your cloud deployment you should edit the host value
elasticsearch.host: http://host.docker.internal:9200
elasticsearch.api_key: <ELASTICSEARCH_API_KEY>

connectors:
  -
    connector_id: <CONNECTOR_ID_FROM_KIBANA>
    service_type: sandfly
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

Using the `elasticsearch.api_key` is the recommended authentication method.
However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

Note: You can change other default configurations by simply uncommenting specific settings in the configuration file and modifying their values.

::::


::::{dropdown} Step 3: Run the Docker image
Run the Docker image with the Connector Service using the following command:

```sh subs=true
docker run \
-v ~/connectors-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors:{{version.stack}} \
/app/bin/elastic-ingest \
-c /config/config.yml
```
% NOTCONSOLE
::::


Refer to [`DOCKER.md`](https://github.com/elastic/connectors/tree/main/docs/DOCKER.md) in the `elastic/connectors` repo for more details.

Find all available Docker images in the [official registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).

::::{tip}
We also have a quickstart self-managed option using Docker Compose, so you can spin up all required services at once: Elasticsearch, Kibana, and the connectors service.
Refer to this [README](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.

::::



### Documents and syncs [es-connectors-sandfly-client-documents-syncs]

The connector syncs the following objects and entities from Sandfly Security:

* **Results**: Investigation results including alerts, errors, and check results from security scans
* **Hosts**: Information about Linux hosts protected by Sandfly Security
* **SSH Keys**: Details about SSH keys discovered during investigations

::::{note}
* License validation is performed during each sync to ensure the Sandfly Security server is properly licensed for Elasticsearch Replication.
* By default, only Alert and Error results are synced.
Enable "Pass Results" configuration to include all result types.

::::



#### Sync types [es-connectors-sandfly-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-sandfly-client-sync-rules]

*Basic* sync rules are identical for all connectors and are available by default.
For more information read [Types of sync rule](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


#### Advanced sync rules [es-connectors-sandfly-client-sync-rules-advanced]

::::{note}
Advanced sync rules are not currently supported for this connector.

::::


### Content Extraction [es-connectors-sandfly-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### End-to-end testing [es-connectors-sandfly-client-testing]

The connector framework enables operators to run functional tests against a real data source.
Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Sandfly Security connector, run the following command:

```shell
$ make ftest NAME=sandfly
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=sandfly DATA_SIZE=small
```

### Known issues [es-connectors-sandfly-client-known-issues]

There are currently no known issues for this connector.
Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-sandfly-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-sandfly-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).
