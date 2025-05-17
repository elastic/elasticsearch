---
navigation_title: "Dropbox"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-dropbox.html
---

# Elastic Dropbox connector reference [es-connectors-dropbox]


The *Elastic Dropbox connector* is a [connector](/reference/search-connectors/index.md) for [Dropbox](https://www.dropbox.com). This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/dropbox.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-dropbox-connector-client-reference]

### Availability and prerequisites [es-connectors-dropbox-client-availability-and-prerequisites]

This connector is available as a self-managed connector.

This self-managed connector is compatible with Elastic versions **8.9.0**+.

To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md#es-build-connector-prerequisites).


### Create a Dropbox connector [es-connectors-dropbox-create-connector-client]


#### Use the UI [es-connectors-dropbox-client-create-use-the-ui]

To create a new Dropbox connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Dropbox** self-managed connector.


#### Use the API [es-connectors-dropbox-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Dropbox self-managed connector.

For example:

```console
PUT _connector/my-dropbox-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Dropbox",
  "service_type": "dropbox"
}
```
% TEST[skip:can’t test in isolation]

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


### Usage [es-connectors-dropbox-client-usage]

Before you can configure your connector, you’ll need to:

* [Create a Dropbox OAuth app](#es-connectors-dropbox-client-create-dropbox-oauth-app)
* [Generate a refresh token](#es-connectors-dropbox-client-refresh-token)

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) Once set up, for additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Dropbox API Authorization [es-connectors-dropbox-client-dropbox-api-authorization]


#### Create Dropbox OAuth App [es-connectors-dropbox-client-create-dropbox-oauth-app]

You’ll need to create an OAuth app in the Dropbox platform by following these steps:

1. Register a new app in the [Dropbox App Console](https://www.dropbox.com/developers/apps). Select **Full Dropbox API app** and choose the following required permissions:

    * `files.content.read`
    * `sharing.read`

        To use document level security, you’ll also need the following permissions:

    * `team_info.read`
    * `team_data.member`
    * `team_data.content.read`
    * `members.read`

2. Once the app is created, make note of the **app key** and **app secret** values which you’ll need to configure the Dropbox connector on your Elastic deployment.


#### Generate a refresh Token [es-connectors-dropbox-client-refresh-token]

To generate a refresh token, follow these steps:

1. Go to the following URL, replacing `<APP_KEY>` with the **app key** value saved earlier: `https://www.dropbox.com/oauth2/authorize?client_id=<APP_KEY>&response_type=code&token_access_type=offline`

    The HTTP response should contain an **authorization code** that you’ll use to generate a refresh token. An authorization code **can only be used once** to create a refresh token.

2. In your terminal, run the following `cURL` command, replacing `<AUTHORIZATION_CODE>`, `<APP_KEY>:<APP_SECRET>` with the values you saved earlier:

    ```shell
    curl -X POST "https://api.dropboxapi.com/oauth2/token?code=<AUTHORIZATION_CODE>&grant_type=authorization_code" -u "<APP_KEY>:<APP_SECRET>"
    ```

    Store the refresh token from the response to be used in the connector configuration.

    Make sure the response has a list of the following scopes:

    * `account_info.read`
    * `files.content.read`
    * `files.metadata.read`
    * `sharing.read`
    * `team_info.read` (if using document level security)
    * `team_data.member` (if using document level security)
    * `team_data.content.read` (if using document level security)
    * `members.read` (if using document level security)



### Configuration [es-connectors-dropbox-client-configuration]




The following configuration fields are required to set up the connector:

`path`
:   The folder path to fetch files/folders from Dropbox. Default value is `/`.

`app_key` (required)
:   The App Key to authenticate your Dropbox application.

`app_secret` (required)
:   The App Secret to authenticate your Dropbox application.

`refresh_token` (required)
:   The refresh token to authenticate your Dropbox application.

use_document_level_security
:   Toggle to enable [document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled, full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. Access control syncs will fetch users' access control lists and store them in a separate index.

`retry_count`
:   The number of retry attempts after a failed request to Dropbox. Default value is `3`.

`concurrent_downloads`
:   The number of concurrent downloads for fetching attachment content. This can help speed up content extraction of attachments. Defaults to `100`.

`use_text_extraction_service`
:   Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that pipeline settings disable text extraction. Default value is `False`.

`use_document_level_security`
:   Toggle to enable [document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled, full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. Access control syncs will fetch users' access control lists and store them in a separate index.

`include_inherited_users_and_groups`
:   Depends on document level security being enabled. Include groups and inherited users when indexing permissions.

::::{warning}
Enabling `Include groups and inherited users` will cause a signficant performance degradation.

::::



### Deployment using Docker [es-connectors-dropbox-client-docker]

You can deploy the Dropbox connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: dropbox
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



### Documents and syncs [es-connectors-dropbox-client-documents-and-syncs]

The connector syncs the following objects and entities:

* **Files**

    * Includes metadata such as file name, path, size, content, etc.

* **Folders**

::::{note}
Due to a Dropbox issue, metadata updates to Paper files from Dropbox Paper are not immediately reflected in the Dropbox UI. This delays the availability of updated results for the connector. Once the metadata changes are visible in the Dropbox UI, the updates are available.

::::


::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Currently, the connector doesn’t retrieve files from shared Team folders.
* Permissions are not synced by default. If [document level security (DLS)](/reference/search-connectors/document-level-security.md) is not enabled **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-dropbox-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-dropbox-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


#### Advanced sync rules [es-connectors-dropbox-client-sync-rules-advanced]

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


The following section describes [advanced sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for this connector.

Advanced sync rules for Dropbox allow you to sync Dropbox files based on a query that matches strings in the filename. You can optionally filter the results of the query by `file_extensions` or `file_categories`. When both are provided, priority is given to `file_categories`. We have some examples below for illustration.

$$$es-connectors-dropbox-client-sync-rules-advanced-example-1$$$
**Example: Query only**

```js
[
  {
    "query": "confidential"
  },
  {
    "query": "dropbox"
  }
]
```
% NOTCONSOLE

$$$es-connectors-dropbox-client-sync-rules-advanced-example-2$$$
**Example: Query with file extension filter**

```js
[
  {
    "query": "dropbox",
    "options": {
      "file_extensions": [
        "txt",
        "pdf"
      ]
    }
  }
]
```
% NOTCONSOLE

$$$es-connectors-dropbox-client-sync-rules-advanced-example-3$$$
**Example: Query with file category filter**

```js
[
  {
    "query": "test",
    "options": {
      "file_categories": [
        {
          ".tag": "paper"
        },
        {
          ".tag": "png"
        }
      ]
    }
  }
]
```
% NOTCONSOLE

$$$es-connectors-dropbox-client-sync-rules-advanced-limitations$$$
**Limitations**

* Content extraction is not supported for Dropbox **Paper** files when advanced sync rules are enabled.


### End-to-end Testing [es-connectors-dropbox-client-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Dropbox connector, run the following command:

```shell
$ make ftest NAME=dropbox
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=dropbox DATA_SIZE=small
```


### Known issues [es-connectors-dropbox-client-known-issues]

Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-dropbox-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md) for a list of troubleshooting tips for all connectors.


### Security [es-connectors-dropbox-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md) for a list of security tips for all connectors.


### Content extraction [es-connectors-dropbox-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).

