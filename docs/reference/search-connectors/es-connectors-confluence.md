---
navigation_title: "Confluence"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-confluence.html
---

# Elastic Confluence connector reference [es-connectors-confluence]


The *Elastic Confluence connector* is a [connector](/reference/search-connectors/index.md) for [Atlassian Confluence](https://www.atlassian.com/software/confluence). This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/confluence.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-confluence-connector-client-reference]

### Availability and prerequisites [es-connectors-confluence-client-availability-prerequisites]

This connector is available as a **self-managed connector** using the **Elastic connector framework**. This self-managed connector is compatible with Elastic versions **8.7.0+**.

::::{note}
Confluence Data Center support was added in 8.13.0 in technical preview and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::


To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a Confluence connector [es-connectors-confluence-create-connector-client]


#### Use the UI [es-connectors-confluence-client-create-use-the-ui]

To create a new Confluence connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Confluence** self-managed connector.


#### Use the API [es-connectors-confluence-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Confluence self-managed connector.

For example:

```console
PUT _connector/my-confluence-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Confluence",
  "service_type": "confluence"
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


### Usage [es-connectors-confluence-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Compatibility [es-connectors-confluence-client-compatability]

* Confluence Cloud or Confluence Server/Data Center **versions 7 or later**


### Configuration [es-connectors-confluence-client-configuration]




The following configuration fields are required to set up the connector:

`data_source`
:   Dropdown to determine the Confluence platform type: `Confluence Cloud`, `Confluence Server`, or `Confluence Data Center`. Default value is `Confluence Server`.

`data_center_username`
:   The username of the account for Confluence Data Center.

`data_center_password`
:   The password of the account to be used for the Confluence Data Center.

`username`
:   The username of the account for Confluence Server.

`password`
:   The password of the account to be used for the Confluence server.

`account_email`
:   The account email for the Confluence Cloud.

`api_token`
:   The API Token to authenticate with Confluence Cloud.

`confluence_url`
:   The domain where the Confluence instance is hosted. Examples:

    * `https://192.158.1.38:8080/`
    * `https://test_user.atlassian.net/`


`spaces`
:   Comma-separated list of [Space Keys](https://confluence.atlassian.com/doc/space-keys-829076188.html) to fetch data from Confluence. If the value is `*`, the connector will fetch data from all spaces present in the configured `spaces`. Default value is `*`. Examples:

    * `EC`, `TP`
    * `*`


`index_labels`
:   Toggle to enable syncing of labels from pages. NOTE: This will increase the amount of network calls to the source, and may decrease performance.

`ssl_enabled`
:   Whether SSL verification will be enabled. Default value is `False`.

`ssl_ca`
:   Content of SSL certificate. Note: If `ssl_enabled` is `False`, the value in this field is ignored. Example certificate:

    ```txt
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```


`retry_count`
:   The number of retry attempts after failed request to Confluence. Default value is `3`.

`concurrent_downloads`
:   The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to `50`.

`use_document_level_security`
:   Toggle to enable [document level security (DLS)](/reference/search-connectors/document-level-security.md).

    When enabled, full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. Access control syncs will fetch users' access control lists and store them in a separate index.

    ::::{note}
    To access user data in Jira Administration, the account you created must be granted **Product Access** for Jira Administration. This access needs to be provided by an administrator from the [Atlassian Admin](http://admin.atlassian.com/), and the access level granted should be `Product Admin`.

    ::::


`use_text_extraction_service`
:   Toggle to enable the local text extraction service. Default value is `False`. Requires a separate deployment of the Elastic Text Extraction Service. Requires that ingest pipeline settings disable text extraction.


### Deployment using Docker [es-connectors-confluence-client-docker]

You can deploy the Confluence connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: confluence
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



### Documents and syncs [es-connectors-confluence-client-documents-syncs]

The connector syncs the following Confluence object types:

* Pages
* Spaces
* Blog Posts
* Attachments

::::{note}
* Content of files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-confluence-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-confluence-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

This connector supports [advanced sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for remote filtering. These rules cover complex query-and-filter scenarios that cannot be expressed with <basic sync rules. Advanced sync rules are defined through a source-specific DSL JSON snippet.


#### Advanced sync rules examples [es-connectors-confluence-client-sync-rules-examples]

**Example 1**: Query for indexing data that is in a particular **Space** with key *DEV*.

```js
[
  {
    "query": "space = DEV"
  }
]
```

**Example 2**: Queries for indexing data based on `created` and `lastmodified` time.

```js
[
  {
    "query": "created >= now('-5w')"
  },
  {
    "query": "lastmodified < startOfYear()"
  }
]
```

**Example 3**: Query for indexing only given types in a **Space** with key *SD*.

```js
[
  {
    "query": "type in ('page', 'attachment') AND space.key = 'SD'"
  }
]
```

::::{note}
Syncing recently created/updated items in Confluence may be delayed when using advanced sync rules, because the search endpoint used for CQL queries returns stale results in the response. For more details refer to the following issue in the [Confluence documentation](https://jira.atlassian.com/browse/CONFCLOUD-73997).

::::



### Document level security [es-connectors-confluence-client-document-level-security]

::::{note}
DLS is automatically available for Atlassian Confluence Cloud since 8.9.0. DLS is available since 8.14.0 for Confluence Server and Confluence Data Center, but requires installing [Extender for Confluence](https://marketplace.atlassian.com/apps/1217507/extender-for-confluence?tab=overview&hosting=datacenter).

::::


Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. Refer to [configuration](#es-connectors-confluence-client-configuration) on this page for how to enable DLS for this connector.

::::{warning}
When the `data_source` is set to Confluence Data Center or Server, the connector will only fetch 1000 users for access control syncs, due a [limitation in the API used](https://auth0.com/docs/manage-users/user-search/retrieve-users-with-get-users-endpoint#limitations).

::::


::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Content Extraction [es-connectors-confluence-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Self-managed connector operations [es-connectors-confluence-client-connector-client-operations]


### End-to-end testing [es-connectors-confluence-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Confluence connector, run the following command:

```shell
$ make ftest NAME=confluence
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=confluence DATA_SIZE=small
```


### Known issues [es-connectors-confluence-client-known-issues]

There are currently no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-confluence-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-confluence-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).