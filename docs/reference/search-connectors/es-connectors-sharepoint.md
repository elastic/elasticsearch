---
navigation_title: "SharePoint Server"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-sharepoint.html
---

# Elastic SharePoint Server connector reference [es-connectors-sharepoint]


The *Elastic SharePoint Server connector* is a [connector](/reference/search-connectors/index.md) for [Microsoft SharePoint Server](https://www.microsoft.com/en-ww/microsoft-365/sharepoint/).

This connector is written in Python using the open code [Elastic connector framework](https://github.com/elastic/connectors/tree/main). View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/sharepoint_server.py).

::::{tip}
Looking for the SharePoint **Online** connector? See the [SharePoint Online reference](/reference/search-connectors/es-connectors-sharepoint-online.md).

::::

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-sharepoint-connector-client-reference]

### Availability and prerequisites [es-connectors-sharepoint-client-availability-prerequisites]

This connector is available as a self-managed connector. This self-managed connector is compatible with Elastic versions **8.9.0+**. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).

::::{note}
This connector is in **beta** and is subject to change. Beta features are subject to change and are not covered by the support SLA of generally available (GA) features. Elastic plans to promote this feature to GA in a future release.

::::



### Create a SharePoint Server connector [es-connectors-sharepoint-client-create-connector-client]


#### Use the UI [es-connectors-sharepoint_server-client-create-use-the-ui]

To create a new SharePoint Server connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **SharePoint Server** self-managed connector.


#### Use the API [es-connectors-sharepoint_server-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed SharePoint Server self-managed connector.

For example:

```console
PUT _connector/my-sharepoint_server-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from SharePoint Server",
  "service_type": "sharepoint_server"
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


### Usage [es-connectors-sharepoint-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md).

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Compatibility [es-connectors-sharepoint-client-compatability]

The following SharePoint Server versions are compatible with the Elastic connector framework:

* SharePoint 2013
* SharePoint 2016
* SharePoint 2019


### Configuration [es-connectors-sharepoint-client-configuration]

The following configuration fields are required to set up the connector:

`authentication`
:   Authentication mode, either **Basic** or **NTLM**.

`username`
:   The username of the account for the SharePoint Server instance.

`password`
:   The password of the account.

`host_url`
:   The server host url where the SharePoint Server instance is hosted. Examples:

    * `https://192.158.1.38:8080`
    * `https://<tenant_name>.sharepoint.com`


`site_collections`
:   Comma-separated list of site collections to fetch from SharePoint Server. Examples:

    * `collection1`
    * `collection1, collection2`


`ssl_enabled`
:   Whether SSL verification will be enabled. Default value is `False`.

`ssl_ca`
:   Content of SSL certificate needed for the SharePoint Server instance. Keep this field empty, if `ssl_enabled` is set to `False`.

    Example certificate:

    ```txt
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```


`retry_count`
:   The number of retry attempts after failed request to the SharePoint Server instance. Default value is `3`.

`use_document_level_security`
:   Toggle to enable [Document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled, full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. Access control syncs fetch users' access control lists and store them in a separate index.

    Once enabled, the following granular permissions toggles will be available:

    * **Fetch unique list permissions**: Enable this option to fetch unique **list** permissions. If this setting is disabled a list will inherit permissions from its parent site.
    * **Fetch unique list item permissions**: Enable this option to fetch unique **list item** permissions. If this setting is disabled a list item will inherit permissions from its parent site.

        ::::{note}
        If left empty the default value `true` will be used for these granular permissions toggles. Note that these settings may increase sync times.

        ::::



### Deployment using Docker [es-connectors-sharepoint-client-docker]

You can deploy the SharePoint Server connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: sharepoint_server
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



### Documents and syncs [es-connectors-sharepoint-client-documents-syncs]

The connector syncs the following SharePoint object types:

* Sites and Subsites
* Lists
* List Items and its attachment content
* Document Libraries and its attachment content(include Web Pages)

::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. Use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elasticsearch Index.

::::



#### Sync types [es-connectors-sharepoint-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental), but this feature is currently disabled by default. Refer to the linked documentation for enabling incremental syncs.


### Document level security [es-connectors-sharepoint-client-document-level-security]

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. Refer to [configuration](#es-connectors-sharepoint-client-configuration) on this page for how to enable DLS for this connector.

::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Sync rules [es-connectors-sharepoint-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently filtering is controlled via ingest pipelines.


### Content Extraction [es-connectors-sharepoint-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Self-managed connector operations [es-connectors-sharepoint-client-connector-client-operations]


### End-to-end testing [es-connectors-sharepoint-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the sharepoint connector, run the following command:

```shell
$ make ftest NAME=sharepoint_server
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=sharepoint_server DATA_SIZE=small
```


### Known issues [es-connectors-sharepoint-client-known-issues]

There are currently no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-sharepoint-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-sharepoint-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Framework and source [es-connectors-sharepoint-client-source]

This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/sharepoint_server.py) (branch *main*, compatible with Elastic *9.0*).
