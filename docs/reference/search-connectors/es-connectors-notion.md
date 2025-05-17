---
navigation_title: "Notion"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-notion.html
---

# Elastic Notion Connector reference [es-connectors-notion]


The Notion connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main). View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/notion.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-notion-connector-client-reference]

### Availability and prerequisites [es-connectors-notion-client-connector-availability-and-prerequisites]

This connector was introduced in Elastic **8.13.0**, available as a **self-managed** self-managed connector.

To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md). Importantly, you must deploy the connectors service on your own infrastructure. You have two deployment options:

* [Run connectors service from source](/reference/search-connectors/es-connectors-run-from-source.md). Use this option if you’re comfortable working with Python and want to iterate quickly locally.
* [Run connectors service in Docker](/reference/search-connectors/es-connectors-run-from-docker.md). Use this option if you want to deploy the connectors to a server, or use a container orchestration platform.

::::{note}
This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

::::



### Usage [es-connectors-notion-client-connector-usage]

To use this connector in the UI, select the **Notion** tile when creating a new connector under **Search → Connectors**.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Create a Notion connector [es-connectors-notion-create-connector-client]


#### Use the UI [es-connectors-notion-client-create-use-the-ui]

To create a new Notion connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Notion** self-managed connector.


#### Use the API [es-connectors-notion-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Notion self-managed connector.

For example:

```console
PUT _connector/my-notion-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Notion",
  "service_type": "notion"
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


### Connecting to Notion [es-connectors-notion-client-connector-connecting-to-notion]

To connect to Notion, the user needs to [create an internal integration](https://www.notion.so/help/create-integrations-with-the-notion-api#create-an-internal-integration) for their Notion workspace, which can access resources using the Internal Integration Secret Token. Configure the Integration with following settings:

1. Users must grant `READ` permission for content, comment and user capabilities for that integration from the Capabilities tab.
2. Users must manually [add the integration as a connection](https://www.notion.so/help/add-and-manage-connections-with-the-api#add-connections-to-pages) to the top-level pages in a workspace. Sub-pages will inherit the connections of the parent page automatically.


### Deploy with Docker [es-connectors-notion-client-connector-docker]

You can deploy the Notion connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: notion
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



### Configuration [es-connectors-notion-client-connector-configuration]

Note the following configuration fields:

`Notion Secret Key`(required)
:   Secret token assigned to your integration, for a particular workspace. Example:

    * `zyx-123453-12a2-100a-1123-93fd09d67394`


`Databases`(required)
:   Comma-separated list of database names to be fetched by the connector. If the value is `*`, connector will fetch all the databases available in the workspace. Example:

    * `database1, database2`
    * `*`


`Pages`(required)
:   Comma-separated list of page names to be fetched by the connector. If the value is `*`, connector will fetch all the pages available in the workspace. Examples:

    * `*`
    * `Page1, Page2`


`Index Comments`
:   Toggle to enable fetching and indexing of comments from the Notion workspace for the configured pages, databases and the corresponding child blocks. Default value is `False`.

::::{note}
Enabling comment indexing could impact connector performance due to increased network calls. Therefore, by default this value is `False`.

::::



#### Content Extraction [es-connectors-notion-client-connector-content-extraction]

Refer to [content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Documents and syncs [es-connectors-notion-client-connector-documents-and-syncs]

The connector syncs the following objects and entities:

* **Pages**

    * Includes metadata such as `page name`, `id`, `last updated time`, etc.

* **Blocks**

    * Includes metadata such as `title`, `type`, `id`, `content` (in case of file block), etc.

* **Databases**

    * Includes metadata such as `name`, `id`, `records`, `size`, etc.

* **Users**

    * Includes metadata such as `name`, `id`, `email address`, etc.

* **Comments**

    * Includes the content and metadata such as `id`, `last updated time`, `created by`, etc.
    * **Note**: Comments are excluded by default.


::::{note}
* Files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to the relevant Elasticsearch index.

::::



### Sync rules [es-connectors-notion-client-connector-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


### Advanced sync rules [es-connectors-notion-client-connector-advanced-sync-rules]

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


The following section describes **advanced sync rules** for this connector, to filter data in Notion *before* indexing into {{es}}. Advanced sync rules are defined through a source-specific DSL JSON snippet.

Advanced sync rules for Notion take the following parameters:

1. `searches`: Notion’s search filter to search by title.
2. `query`: Notion’s database query filter to fetch a specific database.


#### Examples [es-connectors-notion-client-connector-advanced-sync-rules-examples]

**Example 1**

Indexing every page where the title contains `Demo Page`:

```js
  {
    "searches": [
      {
        "filter": {
          "value": "page"
        },
        "query": "Demo Page"
      }
    ]
  }
```
% NOTCONSOLE

**Example 2**

Indexing every database where the title contains `Demo Database`:

```js
{
  "searches": [
    {
      "filter": {
        "value": "database"
      },
      "query": "Demo Database"
    }
  ]
}
```
% NOTCONSOLE

**Example 3**

Indexing every database where the title contains `Demo Database` and every page where the title contains `Demo Page`:

```js
{
  "searches": [
    {
      "filter": {
        "value": "database"
      },
      "query": "Demo Database"
    },
    {
      "filter": {
        "value": "page"
      },
      "query": "Demo Page"
    }
  ]
}
```
% NOTCONSOLE

**Example 4**

Indexing all pages in the workspace:

```js
{
  "searches": [
    {
      "filter": {
        "value": "page"
      },
      "query": ""
    }
  ]
}
```
% NOTCONSOLE

**Example 5**

Indexing all the pages and databases connected to the workspace:

```js
{
  "searches":[
    {
      "query":""
    }
  ]
}
```
% NOTCONSOLE

**Example 6**

Indexing all the rows of a database where the record is `true` for the column `Task completed` and its property(datatype) is a checkbox:

```js
{
  "database_query_filters": [
    {
      "filter": {
          "property": "Task completed",
          "checkbox": {
            "equals": true
          }
      },
      "database_id": "database_id"
    }
  ]
}
```
% NOTCONSOLE

**Example 7**

Indexing all rows of a specific database:

```js
{
  "database_query_filters": [
    {
      "database_id": "database_id"
    }
  ]
}
```
% NOTCONSOLE

**Example 8**

Indexing all blocks defined in `searches` and `database_query_filters`:

```js
{
  "searches":[
    {
      "query":"External tasks",
      "filter":{
        "value":"database"
      }
    },
    {
      "query":"External tasks",
      "filter":{
        "value":"page"
      }
    }
  ],
  "database_query_filters":[
    {
      "database_id":"notion_database_id1",
      "filter":{
        "property":"Task completed",
        "checkbox":{
          "equals":true
        }
      }
    }
  ]
}
```
% NOTCONSOLE

::::{note}
In this example the `filter` object syntax for `database_query_filters` is defined per the [Notion documentation](https://developers.notion.com/reference/post-database-query-filter).

::::



### Connector Client operations [es-connectors-notion-client-connector-connector-client-operations]


#### End-to-end Testing [es-connectors-notion-client-connector-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source, using Docker Compose. You don’t need a running Elasticsearch instance or Notion source to run this test.

Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Notion connector, run the following command:

```shell
$ make ftest NAME=notion
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=notion DATA_SIZE=small
```

By default, `DATA_SIZE=MEDIUM`.


### Known issues [es-connectors-notion-client-connector-known-issues]

* **Updates to new pages may not be reflected immediately in the Notion API.**

    This could lead to these pages not being indexed by the connector, if a sync is initiated immediately after their addition. To ensure all pages are indexed, initiate syncs a few minutes after adding pages to Notion.

* **Notion’s Public API does not support linked databases.**

    Linked databases in Notion are copies of a database that can be filtered, sorted, and viewed differently. To fetch the information in a linked database, you need to target the original **source** database. For more details refer to the [Notion documentation](https://developers.notion.com/docs/working-with-databases#linked-databases).

* **Documents' `properties` objects are serialized as strings under `details`**.

    Notion’s schema for `properties` is not consistent, and can lead to `document_parsing_exceptions` if indexed to Elasticsearch as an object. For this reason, the `properties` object is instead serialized as a JSON string, and stored under the `details` field. If you need to search a sub-object from `properties`, you may need to post-process the `details` field in an ingest pipeline to extract your desired subfield(s).


Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-notion-client-connector-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-notion-client-connector-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

