---
navigation_title: "ServiceNow"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-servicenow.html
---

# Elastic ServiceNow connector reference [es-connectors-servicenow]


The *Elastic ServiceNow connector* is a [connector](/reference/search-connectors/index.md) for [ServiceNow](https://www.servicenow.com).

This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/servicenow.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-servicenow-connector-client-reference]

### Availability and prerequisites [es-connectors-servicenow-client-availability-prerequisites]

The ServiceNow connector was introduced in Elastic version 8.9.0. This connector is available as a self-managed connector. To use this connector as a self-managed connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a ServiceNow connector [es-connectors-servicenow-create-connector-client]


#### Use the UI [es-connectors-servicenow-client-create-use-the-ui]

To create a new ServiceNow connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **ServiceNow** self-managed connector.


#### Use the API [es-connectors-servicenow-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed ServiceNow self-managed connector.

For example:

```console
PUT _connector/my-servicenow-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from ServiceNow",
  "service_type": "servicenow"
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


### Usage [es-connectors-servicenow-client-usage]

To use this connector as a **self-managed connector**, use the **Customized connector** workflow.

For additional operations, see [Usage](/reference/search-connectors/connectors-ui-in-kibana.md).


### Compatibility [es-connectors-servicenow-client-compatibility]

The ServiceNow connector is compatible with the following versions of ServiceNow:

* ServiceNow "Tokyo"
* ServiceNow "San Diego"
* ServiceNow "Rome"
* ServiceNow "Utah"
* ServiceNow "Vancouver"
* ServiceNow "Washington"
* ServiceNow "Xanadu"


### Configuration [es-connectors-servicenow-client-configuration]




The following configuration fields are required to set up the connector:

`url`
:   The host url of the ServiceNow instance.

`username`
:   The username of the account for ServiceNow.

`password`
:   The password of the account used for ServiceNow.

`services`
:   Comma-separated list of services to fetch data from ServiceNow. If the value is `*`, the connector will fetch data from the list of basic services provided by ServiceNow:

    * [User](https://docs.servicenow.com/bundle/utah-platform-administration/page/administer/roles/concept/user.md)
    * [Incident](https://docs.servicenow.com/bundle/tokyo-it-service-management/page/product/incident-management/concept/c_IncidentManagement.md)
    * [Requested Item](https://docs.servicenow.com/bundle/tokyo-servicenow-platform/page/use/service-catalog-requests/task/t_AddNewRequestItems.md)
    * [Knowledge](https://docs.servicenow.com/bundle/tokyo-customer-service-management/page/product/customer-service-management/task/t_SearchTheKnowledgeBase.md)
    * [Change request](https://docs.servicenow.com/bundle/tokyo-it-service-management/page/product/change-management/task/t_CreateAChange.md)

        ::::{note}
        If you have configured a custom service, the `*` value will not fetch data from the basic services above by default. In this case you’ll need to mention these service names explicitly.

        ::::


        Default value is `*`. Examples:

    * `User, Incident, Requested Item, Knowledge, Change request`
    * `*`


`retry_count`
:   The number of retry attempts after a failed request to ServiceNow. Default value is `3`.

`concurrent_downloads`
:   The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to `10`.

`use_text_extraction_service`
:   Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that ingest pipeline settings disable text extraction. Default value is `False`.

`use_document_level_security`
:   Restrict access to documents based on a user’s permissions. Refer to [Document level security](#es-connectors-servicenow-client-dls) for more details.


### Documents and syncs [es-connectors-servicenow-client-documents-syncs]

All services and records the user has access to will be indexed according to the configurations provided. The connector syncs the following ServiceNow object types:

* Records
* Attachments

::::{note}
* Content from files bigger than 10 MB won’t be extracted. Use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced by default. You must enable [document level security](/reference/search-connectors/document-level-security.md). Otherwise, **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-servicenow-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Document level security [es-connectors-servicenow-client-dls]

[Document level security (DLS)](/reference/search-connectors/document-level-security.md) ensures identities and permissions set in ServiceNow are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

The ServiceNow connector supports roles for access control lists (ACLs) to enable document level security in {{es}}. For default services, connectors use the following roles to find users who have access to documents.

| Service | Roles |
| --- | --- |
| User | `admin` |
| Incident | `admin`, `sn_incident_read`, `ml_report_user`, `ml_admin`, `itil` |
| Requested Item | `admin`, `sn_request_read`, `asset`, `atf_test_designer`, `atf_test_admin` |
| Knowledge | `admin`, `knowledge`, `knowledge_manager`, `knowledge_admin` |
| Change request | `admin`, `sn_change_read`, `itil` |

For services other than these defaults, the connector iterates over access controls with `read` operations and finds the respective roles for those services.

:::{important}
The ServiceNow connector applies access control at the service (table) level. This means documents within a given ServiceNow table share the same access control settings. Users with permission to a table can access all documents from that table in Elasticsearch.
:::

::::{note}
The ServiceNow connector does not support scripted and conditional permissions.
::::



### Deployment using Docker [es-connectors-servicenow-client-docker]

You can deploy the ServiceNow connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: servicenow
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



### Sync rules [es-connectors-servicenow-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


#### Advanced sync rules [es-connectors-servicenow-client-sync-rules-advanced]

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


Advanced sync rules are defined through a source-specific DSL JSON snippet.

The following sections provide examples of advanced sync rules for this connector.

$$$es-connectors-servicenow-client-sync-rules-number-incident-service$$$
**Indexing document based on incident number for Incident service**

```js
[
  {
    "service": "Incident",
    "query": "numberSTARTSWITHINC001"
  }
]
```

$$$es-connectors-servicenow-client-sync-rules-active-false-user-service$$$
**Indexing document based on user activity state for User service**

```js
[
  {
    "service": "User",
    "query": "active=False"
  }
]
```

$$$es-connectors-servicenow-client-sync-rules-author-administrator-knowledge-service$$$
**Indexing document based on author name for Knowledge service**

```js
[
  {
    "service": "Knowledge",
    "query": "author.nameSTARTSWITHSystem Administrator"
  }
]
```


### End-to-end Testing [es-connectors-servicenow-client-connector-client-operations-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the ServiceNow connector, run the following command:

```shell
$ make ftest NAME=servicenow
```

Generate performance reports using the following flag: `PERF8=yes`. Toggle test data set size between SMALL, MEDIUM and LARGE with the argument `DATA_SIZE=`. By default, it is set to `MEDIUM`.

Users do not need to have a running Elasticsearch instance or a ServiceNow source to run this test. Docker Compose manages the complete setup of the development environment.


### Known issues [es-connectors-servicenow-client-known-issues]

There are no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues that impact all connectors.


### Troubleshooting [es-connectors-servicenow-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-servicenow-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Content extraction [es-connectors-servicenow-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).