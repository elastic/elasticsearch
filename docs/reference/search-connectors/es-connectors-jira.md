---
navigation_title: "Jira"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-jira.html
---

# Elastic Jira connector reference [es-connectors-jira]


The *Elastic Jira connector* is a [connector](/reference/search-connectors/index.md) for [Atlassian Jira](https://www.atlassian.com/software/jira). This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/jira.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-jira-connector-client-reference]

### Availability and prerequisites [es-connectors-jira-client-availability-prerequisites]

This connector is available as a self-managed connector. This self-managed connector is compatible with Elastic versions **8.7.0+**.

::::{note}
Jira Data Center support was added in 8.13.0 in technical preview and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::


To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a Jira connector [es-connectors-jira-create-connector-client]


#### Use the UI [es-connectors-jira-client-create-use-the-ui]

To create a new Jira connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Jira** self-managed connector.


#### Use the API [es-connectors-jira-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Jira self-managed connector.

For example:

```console
PUT _connector/my-jira-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Jira",
  "service_type": "jira"
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


### Usage [es-connectors-jira-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Compatibility [es-connectors-jira-client-compatability]

* Jira Cloud, Jira Server, and Jira Data Center **versions 7 or later**.


### Configuration [es-connectors-jira-client-configuration]




The following configuration fields are required to set up the connector:

`data_source`
:   Dropdown to determine the Jira platform type: `Jira Cloud`, `Jira Server`, or `Jira Data Center`. Default value is `Jira Cloud`.

`data_center_username`
:   The username of the account for Jira Data Center.

`data_center_password`
:   The password of the account to be used for Jira Data Center.

`username`
:   The username of the account for Jira Server.

`password`
:   The password of the account to be used for Jira Server.

`account_email`
:   Email address to authenticate with Jira Cloud. Example: `jane.doe@example.com`

`api_token`
:   The API Token to authenticate with Jira Cloud.

`jira_url`
:   The domain where Jira is hosted. Examples:

    * [https://192.158.1.38:8080/](https://192.158.1.38:8080/)
    * [https://test_user.atlassian.net/](https://test_user.atlassian.net/)


`projects`
:   Comma-separated list of [Project Keys](https://support.atlassian.com/jira-software-cloud/docs/what-is-an-issue/#Workingwithissues-Projectkeys) to fetch data from Jira server or cloud. If the value is `*` the connector will fetch data from all projects present in the configured projects. Default value is `*`. Examples:

    * `EC`, `TP`
    * `*`

        ::::{warning}
        This field can be bypassed by advanced sync rules.

        ::::


`ssl_enabled`
:   Whether SSL verification will be enabled. Default value is `False`.

`ssl_ca`
:   Content of SSL certificate. Note: In case of `ssl_enabled` is `False`, the `ssl_ca` value will be ignored. Example certificate:

    ```txt
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```


`retry_count`
:   The number of retry attempts after failed request to Jira. Default value is 3.

`concurrent_downloads`
:   The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to 100.

`use_document_level_security`
:   Toggle to enable [document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled, full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. Access control syncs fetch users' access control lists and store them in a separate index.

    ::::{note}
    To access user data in Jira Administration, the account you created must be granted **Product Access** for Jira Administration. This access needs to be provided by an administrator from the [Atlassian Admin](http://admin.atlassian.com/), and the access level granted should be `Product Admin`.

    ::::


`use_text_extraction_service`
:   Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that ingest pipeline settings disable text extraction. Default value is `False`.


### Deployment using Docker [es-connectors-jira-client-docker]

You can deploy the Jira connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: jira
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



### Documents and syncs [es-connectors-jira-client-documents-syncs]

The connector syncs the following objects and entities:

* **Projects**

    * Includes metadata such as description, project key, project type, lead name, etc.

* **Issues**

    * All types of issues including Task, Bug, Sub-task, Enhancement, Story, etc.
    * Includes metadata such as issue type, parent issue details, fix versions, affected versions, resolution, attachments, comments, sub-task details, priority, custom fields, etc.

* **Attachments**

**Note:** Archived projects and issues are not indexed.

::::{note}
* Content from files bigger than 10 MB won’t be extracted
* Permissions are not synced by default. You must first enable [DLS](#es-connectors-jira-client-document-level-security). Otherwise, **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-jira-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-jira-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

This connector supports [advanced sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for remote filtering. These rules cover complex query-and-filter scenarios that cannot be expressed with basic sync rules. Advanced sync rules are defined through a source-specific DSL JSON snippet.


#### Advanced sync rules example [es-connectors-jira-client-sync-rules-examples]

**Example 1**: Queries to index content based on status of Jira issues.

```js
[
  {
    "query": "project = Collaboration AND status = 'In Progress'"
  },
  {
    "query": "status IN ('To Do', 'In Progress', 'Closed')"
  }
]
```
% NOTCONSOLE

**Example 2**: Query to index data based on priority of issues for given projects.

```js
[
  {
    "query": "priority in (Blocker, Critical) AND project in (ProjA, ProjB, ProjC)"
  }
]
```
% NOTCONSOLE

**Example 3**: Query to index data based on assignee and created time.

```js
[
  {
    "query": "assignee is EMPTY and created < -1d"
  }
]
```
% NOTCONSOLE


### Document level security [es-connectors-jira-client-document-level-security]

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. Refer to [configuration](#es-connectors-jira-client-configuration) on this page for how to enable DLS for this connector.

::::{warning}
Enabling DLS for your connector will cause a significant performance degradation, as the API calls to the data source required for this functionality are rate limited. This impacts the speed at which your content can be retrieved.

::::


::::{warning}
When the `data_source` is set to Confluence Data Center or Server, the connector will only fetch 1000 users for access control syncs, due a [limitation in the API used](https://auth0.com/docs/manage-users/user-search/retrieve-users-with-get-users-endpoint#limitations).

::::


::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Content Extraction [es-connectors-jira-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Self-managed connector operations [es-connectors-jira-client-connector-client-operations]


### End-to-end testing [es-connectors-jira-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Jira connector, run the following command:

```shell
$ make ftest NAME=jira
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=jira DATA_SIZE=small
```


### Known issues [es-connectors-jira-client-known-issues]

* **Enabling document-level security impacts performance.**

    Enabling DLS for your connector will cause a significant performance degradation, as the API calls to the data source required for this functionality are rate limited. This impacts the speed at which your content can be retrieved.


Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-jira-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-jira-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).