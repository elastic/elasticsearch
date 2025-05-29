---
navigation_title: "OneDrive"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-onedrive.html
---

# Elastic OneDrive connector reference [es-connectors-onedrive]


The *Elastic OneDrive connector* is a [connector](/reference/search-connectors/index.md) for OneDrive. This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/onedrive.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-onedrive-connector-client-reference]

### Availability and prerequisites [es-connectors-onedrive-client-availability-prerequisites]

This connector is available as a self-managed connector.

This self-managed connector is compatible with Elastic versions **8.10.0+**.

To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a OneDrive connector [es-connectors-onedrive-create-connector-client]


#### Use the UI [es-connectors-onedrive-client-create-use-the-ui]

To create a new OneDrive connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **OneDrive** self-managed connector.


#### Use the API [es-connectors-onedrive-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed OneDrive self-managed connector.

For example:

```console
PUT _connector/my-onedrive-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from OneDrive",
  "service_type": "onedrive"
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


### Usage [es-connectors-onedrive-client-usage]

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


#### Connecting to OneDrive [es-connectors-onedrive-client-usage-connection]

To connect to OneDrive you need to [create an Azure Active Directory application and service principal](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) that can access resources.

Follow these steps:

1. Go to the [Azure portal](https://portal.azure.com) and sign in with your Azure account.
2. Navigate to the **Azure Active Directory** service.
3. Select **App registrations** from the left-hand menu.
4. Click on the **New registration** button to register a new application.
5. Provide a name for your app, and optionally select the supported account types (e.g., single tenant, multi-tenant).
6. Click on the **Register** button to create the app registration.
7. After the registration is complete, you will be redirected to the app’s overview page. Take note of the **Application (client) ID** value, as you’ll need it later.
8. Scroll down to the **API permissions** section and click on the **Add a permission** button.
9. In the **Request API permissions** pane, select **Microsoft Graph** as the API.
10. Choose the application permissions and select the following permissions under the **Application** tab: `User.Read.All`, `File.Read.All`
11. Click on the **Add permissions** button to add the selected permissions to your app. Finally, click on the **Grant admin consent** button to grant the required permissions to the app. This step requires administrative privileges. **NOTE**: If you are not an admin, you need to request the Admin to grant consent via their Azure Portal.
12. Click on **Certificates & Secrets** tab. Go to Client Secrets. Generate a new client secret and keep a note of the string present under `Value` column.


### Deployment using Docker [es-connectors-onedrive-client-docker]

Self-managed connectors are run on your own infrastructure.

You can deploy the OneDrive connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: onedrive
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



### Configuration [es-connectors-onedrive-client-usage-configuration]

The following configuration fields are **required**:

`client_id`
:   Azure application Client ID, unique identifier for your Azure Application, found on the app’s overview page. Example:

    * `ab123453-12a2-100a-1123-93fd09d67394`


`client_secret`
:   Azure application Client Secret, string value that the application uses to prove its identity when requesting a token. Available under the `Certificates & Secrets` tab of your Azure application menu. Example:

    * `eyav1~12aBadIg6SL-STDfg102eBfCGkbKBq_Ddyu`


`tenant_id`
:   Azure application Tenant ID: unique identifier of your Azure Active Directory instance. Example:

    * `123a1b23-12a3-45b6-7c8d-fc931cfb448d`


`retry_count`
:   The number of retry attempts after failed request to OneDrive. Default value is `3`.

`use_document_level_security`
:   Toggle to enable [document level security](/reference/search-connectors/document-level-security.md). When enabled:

    * Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
    * Access control syncs will fetch users' access control lists and store them in a separate index.

        ::::{warning}
        Enabling DLS for your connector will cause a significant performance degradation, as the API calls to the data source required for this functionality are rate limited. This impacts the speed at which your content can be retrieved.

        ::::


`use_text_extraction_service`
:   Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that ingest pipeline settings disable text extraction. Default value is `False`.


### Content Extraction [es-connectors-onedrive-client-usage-content-extraction]

Refer to [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md) for more details.


### Documents and syncs [es-connectors-onedrive-client-documents-syncs]

The connector syncs the following objects and entities:

* **Files**

    * Includes metadata such as file name, path, size, content, etc.

* **Folders**

::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced by default. You must first enable [DLS](#es-connectors-onedrive-client-dls). Otherwise, **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-onedrive-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Document level security [es-connectors-onedrive-client-dls]

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. This feature is available by default for the OneDrive connector. See [Configuration](#es-connectors-onedrive-client-usage-configuration) for how to enable DLS for this connector.

Refer to [document level security](/reference/search-connectors/document-level-security.md) for more details about this feature.

::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data with DLS enabled, when building a search application.

::::



### Sync rules [es-connectors-onedrive-client-documents-sync-rules]

*Basic* sync rules are identical for all connectors and are available by default. For more information read [Types of sync rule](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


#### Advanced sync rules [es-connectors-onedrive-client-sync-rules-advanced]

This connector supports [advanced sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for remote filtering. These rules cover complex query-and-filter scenarios that cannot be expressed with basic sync rules. Advanced sync rules are defined through a source-specific DSL JSON snippet.

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


Here are a few examples of advanced sync rules for this connector.

$$$es-connectors-onedrive-client-sync-rules-advanced-examples-1$$$
**Example 1**

This rule skips indexing for files with `.xlsx` and `.docx` extensions. All other files and folders will be indexed.

```js
[
  {
    "skipFilesWithExtensions": [".xlsx" , ".docx"]
  }
]
```
% NOTCONSOLE

$$$es-connectors-onedrive-client-sync-rules-advanced-examples-2$$$
**Example 2**

This rule focuses on indexing files and folders owned by `user1-domain@onmicrosoft.com` and `user2-domain@onmicrosoft.com` but excludes files with `.py` extension.

```js
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user2-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".py"]
  }
]
```
% NOTCONSOLE

$$$es-connectors-onedrive-client-sync-rules-advanced-examples-3$$$
**Example 3**

This rule indexes only the files and folders directly inside the root folder, excluding any `.md` files.

```js
[
  {
    "skipFilesWithExtensions": [".md"],
    "parentPathPattern": "/drive/root:"
  }
]
```
% NOTCONSOLE

$$$es-connectors-onedrive-client-sync-rules-advanced-examples-4$$$
**Example 4**

This rule indexes files and folders owned by `user1-domain@onmicrosoft.com` and `user3-domain@onmicrosoft.com` that are directly inside the `abc` folder, which is a subfolder of any folder under the `hello` directory in the root. Files with extensions `.pdf` and `.py` are excluded.

```js
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user3-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".pdf", ".py"],
    "parentPathPattern": "/drive/root:/hello/**/abc"
  }
]
```
% NOTCONSOLE

$$$es-connectors-onedrive-client-sync-rules-advanced-examples-5$$$
**Example 5**

This example contains two rules. The first rule indexes all files and folders owned by `user1-domain@onmicrosoft.com` and `user2-domain@onmicrosoft.com`. The second rule indexes files for all other users, but skips files with a `.py` extension.

```js
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user2-domain@onmicrosoft.com"]
  },
  {
    "skipFilesWithExtensions": [".py"]
  }
]
```
% NOTCONSOLE

$$$es-connectors-onedrive-client-sync-rules-advanced-examples-6$$$
**Example 6**

This example contains two rules. The first rule indexes all files owned by `user1-domain@onmicrosoft.com` and `user2-domain@onmicrosoft.com`, excluding `.md` files. The second rule indexes files and folders recursively inside the `abc` folder.

```js
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user2-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".md"]
  },
  {
    "parentPathPattern": "/drive/root:/abc/**"
  }
]
```
% NOTCONSOLE


### Content Extraction [es-connectors-onedrive-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Self-managed connector operations [es-connectors-onedrive-client-connector-client-operations]


### End-to-end testing [es-connectors-onedrive-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the GitHub connector, run the following command:

```shell
$ make ftest NAME=onedrive
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=onedrive DATA_SIZE=small
```


### Known issues [es-connectors-onedrive-client-known-issues]

* **Enabling document-level security impacts performance.**

    Enabling DLS for your connector will cause a significant performance degradation, as the API calls to the data source required for this functionality are rate limited. This impacts the speed at which your content can be retrieved.


Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-onedrive-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-onedrive-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).