---
navigation_title: "Outlook"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-outlook.html
---

# Elastic Outlook connector reference [es-connectors-outlook]


The Elastic Outlook connector is built with the Elastic connector framework and is available as a self-managed [self-managed connector](/reference/search-connectors/self-managed-connectors.md).

## **Self-managed connector reference** [es-connectors-outlook-connector-client-reference]

### Availability and prerequisites [es-connectors-outlook-client-availability-and-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md).


### Create a Outlook connector [es-connectors-outlook-create-connector-client]


#### Use the UI [es-connectors-outlook-client-create-use-the-ui]

To create a new Outlook connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Outlook** self-managed connector.


#### Use the API [es-connectors-outlook-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Outlook self-managed connector.

For example:

```console
PUT _connector/my-outlook-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Outlook",
  "service_type": "outlook"
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


### Usage [es-connectors-outlook-client-usage]

To use this connector as a **self-managed connector**, use the **Outlook** tile from the connectors list OR **Customized connector** workflow.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Connecting to Outlook [es-connectors-outlook-client-connecting-to-outlook]

Outlook connector supports both cloud (Office365 Outlook) and on-premises (Exchange Server) platforms.


#### Connect to Exchange Server [es-connectors-outlook-client-connect-to-exchange-server]

In order to connect to Exchange server, the connector fetches Active Directory users with the help of `ldap3` python library.


#### Connect to Office365 Outlook (Outlook Cloud) [es-connectors-outlook-client-connect-to-office365-outlook-outlook-cloud]

To integrate with the Outlook connector using Azure, follow these steps to create and configure an Azure application:

1. Navigate to the [Azure Portal](https://portal.azure.com/) and log in using your credentials.
2. Click on **App registrations** to register a new application.
3. Navigate to the **Overview** tab. Make a note of the `Client ID` and `Tenant ID`.
4. Click on the **Certificates & secrets** tab and create a new client secret. Keep this secret handy.
5. Go to the **API permissions** tab.

    1. Click on "Add permissions".
    2. Choose "APIs my organization uses".
    3. Search for and select "Office 365 Exchange Online".
        * Add the `full_access_as_app` application permission.
    4. Search for and select "Microsoft Graph"
        * Add the `User.Read.All` application permission.


You can now use the Client ID, Tenant ID, and Client Secret you’ve noted to configure the Outlook connector.


### Configuration [es-connectors-outlook-client-configuration]

`data_source`
:   (required) Dropdown to determine Outlook platform type: `outlook_cloud` or `outlook_server`. Default value is `outlook_cloud`.

`tenant_id`
:   (required if data source is outlook_cloud) The Tenant ID for the Azure account hosting the Outlook instance.

`client_id`
:   (required if data source is outlook_cloud) The Client ID to authenticate with Outlook instance.

`client_secret`
:   (required if data source is outlook_cloud) The Client Secret value to authenticate with Outlook instance.

`exchange_server`
:   (required if data source is outlook_server) IP address to connect with Exchange server. Example: `127.0.0.1`

`active_directory_server`
:   (required if data source is outlook_server) IP address to fetch users from Exchange Active Directory to fetch data. Example: `127.0.0.1`

`username`
:   (required if data source is outlook_server) Username to authenticate with Exchange server.

`password`
:   (required if data source is outlook_server) Password to authenticate with Exchange server.

`domain`
:   (required if data source is outlook_server) Domain name for Exchange server users such as `gmail.com` or `exchange.local`.

`ssl_enabled`
:   Whether SSL verification will be enabled. Default value is `False`. **Note:** This configuration is applicable for `Outlook Server` only.

`ssl_ca`
:   (required if ssl is enabled) Content of SSL certificate. Example certificate:

    ```txt
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```


`use_text_extraction_service`
:   Use [self-hosted content extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-data-extraction-service). Default value is `False`.

`document_level_security`
:   Toggle to enable [Document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled:

    * Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
    * Access control syncs fetch users' access control lists and store them in a separate index.


**Note:** This configuration is applicable for `Outlook Server` only.


### Deployment using Docker [es-connectors-outlook-client-client-docker]

You can deploy the Outlook connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: outlook
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



### Content Extraction [es-connectors-outlook-client-content-extraction]

Refer to [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Documents and syncs [es-connectors-outlook-client-documents-and-syncs]

The connector syncs the following objects and entities:

* **Mails**

    * **Inbox Mails**
    * **Sent Mails**
    * **Archive Mails**
    * **Junk Mails**

* **Contacts**
* **Calendar Events**
* **Tasks**
* **Attachments**

    * **Mail Attachments**
    * **Task Attachments**
    * **Calendar Attachments**


::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-outlook-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Document level security [es-connectors-outlook-client-document-level-security]

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. Refer to [configuration](#es-connectors-outlook-client-configuration) on this page for how to enable DLS for this connector.

::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Sync rules [es-connectors-outlook-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


### Advanced Sync Rules [es-connectors-outlook-client-advanced-sync-rules]

Advanced sync rules are not available for this connector in the present version.


### Connector Client operations [es-connectors-outlook-client-connector-client-operations]


#### End-to-end Testing [es-connectors-outlook-client-end-to-end-testing]

**Note:** End-to-end testing is not available in the current version of the connector.


#### Known issues [es-connectors-outlook-client-known-issues]

There are currently no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-outlook-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-outlook-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Framework and source [es-connectors-outlook-client-source]

This connector is included in the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/outlook.py) (branch *main*, compatible with Elastic *9.0*).

