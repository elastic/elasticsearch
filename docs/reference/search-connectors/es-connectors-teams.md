---
applies_to:
  stack: preview 9.6
  serverless: preview
navigation_title: "Teams"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-teams.html
---

# Elastic Microsoft Teams connector reference [es-connectors-teams]


The Microsoft Teams connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/microsoft_teams) (branch *main*, compatible with Elastic *9.6*).

## **Self-managed connector reference** [es-connectors-teams-connector-client-reference]

### Availability and prerequisites [es-connectors-microsoft-teams-availability-and-prerequisites]

This connector is available as a self-managed connector.

This self-managed connector is compatible with Elastic versions **9.6.0+**.

To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md).

::::{note}
This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::


### Create a Microsoft Teams connector [es-connectors-teams-client-create-connector-client]


#### Use the UI [es-connectors-microsoft_teams-client-create-use-the-ui]

To create a new Microsoft Teams connector:

1. In the Kibana UI, search for "connectors" using the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects) and choose the "Elasticsearch" connectors.
2. Follow the instructions to create a new **Microsoft Teams** self-managed connector.


#### Use the API [es-connectors-microsoft_teams-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Microsoft Teams self-managed connector.

For example:

```console
PUT _connector/my-microsoft_teams-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Microsoft Teams",
  "service_type": "microsoft_teams"
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


### Usage [es-connectors-microsoft-teams-usage]

To use this connector as a **self-managed connector**, use the **Microsoft Teams** tile from the connectors list **Customized connector** workflow.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Connecting to Microsoft Teams [es-connectors-microsoft-teams-connecting-to-microsoft-teams]

The connector connects to Microsoft Teams using application credentials (client secret or certificate), the same approach as SharePoint Online and Outlook. You do not need to install a Teams app in every team or chat.

When [document level security (DLS)](/reference/search-connectors/document-level-security.md) is enabled, search results are restricted to users who belong to the relevant team, channel, or chat. The connector application itself can read content according to the permissions you grant below.

To connect to Microsoft Teams, [register an application in Microsoft Entra ID](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) and grant it the required Microsoft Graph application permissions. Follow these steps:

1. In the [Microsoft Entra admin center](https://entra.microsoft.com), register a new application (confidential client).
2. Record the **Directory (tenant) ID** and **Application (client) ID**.
3. Create either:
    * a **client secret** (Certificates & secrets → New client secret), or
    * a **certificate** (upload a certificate and keep the matching private key).
4. Under **API permissions**, add and grant **admin consent** for the following Microsoft Graph **application** permissions:

    * `Team.ReadBasic.All` — read teams in your organization
    * `TeamMember.Read.All` — read who belongs to each team
    * `User.ReadBasic.All` — read user profiles
    * `Channel.ReadBasic.All` — read channels
    * `ChannelMember.Read.All` — read who can access each channel
    * `ChannelMessage.Read.All` — read channel messages and replies
    * `Chat.Read.All` — read chats, messages, and replies
    * `Files.Read.All` — read file attachments (required when **Fetch attachment content** is enabled)

5. Click **Grant admin consent** to approve the permissions. This step requires administrative privileges. If you are not an admin, request that an admin grant consent via the Entra admin center.

::::{warning}
The connector requires **application** permissions. It does not support delegated permissions (scopes) or username/password authentication.

::::


#### Protected APIs [es-connectors-microsoft-teams-protected-apis]

`ChannelMessage.Read.All` and `Chat.Read.All` are [protected Teams APIs](https://learn.microsoft.com/en-us/graph/teams-protected-apis). Admin consent alone may not be enough — some tenants also need Microsoft to approve access for your application before message content can be read.


### Configuration [es-connectors-microsoft-teams-configuration]

The following configuration fields are available:

`tenant_id` (required)
:   Unique identifier for your Microsoft Entra tenant. Example:

    * `123a1b23-12a3-45b6-7c8d-fc931cfb448d`


`client_id` (required)
:   Unique identifier for your Entra application. Example:

    * `ab123453-12a2-100a-1123-93fd09d67394`


`auth_method` (required)
:   Authentication method to use with Microsoft Graph. Options:

    * `secret` (default) — authenticate with a client secret.
    * `certificate` — authenticate with a certificate and private key.


`secret_value`
:   (required if `auth_method` is `secret`) Client secret value from the **Certificates & secrets** tab of your Entra application. Example:

    * `eyav1~12aBadIg6SL-STDfg102eBfCGkbKBq_Ddyu`


`certificate`
:   (required if `auth_method` is `certificate`) Content of the certificate file uploaded to your Entra application.


`private_key`
:   (required if `auth_method` is `certificate`) Content of the private key file that matches the uploaded certificate.


`fetch_attachment_content`
:   Toggle to index files from channel folders and message attachments, and extract their content. Requires the `Files.Read.All` application permission. Default value is `true`.


`use_text_extraction_service`
:   Toggle to enable the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) for file content. Requires that ingest pipeline settings disable text extraction. Default value is `false`.


`use_document_level_security`
:   Toggle to enable [document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled:

    * Full syncs fetch access control lists for each document and store them in the `_allow_access_control` field.
    * Access control syncs fetch users' access control lists and store them in a separate index.

    Default value is `false`.


#### Deployment using Docker [es-connectors-microsoft-teams-client-docker]

You can deploy the Microsoft Teams connector as a self-managed connector using Docker. Follow these instructions.

::::{dropdown} Step 1: Download sample configuration file
Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/app/connectors_service/config.yml.example --output ~/connectors-config/config.yml
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
    service_type: microsoft_teams
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

Using the `elasticsearch.api_key` is the recommended authentication method. However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

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

::::


Refer to [`DOCKER.md`](https://github.com/elastic/connectors/tree/main/docs/DOCKER.md) in the `elastic/connectors` repo for more details.

Find all available Docker images in the [official registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).

::::{tip}
We also have a quickstart self-managed option using Docker Compose, so you can spin up all required services at once: Elasticsearch, Kibana, and the connectors service. Refer to this [README](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.

::::



#### Content Extraction [es-connectors-microsoft-teams-content-extraction]

Refer to [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Documents and syncs [es-connectors-microsoft-teams-documents-and-syncs]

The connector syncs the following content from Microsoft Teams:

* **Teams**
* **Channels**
* **Channel messages** (including thread replies)
* **Chats** (one-on-one, group, and meeting chats), including messages and thread replies
* **Users** from your Microsoft Entra directory
* **Files** (when **Fetch attachment content** is enabled): files from channel folders and message attachments

Private and shared channels are included. Each file is indexed once, even when it appears in a channel folder and as a message attachment.

::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. Use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced by default. Enable [document-level security (DLS)](/reference/search-connectors/document-level-security.md) to sync permissions. Otherwise, **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
* If the connector is missing required permissions, the sync fails rather than indexing an incomplete set of content.

::::


#### Upgrading from earlier connector versions [es-connectors-microsoft-teams-upgrade]

::::{important}
Applies when upgrading to **9.6.0+** from a Microsoft Teams connector that used username/password or delegated Graph authentication.

If you already have a legacy Microsoft Teams connector or index, **prefer deleting the old connector and index** and creating a new connector with a fresh index. That avoids leftover documents from the previous connector (calendars, tabs, meeting recordings, and other content types that are no longer synced).

If you upgrade in place, the first successful full sync removes documents the new connector no longer produces, including:

* calendars
* channel and chat tabs
* meeting recordings
* legacy team member records (replaced by user profiles)

After upgrading, reconfigure the connector for application-only authentication (client secret or certificate). Username and password fields are no longer supported.

Calendars, meeting recordings, and tabs are no longer synced by this connector. Use the [Outlook connector](/reference/search-connectors/es-connectors-outlook.md) for calendars and the [SharePoint Online connector](/reference/search-connectors/es-connectors-sharepoint-online.md) for SharePoint-hosted meeting recordings.

::::


#### Sync types [es-connectors-microsoft-teams-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector does not support [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Document level security [es-connectors-microsoft-teams-document-level-security]

Document level security (DLS) enables you to restrict access to documents based on a user's permissions. Refer to [configuration](#es-connectors-microsoft-teams-configuration) on this page for how to enable DLS for this connector.

When DLS is enabled, users only see Teams, channels, chats, messages, and files they have access to in Microsoft Teams. User profile documents from your directory are not restricted by DLS.

::::{tip}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Sync rules [es-connectors-microsoft-teams-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


### Advanced Sync Rules [es-connectors-microsoft-teams-advanced-sync-rules]

Advanced sync rules are not available for this connector in the present version.


### End-to-end Testing [es-connectors-microsoft-teams-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Teams connector, run the following command:

```shell
$ make ftest NAME=microsoft_teams
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=microsoft_teams DATA_SIZE=small
```


### Known issues [es-connectors-microsoft-teams-known-issues]

* Messages in one-on-one chats for *Chat with Self* users are not fetched via Graph APIs. Therefore, these messages won’t be indexed into Elasticsearch.

Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-microsoft-teams-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-microsoft-teams-security]

See [Security](/reference/search-connectors/es-connectors-security.md).
