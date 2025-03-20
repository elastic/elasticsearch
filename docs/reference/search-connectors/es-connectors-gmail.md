---
navigation_title: "Gmail"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-gmail.html
---

# Elastic Gmail connector reference [es-connectors-gmail]


The *Elastic GMail connector* is a [connector](/reference/search-connectors/index.md) for GMail.

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-gmail-connector-client-reference]

### Availability and prerequisites [es-connectors-gmail-client-availability]

This connector is available as a self-managed connector from the **Elastic connector framework**.

This self-managed connector is compatible with Elastic versions **8.10.0+**.

To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a Gmail connector [es-connectors-gmail-create-connector-client]


#### Use the UI [es-connectors-gmail-client-create-use-the-ui]

To create a new Gmail connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Gmail** self-managed connector.


#### Use the API [es-connectors-gmail-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Gmail self-managed connector.

For example:

```console
PUT _connector/my-gmail-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Gmail",
  "service_type": "gmail"
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


### Usage [es-connectors-gmail-client-usage]

To use this connector as a **self-managed connector**, use the **Connector** workflow in the Kibana UI.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Connector authentication prerequisites [es-connectors-gmail-client-connector-authentication-prerequisites]

Before syncing any data from GMail, you need to create a [service account](https://cloud.google.com/iam/docs/service-account-overview) with appropriate access to the GMail and the Google Directory API, which is part of the Google Admin SDK API. You also need to enable domain-wide delegation to impersonate the users you’re fetching messages from.

To get started, log into [Google Cloud Platform](https://cloud.google.com) and go to the `Console`.

1. **Create a Google Cloud Project.** Give your project a name, change the project ID and click the Create button.
2. **Enable Google APIs.** Choose APIs & Services from the left menu and click on `Enable APIs and Services`. You need to enable **GMail API** and the **Google Admin SDK API**.
3. **Create a Service Account.** In the `APIs & Services` section, click on `Credentials` and click on `Create credentials` to create a service account. Give your service account a name and a service account ID. This is like an email address and will be used to identify your service account in the future. Click `Done` to finish creating the service account.

    Your service account needs to have access to at least the following scope:

    * `https://www.googleapis.com/auth/gmail.readonly`

4. **Create a Key File**.

    * In the Cloud Console, go to `IAM and Admin` > `Service accounts` page.
    * Click the email address of the service account that you want to create a key for.
    * Click the `Keys` tab. Click the `Add key` drop-down menu, then select `Create new key`.
    * Select JSON as the Key type and then click `Create`. This will download a JSON file that will contain the service account credentials.

5. **Google Workspace domain-wide delegation of authority**.

    To access user data like messages on a Google Workspace domain, the service account that you created needs to be granted access by a super administrator for the domain. You can follow [the official documentation](https://developers.google.com/cloud-search/docs/guides/delegation) to perform Google Workspace domain-wide delegation of authority.

    You need to grant the following **OAuth Scopes** to your service account:

    * `https://www.googleapis.com/auth/admin.directory.user.readonly`

    This step allows the connector to access user data and their group memberships in your Google Workspace organization.



### Configuration [es-connectors-gmail-client-configuration]




The following configuration fields are required:

`GMail service account JSON`
:   The service account credentials generated from Google Cloud Platform (JSON string). Refer to the [Google Cloud documentation](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) for more information.

`Google Workspace admin email`
:   Google Workspace admin email. Required to enable document level security (DLS). A service account with delegated authority can impersonate an admin user with permissions to access Google Workspace user data and their group memberships. Refer to the [Google Cloud documentation](https://support.google.com/a/answer/162106?hl=en) for more information.

`Google customer id`
:   Google customer id. Required to fetch messages and to enable document level security (DLS). Go to `Google Workspace Admin Console` → `Account` and copy the value under `Customer Id`.

`Include spam and trash emails`
:   Toggle to fetch spam and trash emails. Also works with DLS.

`Enable document level security`
:   Toggle to enable [document level security (DLS](/reference/search-connectors/document-level-security.md). DLS is supported for the GMail connector. When enabled:

    * Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
    * Access control syncs will fetch users' access control lists and store them in a separate index.



### Deployment using Docker [es-connectors-gmail-client-deployment-using-docker]

You can deploy the Gmail connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: gmail
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



### Documents and syncs [es-connectors-gmail-client-documents-and-syncs]

The connector will fetch all messages of all users the service account has access to.


#### Sync types [es-connectors-gmail-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-gmail-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

Advanced sync rules are available for this connector. The connector supports the [GMail advanced search syntax](https://support.google.com/mail/answer/7190) under the `messages` field.

For example:

```js
{
  "messages": [
    "before:2021/10/10",
    "from:amy"
  ]
}
```


### Document level security [es-connectors-gmail-client-document-level-security]

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. Refer to [configuration](#es-connectors-gmail-client-configuration) on this page for how to enable DLS for this connector.

::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Known issues [es-connectors-gmail-client-known-issues]

There are currently no known issues for this connector.


### Troubleshooting [es-connectors-gmail-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-gmail-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Framework and source [es-connectors-gmail-client-framework-and-source]

This connector is built in Python with the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/gmail.py) (branch *main*, compatible with Elastic *9.0*).