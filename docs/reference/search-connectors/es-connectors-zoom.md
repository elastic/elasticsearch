---
navigation_title: "Zoom"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-zoom.html
---

# Elastic Zoom connector reference [es-connectors-zoom]


The Zoom connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/zoom.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-zoom-connector-client-reference]

### Availability and prerequisites [es-connectors-zoom-client-connector-availability-and-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md).

::::{note}
This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::



### Create a Zoom connector [es-connectors-zoom-client-create-connector-client]


#### Use the UI [es-connectors-zoom-client-create-use-the-ui]

To create a new Zoom connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Zoom** self-managed connector.


#### Use the API [es-connectors-zoom-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Zoom self-managed connector.

For example:

```console
PUT _connector/my-zoom-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Zoom",
  "service_type": "zoom"
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


### Usage [es-connectors-zoom-client-connector-usage]

To use this connector in the UI, select the **Teams** tile when creating a new connector under **Search → Connectors**.

If you’re already familiar with how connectors work, you can also use the [Connector APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector).

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Connecting to Zoom [es-connectors-zoom-client-connector-connecting-to-zoom]

To connect to Zoom you need to [create an Server-to-Server OAuth application](https://developers.zoom.us/docs/internal-apps/s2s-oauth/) that can access resources. Follow these steps:

1. Go to the [Zoom App Marketplace](https://marketplace.zoom.us/) and sign in with your Zoom account.
2. Navigate to the "Develop" service.
3. Select "Build App" from the dropdown menu.
4. Click on the "Server-to-Server OAuth" button to register a new application.
5. Provide a name for your app.
6. Click on the "Create" button to create the app registration.
7. After the registration is complete, you will be redirected to the app’s overview page. Take note of the "App Credentials" value, as you’ll need it later.
8. Navigate to the "Scopes" section and click on the "Add Scopes" button.
9. The following granular scopes need to be added to the app.

    ```bash
    user:read:list_users:admin
    meeting:read:list_meetings:admin
    meeting:read:list_past_participants:admin
    cloud_recording:read:list_user_recordings:admin
    team_chat:read:list_user_channels:admin
    team_chat:read:list_user_messages:admin
    ```


::::{admonition}
The connector requires a minimum scope of `user:read:list_users:admin` to ingest data into Elasticsearch.

::::


+ 10. Click on the "Done" button to add the selected scopes to your app. 11. Navigate to the "Activation" section and input the necessary information to activate the app.

After completion, use the following configuration parameters to configure the connector.


### Configuration [es-connectors-zoom-client-connector-configuration]

The following configuration fields are required:

`Zoom application Account ID`
:   (required) "Account ID" is a unique identifier associated with a specific Zoom account within the Zoom platform, found on the app’s overview page. Example:

    * `KVx-aQssTOutOAGrDfgMaA`


`Zoom application Client ID`
:   (required) "Client ID" refers to a unique identifier associated with an application that integrates with the Zoom platform, found on the app’s overview page. Example:

    * `49Z69_rnRiaF4JYyfHusw`


`Zoom application Client Secret`
:   (required) The "Client Secret" refers to a confidential piece of information generated when developers register an application on the Zoom Developer Portal for integration with the Zoom platform, found on the app’s overview page. Example:

    * `eieiUJRsiH543P5NbYadavczjkqgdRTw`


`Recording Age Limit (Months)`
:   (required) How far back in time to request recordings from Zoom. Recordings older than this will not be indexed. This configuration parameter allows you to define a time limit, measured in months, for which recordings will be indexed.

`Fetch past meeting details`
:   Retrieve more information about previous meetings, including their details and participants. Default value is `False`. Enable this option to fetch past meeting details. This setting can increase sync time.


#### Deployment using Docker [es-connectors-zoom-client-client-docker]

You can deploy the Zoom connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: zoom
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



#### Content Extraction [es-connectors-zoom-client-connector-content-extraction]

Refer to [content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Documents and syncs [es-connectors-zoom-client-connector-documents-and-syncs]

The connector syncs the following objects and entities:

* **Users**
* **Live Meetings**
* **Upcoming Meetings**
* **Past Meetings**
* **Recordings**
* **Channels**
* **Chat Messages**
* **Chat Files**

::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-zoom-client-connector-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-zoom-client-connector-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


### Advanced Sync Rules [es-connectors-zoom-client-connector-advanced-sync-rules]

Advanced sync rules are not available for this connector in the present version.


### Connector Client operations [es-connectors-zoom-client-connector-connector-client-operations]


#### End-to-end Testing [es-connectors-zoom-client-connector-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Zoom connector, run the following command:

```shell
$ make ftest NAME=zoom
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=zoom DATA_SIZE=small
```

### Known issues [es-connectors-zoom-client-connector-known-issues]

* **Meetings**: Users can only index meetings that are less than a month old.
* **Chat Messages & Files**:Users can only index chats and files that are less than 6 months old.

Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for *all* connectors.


### Troubleshooting [es-connectors-zoom-client-connector-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-zoom-client-connector-security]

See [Security](/reference/search-connectors/es-connectors-security.md).