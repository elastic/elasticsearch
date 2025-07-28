---
navigation_title: "Slack"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-slack.html
---

# Elastic Slack connector reference [es-connectors-slack]

The Slack connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/slack.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-slack-connector-client-reference]

### Availability and prerequisites [es-connectors-slack-client-availability]

This connector is available as a self-managed connector from the **Elastic connector framework**.

This self-managed connector is compatible with Elastic versions **8.10.0+**.

To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).

::::{note}
This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::



### Create a Slack connector [es-connectors-slack-client-create-connector-client]


#### Use the UI [es-connectors-slack-client-create-use-the-ui]

To create a new Slack connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Slack** self-managed connector.


#### Use the API [es-connectors-slack-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Slack self-managed connector.

For example:

```console
PUT _connector/my-slack-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Slack",
  "service_type": "slack"
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


### Usage [es-connectors-slack-client-usage]

To use this connector as a **self-managed connector**, use the **Connector** workflow in the Kibana UI.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).

::::{note}
You need to create a Slack application to authenticate with Slack.

::::



#### Create a Slack application [es-connectors-slack-client-app]

When created you’ll receive a credential that the connector uses for authentication. A new Bot user will also be created.

::::{tip}
The connector will only sync messages from the channels of which the Bot user is a member.

::::


To create the app, follow these steps:

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps) and click "Create New App".
2. Choose "From Scratch".
3. Name the app, and select the workspace you want to sync from. Depending on the workspace’s settings, you may get a warning about requiring admin approval. That will be handled later.
4. Navigate to "OAuth & Permissions" in the sidebar.
5. Scroll down to the "Scopes" section and add these scopes:

    * `channels:history`
    * `channels:read`
    * `users:read`.

        Optionally, you can also add `channels:join` if you want the App Bot to automatically be able to add itself to public channels.

6. Scroll up to "OAuth Tokens for Your Workspace" and install the application. Your workspace may require you to get administrator approval. If so, request approval now and return to the next step once it has been approved.
7. Copy and save the new "Bot User OAuth Token". This credential will be used when configuring the connector.


### Deploy with Docker [es-connectors-slack-client-docker]

You can deploy the Slack connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: slack
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



### Configuration [es-connectors-slack-client-configuration]

The following settings are required to set up this connector:

`token`(required)
:   The Bot User OAuth Token generated by creating and installing your Slack App.

`fetch_last_n_days`(required)
:   The number of days of history to fetch from Slack. This must be a positive number to fetch a subset of data, going back that many days. If set to `0`, it will fetch all data since the beginning of the workspace. The default is 180 days.

`auto_join_channels`(required)
:   Whether or not the connector should have the App’s Bot User automatically invite itself into all public channels. The connector will only sync messages from the channels of which the Bot user is a member. By default, the bot will not invite itself to any channels, and must be manually invited to each channel that you wish to sync. If this setting is enabled, your App must have the `channels.join` scope.

`sync_users`(required)
:   Whether or not the connector should index a document for each Slack user. By default, the connector will create documents only for Channels and Messages. However, regardless of the value of this setting, the Slack App does need the `users.read` scope and will make requests to enumerate all of the workspace’s users. This allows the messages to be enriched with human-readable usernames, and not rely on unreadable User UIDs. Therefore, disabling this setting does not result in a speed improvement, but merely results in less overall storage in Elasticsearch.


### Sync rules [es-connectors-slack-client-sync-rules]

*Basic* sync rules are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version.

For more information read [Types of sync rule](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


### Content Extraction [es-connectors-slack-client-content-extraction]

This connector does not currently support processing Slack attachments or other binary files.


### Documents and syncs [es-connectors-slack-client-documents-syncs]

The connector syncs the following objects and entities:

* **Channels**
* **Messages**
* **Users** (configurably)

::::{note}
* Only public channels and messages from public channels are synced.
* No permissions are synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



### Self-managed connector operations [es-connectors-slack-client-connector-client-operations]


### End-to-end testing [es-connectors-slack-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the GitHub connector, run the following command:

```shell
$ make ftest NAME=slack
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=slack DATA_SIZE=small
```


### Known issues [es-connectors-slack-client-known-issues]

There are currently no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-slack-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-slack-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).