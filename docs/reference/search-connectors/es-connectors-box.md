---
navigation_title: "Box"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-box.html
---

# Elastic Box connector reference [es-connectors-box]


Th Box connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/box.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-box-connector-client-reference]

### Availability and prerequisites [es-connectors-box-client-availability-and-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md).

::::{note}
This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::



### Create a Box connector [es-connectors-box-client-create-connector-client]


#### Use the UI [es-connectors-box-client-create-use-the-ui]

To create a new Box connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Box** self-managed connector.


#### Use the API [es-connectors-box-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Box self-managed connector.

For example:

```console
PUT _connector/my-box-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Box",
  "service_type": "box"
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


### Usage [es-connectors-box-client-usage]

To use this connector as a **self-managed connector**, use the **Box** tile from the connectors list OR **Customized connector** workflow.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Box API Authorization [es-connectors-box-client-api-authorization]


#### Box Free Account [es-connectors-box-client-free-account]

$$$es-connectors-box-client-create-oauth-custom-app$$$
**Create Box User Authentication (OAuth 2.0) Custom App**

You’ll need to create an OAuth app in the Box developer console by following these steps:

1. Register a new app in the [Box dev console](https://app.box.com/developers/console) with custom App and select User authentication (OAuth 2.0).
2. Add the URL of the web page in **Redirect URIs**, which is accessible by you.
3. Check "Write all files and folders stored in Box" in Application Scopes.
4. Once the app is created, **Client ID** and **Client secret** values are available in the configuration tab. Keep these handy.

$$$es-connectors-box-client-connector-generate-a-refresh-token$$$
**Generate a refresh Token**

To generate a refresh token, follow these steps:

1. Go to the following URL, replacing `<CLIENT_ID>` with the **Client ID** value saved earlier. For example:

    ```bash
    https://account.box.com/api/oauth2/authorize?response_type=code&client_id=<CLIENT_ID>
    ```

2. Grant access to your application.
3. You will now be redirected to the web page that you configured in **Redirect URIs**, and the HTTP response should contain an **authorization code** that you’ll use to generate a refresh token. **Note:** Authorization codes to generate refresh tokens can only be used once and are only valid for 30 seconds.
4. In your terminal, run the following `curl` command, replacing `<AUTHORIZATION_CODE>`, `<CLIENT_ID> and <CLIENT_SECRET>` with the values you saved earlier:

    ```bash
    curl -i -X POST "https://api.box.com/oauth2/token" \
         -H "Content-Type: application/x-www-form-urlencoded" \
         -d "client_id=<CLIENT_ID>" \
         -d "client_secret=<CLIENT_SECRET>" \
         -d "code=<AUTHORIZATION_CODE>" \
         -d "grant_type=authorization_code"
    ```

    Save the refresh token from the response. You’ll need this for the connector configuration.



#### Box Enterprise Account [es-connectors-box-client-enterprise-account]

$$$es-connectors-box-client-connector-create-box-server-authentication-client-credentials-grant-custom-app$$$
**Create Box Server Authentication (Client Credentials Grant) Custom App**

1. Register a new app in the [Box dev console](https://app.box.com/developers/console) with custom App and select Server Authentication (Client Credentials Grant).
2. Check following permissions:

    * "Write all files and folders stored in Box" in Application Scopes
    * "Make API calls using the as-user header" in Advanced Features

3. Select `App + Enterprise Access` in App Access Level.
4. Authorize your application from the admin console. If you do not have permission, you may need to submit the application for authorization. Save the **Client Credentials** and **Enterprise ID**. You’ll need these to configure the connector.


### Configuration [es-connectors-box-client-configuration]

`Box Account`  (required)
:   Dropdown to determine Box Account type: `Box Free Account` or `Box Enterprise Account`. Default value is `Box Free Account`.

`Client ID`  (required)
:   The Client ID to authenticate with Box instance.

`Client Secret`  (required)
:   The Client Secret to authenticate with Box instance.

`Refresh Token`  (required if Box Account is Box Free)
:   The Refresh Token to generate Access Token. **NOTE:** If the process terminates, you’ll need to generate a new refresh token.

`Enterprise ID`  (required if Box Account is Box Enterprise)
:   The Enterprise ID to authenticate with Box instance.


#### Deployment using Docker [es-connectors-box-client-client-docker]

You can deploy the Box connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: box
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



### Content Extraction [es-connectors-box-client-content-extraction]

Refer to [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Documents and syncs [es-connectors-box-client-documents-and-syncs]

The connector syncs the following objects and entities:

* **Files**
* **Folders**

::::{note}
* Files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-box-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-box-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


### Advanced Sync Rules [es-connectors-box-client-advanced-sync-rules]

Advanced sync rules are not available for this connector in the present version.


### End-to-end Testing [es-connectors-box-client-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Box connector, run the following command:

```shell
$ make ftest NAME=box
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=box DATA_SIZE=small
```


### Known issues [es-connectors-box-client-known-issues]

There are no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-box-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-box-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

