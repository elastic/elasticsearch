---
navigation_title: "GraphQL"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-graphql.html
---

# Elastic GraphQL connector reference [es-connectors-graphql]


The Elastic GraphQL connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main). View the [source code for this connector](https://github.com/elastic/connectors/blob/main/connectors/sources/graphql.py).


## Availability and prerequisites [es-connectors-graphql-connector-availability-and-prerequisites]

This connector was introduced in Elastic **8.14.0**, available as a **self-managed** self-managed connector.

To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md). Importantly, you must deploy the connectors service on your own infrastructure. You have two deployment options:

* [Run connectors service from source](/reference/search-connectors/es-connectors-run-from-source.md). Use this option if you’re comfortable working with Python and want to iterate quickly locally.
* [Run connectors service in Docker](/reference/search-connectors/es-connectors-run-from-docker.md). Use this option if you want to deploy the connectors to a server, or use a container orchestration platform.

::::{note}
This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::



## Usage [es-connectors-graphql-connector-usage]

To set up this connector in the UI, select the **GraphQL** tile when creating a new connector under **Search → Connectors**.

If you’re already familiar with how connectors work, you can also use the [Connector APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector).

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


## Deploy with Docker [es-connectors-graphql-connector-docker]

You can deploy the GraphQL connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: graphql
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



## Configuration [es-connectors-graphql-connector-configuration]


### Configure GraphQL connector [es-connectors-graphql-connector-configure-graphql-connector]

Note the following configuration fields:

`http_endpoint` (required)
:   Base URL of the GraphQL endpoint. **Example**: `https://api.xyz.com/graphql`

`http_method` (required)
:   `GET` or `POST`.

`authentication_method`(required)
:   Select from `No Auth`, `Basic Auth`, and `Bearer Token`.

`username`
:   Required when using basic authentication.

`password`
:   Required when using basic authentication.

`token`
:   Required when using bearer token authentication.

`graphql_query` (required)
:   Query used to fetch data from the source. Can contain variables provided in the `graphql_variables` field. The connector will substitute the variables in the query with values from `graphql_variables` and make a GraphQL query to the source.

    **Example**:

    ```js
    query getUser($id: ID!) {
        user(id: $id) {
            name
            email
        }
    }
    ```


`graphql_variables`
:   A JSON object of key/value pairs containing variables used in the GraphQL query. The connector will substitute the variables in the query with the values provided here and make a GraphQL query to the source.

    **Example**:

    For the GraphQL query `query getUser($id: ID!) { user(id: $id) { name } }`

    * Where the value of `graphql_variables` is `{"id": "123"}`
    * The connector will execute `query getUser { user(id: "123") { name } }` to fetch data from the source


`graphql_object_to_id_map` (required)
:   A JSON mapping between GraphQL response objects to index and their ID fields. The connector will fetch data for each object (JSON key) and use the provided ID field (JSON value) to index the object into Elasticsearch. The connector will index all fields for each object specified in the mapping. Use dot `(.)` notation to specify the full path from the root of the GraphQL response to the desired object.

    **Example**:

    The GraphQL query `query getUser { organization { users{ user_id name email} } }` fetches all available users from the source. To index every user as a separate document configure this field as below.

    ```js
    {
        "organization.users": "user_id"
    }
    ```

    In this example `user_id` is unique in every user document. Therefore, we set `user_id` as the value for `organization.users`.

    ::::{note}
    The path provided in this field should only contain JSON objects and not lists.

    ::::


`headers`
:   JSON object containing custom headers to be sent with each GraphQL request:

    ```js
    {
        "content-type": "Application/json"
    }
    ```


`pagination_model` (required)
:   This field specifies the pagination model to be used by the connector. The connector supports `No pagination` and `Cursor-based pagination` pagination models.

    For cursor-based pagination, add `pageInfo {endCursor hasNextPage}` and an `after` argument variable in your query at the desired node (`Pagination key`). Use the `after` query argument with a variable to iterate through pages. The default value for this field is `No pagination`. Example:

    For `Cursor-based pagination`, the query should look like this example:

    ```js
    query getUsers($cursor: String!) {
        sampleData {
            users(after: $cursor) {
                pageInfo {
                    endCursor
                    hasNextPage
                }
                nodes {
                    first_name
                    last_name
                    address
                }
            }
        }
    }
    ```

    The value of `pagination_key` is `sampleData.users` so it must contain:

    * `pageInfo {endCursor hasNextPage}`
    * the `after` argument with a variable when using cursor-based pagination


`pagination_key` (required)
:   Specifies which GraphQL object is used for pagination. Use `.` to provide the full path of the object from the root of the response.

    **Example**:

    * `organization.users`


`connection_timeout`
:   Specifies the maximum time in seconds to wait for a response from the GraphQL source. Default value is **30 seconds**.


## Documents and syncs [es-connectors-graphql-connector-documents-and-syncs]

The connector syncs the objects and entities based on GraphQL Query and GraphQL Object List.


## Sync types [es-connectors-graphql-connector-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector currently does not support [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


## Sync rules [es-connectors-graphql-connector-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


## Advanced Sync Rules [es-connectors-graphql-connector-advanced-sync-rules]

Advanced sync rules are not available for this connector in the present version.


## Connector Client operations [es-connectors-graphql-connector-connector-client-operations]


### End-to-end Testing [es-connectors-graphql-connector-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source, using Docker Compose. You don’t need a running Elasticsearch instance or GraphQL source to run this test.

Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the GraphQL connector, run the following command:

```shell
$ make ftest NAME=graphql
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=graphql DATA_SIZE=small
```

By default, `DATA_SIZE=MEDIUM`.


## Known issues [es-connectors-graphql-connector-known-issues]

* Every document will be updated in every sync.
* If the same field name exists with different types across different objects, the connector might raise a mapping parser exception.

Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


## Troubleshooting [es-connectors-graphql-connector-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


## Security [es-connectors-graphql-connector-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

