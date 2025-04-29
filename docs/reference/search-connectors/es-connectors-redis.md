---
navigation_title: "Redis"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-redis.html
---

# Redis connector reference [es-connectors-redis]


The Redis connector is built with the Elastic connectors Python framework and is available as a self-managed [self-managed connector](/reference/search-connectors/self-managed-connectors.md). View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/redis.py) (branch *main*, compatible with Elastic *9.0*).


## Availability and prerequisites [es-connectors-redis-connector-availability-and-prerequisites]

This connector was introduced in Elastic **8.13.0**, available as a **self-managed** self-managed connector.

To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md). Importantly, you must deploy the connectors service on your own infrastructure. You have two deployment options:

* [Run connectors service from source](/reference/search-connectors/es-connectors-run-from-source.md). Use this option if you’re comfortable working with Python and want to iterate quickly locally.
* [Run connectors service in Docker](/reference/search-connectors/es-connectors-run-from-docker.md). Use this option if you want to deploy the connectors to a server, or use a container orchestration platform.

::::{note}
This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

::::



## Usage [es-connectors-redis-connector-usage]

To set up this connector in the UI, select the **Redis** tile when creating a new connector under **Search → Connectors**.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


## Deploy with Docker [es-connectors-redis-connector-docker]

You can deploy the Redis connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: redis
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



## Configuration [es-connectors-redis-connector-configuration]

`host` (required)
:   The IP of your Redis server/cloud. Example:

    * `127.0.0.1`
    * `redis-12345.us-east-1.ec2.cloud.redislabs.com`


`port` (required)
:   Port where the Redis server/cloud instance is hosted. Example:

    * `6379`


`username` (optional)
:   Username for your Redis server/cloud. Example:

    * `default`


`password` (optional)
:   Password for your Redis server/cloud instance. Example:

    * `changeme`


`database`  (required)
:   List of database index for your Redis server/cloud. * will fetch data from all databases. Example:

    * `0,1,2`
    * `*`

        ::::{note}
        This field is ignored when using advanced sync rules.

        ::::


`ssl_enabled`
:   Toggle to use SSL/TLS. Disabled by default.

`mutual_tls_enabled`
:   Toggle to use secure mutual SSL/TLS. Ensure that your Redis deployment supports mutual SSL/TLS connections. Disabled by default. Depends on `ssl_enabled`.

`tls_certfile`
:   Specifies the certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the Redis instance. Depends on `mutual_tls_enabled`.

`tls_keyfile`
:   Specifies the client private key. The value of the key is used to validate the connection in the Redis instance. Depends on `mutual_tls_enabled`.


## Documents and syncs [es-connectors-redis-connector-documents-and-syncs]

The connector syncs the following objects and entities:

* KEYS and VALUES of every database index

::::{note}
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to the relevant Elasticsearch index.

::::



## Sync rules [es-connectors-redis-connector-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


## Advanced Sync Rules [es-connectors-redis-connector-advanced-sync-rules]

[Advanced sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) are defined through a source-specific DSL JSON snippet.

Use advanced sync rules to filter data at the Redis source, without needing to index all data into Elasticsearch.

They take the following parameters:

1. `database`:  Specify the Redis database index as an integer value.
2. `key_pattern`: 2. `key_pattern`: Pattern for finding keys in Redis.
3. `type`: Key type for the Redis.

    Supported values:

    * `HASH`
    * `LIST`
    * `SET`
    * `STREAM`
    * `STRING`
    * `ZSET`


::::{note}
Provide at least one of the following: `key_pattern` or `type`, or both.

::::



### Advanced sync rules examples [es-connectors-redis-connector-advanced-sync-rules-examples]


#### Example 1 [es-connectors-redis-connector-advanced-sync-rules-example-1]

**Fetch database records where keys start with `alpha`**:

```js
[
  {
    "database": 0,
    "key_pattern": "alpha*"
  }
]
```
% NOTCONSOLE


#### Example 2 [es-connectors-redis-connector-advanced-sync-rules-example-2]

**Fetch database records with exact match by specifying the full key name:**

```js
[
  {
    "database": 0,
    "key_pattern": "alpha"
  }
]
```
% NOTCONSOLE


#### Example 3 [es-connectors-redis-connector-advanced-sync-rules-example-3]

**Fetch database records where keys start with `test1`, `test2` or `test3`:**

```js
[
  {
    "database": 0,
    "key_pattern": "test[123]"
  }
```
% NOTCONSOLE


#### Example 4 [es-connectors-redis-connector-advanced-sync-rules-example-4]

**Exclude database records where keys start with `test1`, `test2` or `test3`:**

```js
[
  {
    "database": 0,
    "key_pattern": "test[^123]"
  }
]
```
% NOTCONSOLE


#### Example 5 [es-connectors-redis-connector-advanced-sync-rules-example-5]

**Fetch all database records:**

```js
[
  {
    "database": 0,
    "key_pattern": "*"
  }
]
```
% NOTCONSOLE


#### Example 6 [es-connectors-redis-connector-advanced-sync-rules-example-6]

**Fetch all database records where type is `SET`:**

```js
[
  {
    "database": 0,
    "key_pattern": "*",
    "type": "SET"
  }
]
```
% NOTCONSOLE


#### Example 7 [es-connectors-redis-connector-advanced-sync-rules-example-7]

**Fetch database records where type is `SET`**:

```js
[
  {
    "database": 0,
    "type": "SET"
  }
]
```
% NOTCONSOLE


## Connector Client operations [es-connectors-redis-connector-connector-client-operations]


### End-to-end Testing [es-connectors-redis-connector-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source, using Docker Compose. You don’t need a running Elasticsearch instance or Redis source to run this test.

Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Redis connector, run the following command:

```shell
$ make ftest NAME=redis
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=redis DATA_SIZE=small
```

By default, `DATA_SIZE=MEDIUM`.


## Known issues [es-connectors-redis-connector-known-issues]

* The last modified time is unavailable when retrieving keys/values from the Redis database. As a result, **all objects** are indexed each time an advanced sync rule query is executed.

Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


## Troubleshooting [es-connectors-redis-connector-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


## Security [es-connectors-redis-connector-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

