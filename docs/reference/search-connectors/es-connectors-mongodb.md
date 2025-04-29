---
navigation_title: "MongoDB"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-mongodb.html
---

# Elastic MongoDB connector reference [es-connectors-mongodb]


The *Elastic MongoDB connector* is a [connector](/reference/search-connectors/index.md) for [MongoDB](https://www.mongodb.com) data sources. This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/mongodb.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-mongodb-connector-client-reference]

### Availability and prerequisites [es-connectors-mongodb-client-prerequisites]

This connector is also available as a **self-managed connector** from the **Elastic connector framework**. To use this connector as a self-managed connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Compatibility [es-connectors-mongodb-client-compatibility]

This connector is compatible with **MongoDB Atlas** and **MongoDB 3.6 and later**.

The data source and your Elastic deployment must be able to communicate with each other over a network.


### Configuration [es-connectors-mongodb-client-configuration]

The following configuration fields are required to set up the connector:

`host`
:   The URI of the MongoDB host. Examples:

    * `mongodb+srv://my_username:my_password@cluster0.mongodb.net/mydb?w=majority`
    * `mongodb://127.0.0.1:27017`


`user`
:   The MongoDB username the connector will use.

    The user must have access to the configured database and collection. You may want to create a dedicated, read-only user for each connector.


`password`
:   The MongoDB password the connector will use.

::::{note}
Anonymous authentication is supported for *testing purposes only*, but should not be used in production. Omit the username and password, to use default values.

::::


`database`
:   The MongoDB database to sync. The database must be accessible using the configured username and password.

`collection`
:   The MongoDB collection to sync. The collection must exist within the configured database. The collection must be accessible using the configured username and password.

`direct_connection`
:   Whether to use the [direct connection option for the MongoDB client](https://www.mongodb.com/docs/ruby-driver/current/reference/create-client/#direct-connection). Default value is `False`.

`ssl_enabled`
:   Whether to establish a secure connection to the MongoDB server using SSL/TLS encryption. Ensure that your MongoDB deployment supports SSL/TLS connections. **Enable** if your MongoDB cluster uses DNS SRV records (namely MongoDB Atlas users).

    Default value is `False`.


`ssl_ca`
:   Specifies the root certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the MongoDB instance.

::::{tip}
Atlas users can leave this blank because [Atlas uses a widely trusted root CA](https://www.mongodb.com/docs/atlas/reference/faq/security/#which-certificate-authority-signs-mongodb-atlas-tls-certificates-).

::::


`tls_insecure`
:   Skips various certificate validations (if SSL is enabled). Default value is `False`.

::::{note}
We strongly recommend leaving this option disabled in production environments.

::::



### Create a MongoDB connector [es-connectors-mongodb-create-connector-client]


#### Use the UI [es-connectors-mongodb-client-create-use-the-ui]

To create a new MongoDB connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **MongoDB** self-managed connector.


#### Use the API [es-connectors-mongodb-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed MongoDB self-managed connector.

For example:

```console
PUT _connector/my-mongodb-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from MongoDB",
  "service_type": "mongodb"
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


### Usage [es-connectors-mongodb-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Known issues [es-connectors-mongodb-client-known-issues]


#### SSL must be enabled for MongoDB Atlas [es-connectors-mongodb-client-known-issues-ssl-tls-812]

* A bug introduced in **8.12.0** causes the connector to fail to sync Mongo **Atlas** urls (`mongo+srv`) unless SSL/TLS is enabled.


#### Expressions and variables in aggregation pipelines [es-connectors-mongodb-client-known-issues-expressions-and-variables-in-aggregation-pipelines]

It’s not possible to use expressions like `new Date()` inside an aggregation pipeline. These expressions won’t be evaluated by the underlying MongoDB client, but will be passed as a string to the MongoDB instance. A possible workaround is to use [aggregation variables](https://www.mongodb.com/docs/manual/reference/aggregation-variables/).

Incorrect (`new Date()` will be interpreted as string):

```js
{
    "aggregate": {
        "pipeline": [
            {
                "$match": {
                  "expiresAt": {
                    "$gte": "new Date()"
                  }
                }
            }
        ]
    }
}
```
% NOTCONSOLE

Correct (usage of [$$NOW](https://www.mongodb.com/docs/manual/reference/aggregation-variables/#mongodb-variable-variable.NOW)):

```js
{
  "aggregate": {
    "pipeline": [
      {
        "$addFields": {
          "current_date": {
            "$toDate": "$NOW"
          }
        }
      },
      {
        "$match": {
          "$expr": {
            "$gte": [
              "$expiresAt",
              "$current_date"
            ]
          }
        }
      }
    ]
  }
}
```
% NOTCONSOLE


#### Connecting with self-signed or custom CA TLS Cert [es-connectors-mongodb-client-known-issues-tls-with-invalid-cert]

Currently, the MongoDB connector does not support working with self-signed or custom CA certs when connecting to your self-managed MongoDB host.

::::{warning}
The following workaround should not be used in production.

::::


This can be worked around in development environments, by appending certain query parameters to the configured host.

For example, if your host is `mongodb+srv://my.mongo.host.com`, appending `?tls=true&tlsAllowInvalidCertificates=true` will allow disabling TLS certificate verification.

The full host in this example will look like this:

`mongodb+srv://my.mongo.host.com/?tls=true&tlsAllowInvalidCertificates=true`


#### Docker image errors out for versions 8.12.0 and 8.12.1 [es-connectors-mongodb-known-issues-docker-image-fails]

A bug introduced in **8.12.0** causes the Connectors docker image to error out if run using MongoDB as its source. The command line will output the error `cannot import name 'coroutine' from 'asyncio'`. *** This issue is fixed in versions *8.12.2** and **8.13.0**. ** This bug does not affect Elastic managed connectors.

See [Known issues](/release-notes/known-issues.md) for any issues affecting all connectors.


### Troubleshooting [es-connectors-mongodb-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-mongodb-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Deployment using Docker [es-connectors-mongodb-client-docker]

You can deploy the MongoDB connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: mongodb
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



### Documents and syncs [es-connectors-mongodb-client-syncs]

The following describes the default syncing behavior for this connector. Use [sync rules](/reference/search-connectors/es-sync-rules.md) and [ingest pipelines](docs-content://solutions/search/ingest-for-search.md) to customize syncing for specific indices.

All documents in the configured MongoDB database and collection are extracted and transformed into documents in your Elasticsearch index.

* The connector creates one **Elasticsearch document** for each MongoDB document in the configured database and collection.
* For each document, the connector transforms each MongoDB field into an **Elasticsearch field**.
* For each field, Elasticsearch [dynamically determines the **data type**^](docs-content://manage-data/data-store/mapping/dynamic-mapping.md).

This results in Elasticsearch documents that closely match the original MongoDB documents.

The Elasticsearch mapping is created when the first document is created.

Each sync is a "full" sync. For each MongoDB document discovered:

* If it does not exist, the document is created in Elasticsearch.
* If it already exists in Elasticsearch, the Elasticsearch document is replaced and the version is incremented.
* If an existing Elasticsearch document no longer exists in the MongoDB collection, it is deleted from Elasticsearch.
* Embedded documents are stored as an `object` field in the parent document.

This is recursive, because embedded documents can themselves contain embedded documents.

::::{note}
* Files bigger than 10 MB won’t be extracted
* Permissions are not synced. All documents indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



### Sync rules [es-connectors-mongodb-client-sync-rules]

The following sections describe [Sync rules](/reference/search-connectors/es-sync-rules.md) for this connector.

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

[Advanced rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for MongoDB can be used to express either `find` queries or aggregation pipelines. They can also be used to tune options available when issuing these queries/pipelines.


#### `find` queries [es-connectors-mongodb-client-sync-rules-find]

::::{note}
You must create a [text index](https://www.mongodb.com/docs/current/core/indexes/index-types/index-text/) on the MongoDB collection in order to perform text searches.

::::


For `find` queries, the structure of this JSON DSL should look like:

```js
{
	"find":{
		"filter": {
			// find query goes here
		},
		"options":{
			// query options go here
		}
	}
}
```

For example:

```js
{
	"find": {
		"filter": {
			"$text": {
				"$search": "garden",
				"$caseSensitive": false
			}
		},
		"skip": 10,
		"limit": 1000
	}
}
```
% NOTCONSOLE

`find` queries also support additional options, for example the `projection` object:

```js
{
  "find": {
    "filter": {
      "languages": [
        "English"
      ],
      "runtime": {
        "$gt":90
      }
    },
    "projection":{
      "tomatoes": 1
    }
  }
}
```
% NOTCONSOLE

Where the available options are:

* `allow_disk_use` (true, false) — When set to true, the server can write temporary data to disk while executing the find operation. This option is only available on MongoDB server versions 4.4 and newer.
* `allow_partial_results` (true, false) — Allows the query to get partial results if some shards are down.
* `batch_size` (Integer) — The number of documents returned in each batch of results from MongoDB.
* `filter` (Object) — The filter criteria for the query.
* `limit` (Integer) — The max number of docs to return from the query.
* `max_time_ms` (Integer) — The maximum amount of time to allow the query to run, in milliseconds.
* `no_cursor_timeout` (true, false) — The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
* `projection` (Array, Object) — The fields to include or exclude from each doc in the result set. If an array, it should have at least one item.
* `return_key` (true, false) — Return index keys rather than the documents.
* `show_record_id` (true, false) — Return the `$recordId` for each doc in the result set.
* `skip` (Integer) — The number of docs to skip before returning results.


#### Aggregation pipelines [es-connectors-mongodb-client-sync-rules-aggregation]

Similarly, for aggregation pipelines, the structure of the JSON DSL should look like:

```js
{
	"aggregate":{
		"pipeline": [
			// pipeline elements go here
		],
		"options": {
            // pipeline options go here
		}
    }
}
```

Where the available options are:

* `allowDiskUse` (true, false) — Set to true if disk usage is allowed during the aggregation.
* `batchSize` (Integer) — The number of documents to return per batch.
* `bypassDocumentValidation` (true, false) — Whether or not to skip document level validation.
* `collation` (Object) — The collation to use.
* `comment` (String) — A user-provided comment to attach to this command.
* `hint` (String) — The index to use for the aggregation.
* `let` (Object) — Mapping of variables to use in the pipeline. See the server documentation for details.
* `maxTimeMs` (Integer) — The maximum amount of time in milliseconds to allow the aggregation to run.


### Migrating from the Ruby connector framework [es-connectors-mongodb-client-migration-from-ruby]

As part of the 8.8.0 release the MongoDB connector was moved from the [Ruby connectors framework](https://github.com/elastic/connectors/tree/main) to the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

This change introduces minor formatting modifications to data ingested from MongoDB:

1. Nested object id field name has changed from "_id" to "id". For example, if you had a field "customer._id", this will now be named "customer.id".
2. Date format has changed from `YYYY-MM-DD'T'HH:mm:ss.fff'Z'` to `YYYY-MM-DD'T'HH:mm:ss`

If your MongoDB connector stopped working after migrating from 8.7.x to 8.8.x, read the workaround outlined in [Known issues](/release-notes/known-issues.md). If that does not work, we recommend deleting the search index attached to this connector and re-creating a MongoDB connector from scratch.

