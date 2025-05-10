---
navigation_title: "Microsoft SQL"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-ms-sql.html
---

# Elastic Microsoft SQL connector reference [es-connectors-ms-sql]


The *Elastic Microsoft SQL connector* is a [connector](/reference/search-connectors/index.md) for [Microsoft SQL](https://learn.microsoft.com/en-us/sql/) databases. This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/mssql.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-ms-sql-connector-client-reference]

### Availability and prerequisites [es-connectors-ms-sql-client-availability-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a Microsoft SQL connector [es-connectors-{{service_type}}-create-connector-client]


#### Use the UI [es-connectors-mssql-client-create-use-the-ui]

To create a new Microsoft SQL connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Microsoft SQL** self-managed connector.


#### Use the API [es-connectors-mssql-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Microsoft SQL self-managed connector.

For example:

```console
PUT _connector/my-mssql-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Microsoft SQL",
  "service_type": "mssql"
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


### Usage [es-connectors-ms-sql-client-usage]

Users require the `sysadmin` server role. Note that SQL Server Authentication is required. Windows Authentication is not supported.

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Compatibility [es-connectors-ms-sql-client-compatability]

The following are compatible with Elastic connector frameworks:

* Microsoft SQL Server versions 2017, 2019
* Azure SQL
* Amazon RDS for SQL Server


### Configuration [es-connectors-ms-sql-client-configuration]




The following configuration fields are required to set up the connector:

`host`
:   The server host address where the Microsoft SQL Server is hosted. Default value is `127.0.0.1`. Examples:

    * `192.158.1.38`
    * `demo.instance.demo-region.demo.service.com`


`port`
:   The port where the Microsoft SQL Server is hosted. Default value is `9090`.

`username`
:   The username of the account for Microsoft SQL Server. (SQL Server Authentication only)

`password`
:   The password of the account to be used for the Microsoft SQL Server. (SQL Server Authentication only)

`database`
:   Name of the Microsoft SQL Server database. Examples:

    * `employee_database`
    * `customer_database`


`tables`
:   Comma-separated list of tables. The Microsoft SQL connector will fetch data from all tables present in the configured database, if the value is `*` . Default value is `*`. Examples:

    * `table_1, table_2`
    * `*`

        ::::{warning}
        This field can be bypassed by advanced sync rules.

        ::::


`fetch_size`
:   Rows fetched per request.

`retry_count`
:   The number of retry attempts per failed request.

`schema`
:   Name of the Microsoft SQL Server schema. Default value is `dbo`.

    Examples:

    * `dbo`
    * `custom_schema`


`ssl_enabled`
:   SSL verification enablement. Default value is `False`.

`ssl_ca`
:   Content of SSL certificate. If SSL is disabled, the `ssl_ca` value will be ignored.

    ::::{dropdown} Expand to see an example certificate
    ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```

    ::::


`validate_host`
:   Host validation enablement. Default value is `False`.


### Deployment using Docker [es-connectors-ms-sql-client-docker]

You can deploy the Microsoft SQL connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: mssql
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



### Documents and syncs [es-connectors-ms-sql-client-documents-syncs]

* Tables with no primary key defined are skipped.
* If the `last_user_update` of `sys.dm_db_index_usage_stats` table is not available for a specific table and database then all data in that table will be synced.

::::{note}
* Files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



### Sync rules [es-connectors-ms-sql-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default. For more information read [sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


#### Advanced sync rules [es-connectors-ms-sql-client-sync-rules-advanced]

This connector supports [advanced sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for remote filtering. These rules cover complex query-and-filter scenarios that cannot be expressed with basic sync rules. Advanced sync rules are defined through a source-specific DSL JSON snippet.

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


Here are a few examples of advanced sync rules for this connector.

::::{dropdown} Expand to see example data
**`employee` table**

| emp_id | name | age |
| --- | --- | --- |
| 3 | John | 28 |
| 10 | Jane | 35 |
| 14 | Alex | 22 |

* **`customer` table**

| c_id | name | age |
| --- | --- | --- |
| 2 | Elm | 24 |
| 6 | Pine | 30 |
| 9 | Oak | 34 |

::::


$$$es-connectors-ms-sql-client-sync-rules-advanced-queries$$$
**Example: Two queries**

These rules fetch all records from both the `employee` and `customer` tables. The data from these tables will be synced separately to Elasticsearch.

```js
[
  {
    "tables": [
      "employee"
    ],
    "query": "SELECT * FROM employee"
  },
  {
    "tables": [
      "customer"
    ],
    "query": "SELECT * FROM customer"
  }
]
```
% NOTCONSOLE

$$$es-connectors-ms-sql-client-sync-rules-example-one-where$$$
**Example: One WHERE query**

This rule fetches only the records from the `employee` table where the `emp_id` is greater than 5. Only these filtered records will be synced to Elasticsearch.

```js
[
  {
    "tables": ["employee"],
    "query": "SELECT * FROM employee WHERE emp_id > 5"
  }
]
```
% NOTCONSOLE

$$$es-connectors-ms-sql-client-sync-rules-example-one-join$$$
**Example: One JOIN query**

This rule fetches records by performing an INNER JOIN between the `employee` and `customer` tables on the condition that the `emp_id` in `employee` matches the `c_id` in `customer`. The result of this combined data will be synced to Elasticsearch.

```js
[
  {
    "tables": ["employee", "customer"],
    "query": "SELECT * FROM employee INNER JOIN customer ON employee.emp_id = customer.c_id"
  }
]
```
% NOTCONSOLE

::::{warning}
When using advanced rules, a query can bypass the configuration field `tables`. This will happen if the query specifies a table that doesn’t appear in the configuration. This can also happen if the configuration specifies `*` to fetch all tables while the advanced sync rule requests for only a subset of tables.

::::



### End-to-end testing [es-connectors-ms-sql-client-client-operations-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Microsoft SQL connector, run the following command:

```shell
make ftest NAME=mssql
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=mssql DATA_SIZE=small
```


### Known issues [es-connectors-ms-sql-client-known-issues]

There are no known issues for this connector. See [Known issues](/release-notes/known-issues.md) for any issues affecting all connectors.


### Troubleshooting [es-connectors-ms-sql-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-ms-sql-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

This connector uses the [generic database connector source code](https://github.com/elastic/connectors-python/blob/master/connectors/sources/generic_database.py) (branch *main*, compatible with Elastic *9.0*).

View [additional code specific to this data source](https://github.com/elastic/connectors/tree/main/connectors/sources/mssql.py) (branch *main*, compatible with Elastic *9.0*).