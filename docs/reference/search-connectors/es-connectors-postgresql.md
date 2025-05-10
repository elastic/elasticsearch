---
navigation_title: "PostgreSQL"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-postgresql.html
---

# Elastic PostgreSQL connector reference [es-connectors-postgresql]


The *Elastic PostgreSQL connector* is a connector for [PostgreSQL](https://www.postgresql.org). This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

This connector uses the [generic database connector source code](https://github.com/elastic/connectors/blob/master/connectors/sources/generic_database.py) (branch *main*, compatible with Elastic *9.0*). View the specific [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/postgresql.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-postgresql-connector-client-reference]

### Availability and prerequisites [es-connectors-postgresql-client-availability-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a PostgreSQL connector [es-connectors-postgresql-create-connector-client]


#### Use the UI [es-connectors-postgresql-client-create-use-the-ui]

To create a new PostgreSQL connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **PostgreSQL** self-managed connector.


#### Use the API [es-connectors-postgresql-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed PostgreSQL self-managed connector.

For example:

```console
PUT _connector/my-postgresql-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from PostgreSQL",
  "service_type": "postgresql"
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


### Usage [es-connectors-postgresql-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md).

::::{tip}
Users must set `track_commit_timestamp` to `on`. To do this, run `ALTER SYSTEM SET track_commit_timestamp = on;` in PostgreSQL server.

::::


For additional operations, see.

::::{note}
For an end-to-end example of the self-managed connector workflow, see [Tutorial](/reference/search-connectors/es-postgresql-connector-client-tutorial.md).

::::



### Compatibility [es-connectors-postgresql-client-compatibility]

PostgreSQL versions 11 to 15 are compatible with Elastic connector frameworks.


### Configuration [es-connectors-postgresql-client-configuration]

Set the following configuration fields:

`host`
:   The server host address where the PostgreSQL instance is hosted. Examples:

    * `192.158.1.38`
    * `demo.instance.demo-region.demo.service.com`


`port`
:   The port where the PostgreSQL instance is hosted. Examples:

    * `5432`
    * `9090`


`username`
:   The username of the PostgreSQL account.

`password`
:   The password of the PostgreSQL account.

`database`
:   Name of the PostgreSQL database. Examples:

    * `employee_database`
    * `customer_database`


`schema`
:   The schema of the PostgreSQL database.

`tables`
:   A list of tables separated by commas. The PostgreSQL connector will fetch data from all tables present in the configured database, if the value is `*` . Default value is `*`. Examples:

    * `table_1, table_2`
    * `*`

        ::::{warning}
        This field can be bypassed when using advanced sync rules.

        ::::


`ssl_enabled`
:   Whether SSL verification will be enabled. Default value is `True`.

`ssl_ca`
:   Content of SSL certificate (if SSL is enabled). If SSL is disabled, the `ssl_ca` value will be ignored.

    ::::{dropdown} Expand to see an example certificate
    ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    AlVTMQwwCgYDVQQKEwNJQk0xFjAUBgNVBAsTDURlZmF1bHROb2RlMDExFjAUBgNV
    BAsTDURlZmF1bHRDZWxsMDExGTAXBgNVBAsTEFJvb3QgQ2VydGlmaWNhdGUxEjAQ
    BgNVBAMTCWxvY2FsaG9zdDAeFw0yMTEyMTQyMjA3MTZaFw0yMjEyMTQyMjA3MTZa
    MF8xCzAJBgNVBAYTAlVTMQwwCgYDVQQKEwNJQk0xFjAUBgNVBAsTDURlZmF1bHRO
    b2RlMDExFjAUBgNVBAsTDURlZmF1bHRDZWxsMDExEjAQBgNVBAMTCWxvY2FsaG9z
    dDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMv5HCsJZIpI5zCy+jXV
    z6lmzNc9UcVSEEHn86h6zT6pxuY90TYeAhlZ9hZ+SCKn4OQ4GoDRZhLPTkYDt+wW
    CV3NTIy9uCGUSJ6xjCKoxClJmgSQdg5m4HzwfY4ofoEZ5iZQ0Zmt62jGRWc0zuxj
    hegnM+eO2reBJYu6Ypa9RPJdYJsmn1RNnC74IDY8Y95qn+WZj//UALCpYfX41hko
    i7TWD9GKQO8SBmAxhjCDifOxVBokoxYrNdzESl0LXvnzEadeZTd9BfUtTaBHhx6t
    njqqCPrbTY+3jAbZFd4RiERPnhLVKMytw5ot506BhPrUtpr2lusbN5svNXjuLeea
    MMUCAwEAAaOBoDCBnTATBgNVHSMEDDAKgAhOatpLwvJFqjAdBgNVHSUEFjAUBggr
    BgEFBQcDAQYIKwYBBQUHAwIwVAYDVR0RBE0wS4E+UHJvZmlsZVVVSUQ6QXBwU3J2
    MDEtQkFTRS05MDkzMzJjMC1iNmFiLTQ2OTMtYWI5NC01Mjc1ZDI1MmFmNDiCCWxv
    Y2FsaG9zdDARBgNVHQ4ECgQITzqhA5sO8O4wDQYJKoZIhvcNAQELBQADggEBAKR0
    gY/BM69S6BDyWp5dxcpmZ9FS783FBbdUXjVtTkQno+oYURDrhCdsfTLYtqUlP4J4
    CHoskP+MwJjRIoKhPVQMv14Q4VC2J9coYXnePhFjE+6MaZbTjq9WaekGrpKkMaQA
    iQt5b67jo7y63CZKIo9yBvs7sxODQzDn3wZwyux2vPegXSaTHR/rop/s/mPk3YTS
    hQprs/IVtPoWU4/TsDN3gIlrAYGbcs29CAt5q9MfzkMmKsuDkTZD0ry42VjxjAmk
    xw23l/k8RoD1wRWaDVbgpjwSzt+kl+vJE/ip2w3h69eEZ9wbo6scRO5lCO2JM4Pr
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```

    ::::



### Deployment using Docker [es-connectors-postgresql-client-docker]

You can deploy the PostgreSQL connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: postgresql
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



### Documents and syncs [es-connectors-postgresql-client-documents-syncs]

* Tables must be owned by a PostgreSQL user.
* Tables with no primary key defined are skipped.
* To fetch the last updated time in PostgreSQL, `track_commit_timestamp` must be set to `on`. Otherwise, all data will be indexed in every sync.

::::{note}
* Files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



### Sync rules [es-connectors-postgresql-client-sync-rules]


#### Advanced sync rules [es-connectors-postgresql-client-sync-rules-advanced]

::::{note}
A //connectors-sync-types-full, full sync is required for advanced sync rules to take effect.

::::


Advanced sync rules are defined through a source-specific DSL JSON snippet.


##### Example data [es-connectors-postgresql-client-sync-rules-advanced-example-data]

Here is some example data that will be used in the following examples.

$$$es-connectors-postgresql-client-sync-rules-advanced-example-data-1$$$
**`employee` table**

| emp_id | name | age |
| --- | --- | --- |
| 3 | John | 28 |
| 10 | Jane | 35 |
| 14 | Alex | 22 |

$$$es-connectors-postgresql-client-sync-rules-advanced-example-2$$$
**`customer` table**

| c_id | name | age |
| --- | --- | --- |
| 2 | Elm | 24 |
| 6 | Pine | 30 |
| 9 | Oak | 34 |


##### Advanced sync rules examples [es-connectors-postgresql-client-sync-rules-advanced-examples]

$$$es-connectors-postgresql-client-sync-rules-advanced-examples-1$$$
**Multiple table queries**

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

$$$es-connectors-postgresql-client-sync-rules-advanced-examples-1-id-columns$$$
**Multiple table queries with `id_columns`**

In 8.15.0, we added a new optional `id_columns` field in our advanced sync rules for the PostgreSQL connector. Use the `id_columns` field to ingest tables which do not have a primary key. Include the names of unique fields so that the connector can use them to generate unique IDs for documents.

```js
[
  {
    "tables": [
      "employee"
    ],
    "query": "SELECT * FROM employee",
    "id_columns": ["emp_id"]
  },
  {
    "tables": [
      "customer"
    ],
    "query": "SELECT * FROM customer",
    "id_columns": ["c_id"]
  }
]
```
% NOTCONSOLE

This example uses the `id_columns` field to specify the unique fields `emp_id` and `c_id` for the `employee` and `customer` tables, respectively.

$$$es-connectors-postgresql-client-sync-rules-advanced-examples-2$$$
**Filtering data with `WHERE` clause**

```js
[
  {
    "tables": ["employee"],
    "query": "SELECT * FROM employee WHERE emp_id > 5"
  }
]
```
% NOTCONSOLE

$$$es-connectors-postgresql-client-sync-rules-advanced-examples-3$$$
**`JOIN` operations**

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



### End-to-end testing [es-connectors-postgresql-client-client-operations-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the PostgreSQL connector, run the following command:

```shell
$ make ftest NAME=postgresql
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=postgresql DATA_SIZE=small
```


### Known issues [es-connectors-postgresql-client-known-issues]

There are no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-postgresql-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-postgresql-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).