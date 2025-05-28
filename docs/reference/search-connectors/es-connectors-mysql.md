---
navigation_title: "MySQL"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-mysql.html
---

# Elastic MySQL connector reference [es-connectors-mysql]


The *Elastic MySQL connector* is a [connector](/reference/search-connectors/index.md) for [MySQL](https://www.mysql.com) data sources. This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/mysql.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-mysql-connector-client-reference]

### Availability and prerequisites [es-connectors-mysql-client-prerequisites]

This connector is available as a **self-managed managed connector** in Elastic versions **8.5.0 and later**. To use this connector as a self-managed connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).

This connector has no additional prerequisites beyond the shared requirements, linked above.

### Create a MySQL connector [es-connectors-mysql-create-connector-client]


#### Use the UI [es-connectors-mysql-client-create-use-the-ui]

To create a new MySQL connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **MySQL** self-managed connector.


#### Use the API [es-connectors-mysql-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed MySQL self-managed connector.

For example:

```console
PUT _connector/my-mysql-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from MySQL",
  "service_type": "mysql"
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


### Usage [es-connectors-mysql-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md).

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Compatibility [es-connectors-mysql-client-compatibility]

This connector is compatible with **MySQL 5.6 and later**.

The connector is also compatible with **MariaDB** databases compatible with the above.

The data source and your Elastic deployment must be able to communicate with each other over a network.


### Configuration [es-connectors-mysql-client-configuration]

Each time you create an index to be managed by this connector, you will create a new connector configuration. You will need some or all of the following information about the data source.

Host
:   The IP address or domain name of the MySQL host, excluding port. Examples:

    * `192.158.1.38`
    * `localhost`


Port
:   The port of the MySQL host. Examples:

    * `3306`
    * `3307`


Username
:   The MySQL username the connector will use.

    The user must have access to the configured database. You may want to create a dedicated, read-only user for each connector.


Password
:   The MySQL password the connector will use.

Database
:   The MySQL database to sync. The database must be accessible using the configured username and password.

    Examples:

    * `products`
    * `orders`


Tables
:   The tables in the configured database to sync. One or more table names, separated by commas. The tables must be accessible using the configured username and password.

    Examples:

    * `furniture, food, toys`
    * `laptops`


Enable SSL
:   Whether SSL verification will be enabled. Default value is `True`.

SSL Certificate
:   Content of SSL certificate. If SSL is disabled, the SSL certificate value will be ignored.

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



### Known issues [es-connectors-mysql-client-known-issues]

This connector has the following known issues:

* **Upgrading from a tech preview connector (8.7 or earlier) to 8.8 will cause the MySQL connector configuration to be invalid.**

    MySQL connectors prior to 8.8 can be missing some configuration fields that are required for the connector to run. If you would like to continue using your MySQL connector after upgrading from 8.7 or earlier, run the script below to fix your connector’s configuration. This will populate the configuration with the missing fields. The auxilliary information needed for the configuration will then be automatically added by by the self-managed connector.

    ```console
    POST /.elastic-connectors/_update/connector_id
    {
      "doc" : {
        "configuration": {
          "tables": {
            "type": "list",
            "value": "*"
          },
          "ssl_enabled": {
            "type": "bool",
            "value": false
          },
          "ssl_ca": {
            "type": "str",
            "value": ""
          },
          "fetch_size": {
            "type": "int",
            "value": 50
          },
          "retry_count": {
            "type": "int",
            "value": 3
          }
        }
      }
    }
    ```

* **Upgrading to 8.8 does not migrate MySQL sync rules.**

    After upgrading, you must re-create your sync rules.


See [Known issues](/release-notes/known-issues.md) for any issues affecting all connectors.


### Documents and syncs [es-connectors-mysql-client-syncs]

The following describes the default syncing behavior for this connector. Use [sync rules](/reference/search-connectors/es-sync-rules.md) and [ingest pipelines](docs-content://solutions/search/ingest-for-search.md) to customize syncing for specific indices.

All records in the MySQL database included in your connector configuration are extracted and transformed into documents in your Elasticsearch index.

* For each row in your MySQL database table, the connector creates one **Elasticsearch document**.
* For each column, the connector transforms the column into an **Elasticsearch field**.
* Elasticsearch [dynamically maps^](docs-content://manage-data/data-store/mapping/dynamic-mapping.md) MySQL data types to **Elasticsearch data types**.
* Tables with no primary key defined are skipped.
* Field values that represent other records are replaced with the primary key for that record (composite primary keys are joined with `_`).

The Elasticsearch mapping is created when the first document is created.

Each sync is a "full" sync.

For each MySQL row discovered:

* If it does not exist, the document is created in Elasticsearch.
* If it already exists in Elasticsearch, the Elasticsearch document is replaced and the version is incremented.
* If an existing Elasticsearch document no longer exists in the MySQL table, it is deleted from Elasticsearch.


### Deployment using Docker [es-connectors-mysql-client-docker]

You can deploy the MySQL connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: mysql
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



### Sync rules [es-connectors-mysql-client-sync-rules]

The following sections describe [Sync rules](/reference/search-connectors/es-sync-rules.md) for this connector.

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

[Advanced rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for MySQL can be used to pass arbitrary SQL statements to a MySQL instance.

::::{important}
You need to specify the tables used in your custom query in the "tables" field.

::::


For example:

```js
[
    {
        "tables": ["table1", "table2"],
        "query": "SELECT ... FROM ..."
    }
]
```

::::{warning}
When using advanced rules, a query can bypass the configuration field `tables`. This will happen if the query specifies a table that doesn’t appear in the configuration. This can also happen if the configuration specifies `*` to fetch all tables while the advanced sync rule requests for only a subset of tables.

::::



### Troubleshooting [es-connectors-mysql-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-mysql-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

