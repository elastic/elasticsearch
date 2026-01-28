---
mapped_pages: []
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Apache Superset & Preset [sql-client-apps-superset]

You can use the [`elasticsearch-dbapi`](https://github.com/preset-io/elasticsearch-dbapi) driver to access {{es}} data from [Apache Superset™](https://superset.apache.org/) and [Preset](https://preset.io/).

The `elasticsearch-dbapi` package, originally developed and contributed by [Preset](https://preset.io/), provides a [DBAPI 2.0](https://peps.python.org/pep-0249/) and [SQLAlchemy](https://www.sqlalchemy.org/) dialect for {{es}}. It communicates with {{es}} through the [SQL REST API](sql-rest.md).

::::{important}
Elastic does not endorse, promote or provide support for these applications; for native {{es}} integration in these products, reach out to their respective vendors.
::::


## Prerequisites [_prerequisites_superset]

* [Apache Superset](https://superset.apache.org/) 5.0.0 or higher, or a [Preset](https://preset.io/) workspace
* Python package [`elasticsearch-dbapi`](https://github.com/preset-io/elasticsearch-dbapi):

  ```bash
  pip install elasticsearch-dbapi
  ```


## Add an {{es}} database [_add_an_es_database]

In Superset, select **+ Database** from the database connections screen. Choose **Elasticsearch** from the list of supported databases, or select **Other** and enter a SQLAlchemy URI directly.


## Configure the connection [_configure_the_connection]

Enter the SQLAlchemy URI for your {{es}} cluster. The URI format depends on your authentication method and deployment type.

For username and password authentication:

```
elasticsearch+https://{username}:{password}@{host}:{port}/
```

For [API key](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/create-api-key) authentication:

```
elasticsearch+https://{host}:{port}/?api_key={API_KEY}
```

For Elastic Cloud deployments, use the {{es}} endpoint URL from your deployment details:

```
elasticsearch+https://{username}:{password}@{deployment_id}.{region}.cloud.es.io:9243/
```

::::{note}
For a local, unencrypted cluster, replace `https` with `http` in the URI scheme (for example, `elasticsearch+http://localhost:9200/`).
::::


## Verify the connection [_verify_the_connection_superset]

Use the **Test Connection** button to confirm that Superset can reach your {{es}} cluster. Once connected, {{es}} indices appear as datasets that can be queried and visualized using SQL within Superset.


## Additional configuration [_additional_configuration_superset]

The `elasticsearch-dbapi` driver accepts optional parameters through the SQLAlchemy URI query string:

| Parameter | Description |
| --- | --- |
| `fetch_size` | Maximum number of rows per query result (default: `10000`) |
| `time_zone` | Timezone for date/time values (default: `UTC`) |
| `http_compress` | Enable HTTP compression (`true` or `false`) |
| `timeout` | Connection timeout in seconds |

For example:

```
elasticsearch+https://user:pass@host:9243/?fetch_size=5000&time_zone=US/Eastern
```

Refer to the [`elasticsearch-dbapi` documentation](https://github.com/preset-io/elasticsearch-dbapi) for the full list of supported parameters.


## Known limitations [_known_limitations_superset]

The `elasticsearch-dbapi` driver has some known limitations:

* Array-type columns are not supported
* Nested and object field types are returned as strings
* Geo-point field types are returned as strings
* Indices starting with `.` are filtered out by default

Apache, Apache Superset, Superset, and the Superset logo are trademarks of the [Apache Software Foundation](https://www.apache.org/foundation/marks/).
