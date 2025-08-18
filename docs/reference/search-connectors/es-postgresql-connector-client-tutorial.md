---
navigation_title: "Tutorial"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-postgresql-connector-client-tutorial.html
  - https://www.elastic.co/guide/en/starting-with-the-elasticsearch-platform-and-its-solutions/current/getting-started-appsearch.html
applies_to:
  stack: ga
  serverless:
    elasticsearch: ga
description: Synchronize data from a PostgreSQL data source into Elasticsearch.
---

# Set up a self-managed PostgreSQL connector

Elastic connectors enable you to create searchable, read-only replicas of your data sources in {{es}}.
This tutorial walks you through the process of creating a self-managed connector for a PostgreSQL data source.

If you want to deploy a self-managed connector for another data source, use this tutorial as a blueprint. Refer to the list of available [connectors](/reference/search-connectors/index.md).

::::{tip}
Want to get started quickly testing a self-managed connector and a self-managed cluster using Docker Compose? Refer to the [readme](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.
::::

## Prerequisites [es-postgresql-connector-client-tutorial-prerequisites]

### Elastic prerequisites [es-postgresql-connector-client-tutorial-prerequisites-elastic]

You must satisfy the [prerequisites](/reference/search-connectors/self-managed-connectors.md#es-build-connector-prerequisites) for self-managed connectors.

<!--
TBD: If you're using {{es-serverless}}, you must have a `developer` or `admin` predefined role or an equivalent custom role to add the connector. 
-->

### PostgreSQL prerequisites [es-postgresql-connector-client-tutorial-postgresql-prerequisites]

You need:

* PostgreSQL version 11+.
* Tables must be owned by a PostgreSQL user.
* Database `superuser` privileges are required to index all database tables.

::::{tip}
You should enable recording of the commit time of PostgreSQL transactions. Otherwise, *all* data will be indexed in every sync. By default, `track_commit_timestamp` is `off`.

Enable this by running the following command on the PosgreSQL server command line:

```shell
ALTER SYSTEM SET track_commit_timestamp = on;
```

Then restart the PostgreSQL server.

::::

## Set up the connector

:::::{stepper}
::::{step} Create an Elasticsearch index
To store data in {{es}}, the connector needs to create an index.
By default, connectors use [dynamic mappings](docs-content://manage-data/data-store/mapping.md#mapping-dynamic) to automatically infer the data types of your fields.
If you [use APIs](/reference/search-connectors/api-tutorial.md) or {{es-serverless}}, you can create an index with the desired field mappings and index settings before you create the connector.
Defining your own mappings upfront gives you more control over how your data is indexed.

Navigate to **{{index-manage-app}}** or use the [global search field](docs-content://explore-analyze/find-and-organize/find-apps-and-objects.md).
Follow the index creation workflow then optionally define field mappings.
For example, to add semantic search capabilities, you could add an extra field that stores your vectors for semantic search.
::::
::::{step} Create the connector
Navigate to **{{connectors-app}}** or use the global search field.
Follow the connector creation process in the UI. For example:

1. Select **PostgreSQL** from the list of connectors.
1. Edit the name and description for the connector. This will help your team identify the connector.
1. Gather configuration details.
   Before you can proceed to the next step, you need to gather some details about your Elastic deployment:

    * Elasticsearch endpoint:
      * If you’re an Elastic Cloud user, find your deployment’s Elasticsearch endpoint in the Cloud UI under **Cloud > Deployments > <your-deployment> > Elasticsearch**.
      * If you’re running your Elastic deployment and the connector service in Docker, the default Elasticsearch endpoint is `http://host.docker.internal:9200`.
    * API key: You’ll need this key to configure the connector. Use an existing key or create a new one.
    * Connector ID: Your unique connector ID is automatically generated when you create the connector.
::::
::::{step} Run the connector service
You must run the connector code on your own infrastructure and link it to {{es}}.
You have two options: [Run with Docker](/reference/search-connectors/es-connectors-run-from-docker.md) and [Run from source](/reference/search-connectors/es-connectors-run-from-source.md).
For this example, we’ll use the latter method:

1. Clone or fork the repository locally with the following command: `git clone https://github.com/elastic/connectors`.
1. Open the `config.yml` configuration file in your editor of choice.
1. Replace the values for `host`, `api_key`, and `connector_id` with the values you gathered earlier. Use the `service_type` value `postgresql` for this connector.

    :::{dropdown} Expand to see an example config.yml file
    Replace the values for `host`, `api_key`, and `connector_id` with your own values.
    Use the `service_type` value `postgresql` for this connector.

    ```yaml
    elasticsearch:
      host: "<YOUR-ELASTIC-URL>>" # Your Elasticsearch endpoint
      api_key: "<YOUR-API-KEY>" # Your top-level Elasticsearch API key
    ...
    connectors:
      -
        connector_id: "<YOUR-CONNECTOR-ID>"
        api_key: "<YOUR-API-KEY>" # Your scoped connector index API key (optional). If not provided, the top-level API key is used.
        service_type: "postgresql"
    
    sources:
      # mongodb: connectors.sources.mongo:MongoDataSource
      # s3: connectors.sources.s3:S3DataSource
      # dir: connectors.sources.directory:DirectoryDataSource
      # mysql: connectors.sources.mysql:MySqlDataSource
      # network_drive: connectors.sources.network_drive:NASDataSource
      # google_cloud_storage: connectors.sources.google_cloud_storage:GoogleCloudStorageDataSource
      # azure_blob_storage: connectors.sources.azure_blob_storage:AzureBlobStorageDataSource
      postgresql: connectors.sources.postgresql:PostgreSQLDataSource
      # oracle: connectors.sources.oracle:OracleDataSource
      # sharepoint: connectors.sources.sharepoint:SharepointDataSource
      # mssql: connectors.sources.mssql:MSSQLDataSource
      # jira: connectors.sources.jira:JiraDataSource
    ```

1. Now that you’ve configured the connector code, you can run the connector service. In your terminal or IDE:

    1. `cd` into the root of your `connectors` clone/fork.
    1. Run the following command: `make run`.

The connector service should now be running.
The UI will let you know that the connector has successfully connected to {{es}}.

:::{tip}
Here we’re working locally. In production setups, you’ll deploy the connector service to your own infrastructure.
:::
::::
::::{step} Add your data source details

Now your connector instance is up and running, but it doesn’t yet know where to sync data from.
The final piece of the puzzle is to configure your connector with details about the PostgreSQL instance.

Return to **{{connectors-app}}** to complete the connector creation process in the UI.
Enter the following PostgreSQL instance details:

* **Host**: The server host address for your PostgreSQL instance.
* **Port**: The port number for your PostgreSQL instance.
* **Username**: The username of the PostgreSQL account.
* **Password**: The password for that user.
* **Database**: The name of the PostgreSQL database.
* **Schema**: The schema of the PostgreSQL database.
* **Comma-separated list of tables**: `*` will fetch data from all tables in the configured database.

:::{note}
Configuration details are specific to the connector type.
The keys and values will differ depending on which third-party data source you’re connecting to.
Refer to the [](/reference/search-connectors/es-connectors-postgresql.md) for more details.
:::
::::
::::{step} Link your index
If you [use APIs](/reference/search-connectors/api-tutorial.md) or {{es-serverless}}, you can create an index or choose an existing index for use by the connector.
Otherwise, the index is created for you and uses dynamic mappings for the fields.
::::
:::::

## Sync your data [es-postgresql-connector-client-tutorial-sync-data-source]

In the **{{connectors-app}}** page, you can launch a sync on-demand or on a schedule.
The connector will traverse the database and synchronize documents to your index.

If you navigate to the terminal window where you’re running the connector service, after a sync occurs you should see output like the following:

```shell
[FMWK][13:22:26][INFO] Fetcher <create: 499 update: 0 |delete: 0>
[FMWK][13:22:26][INF0] Fetcher <create: 599 update: 0 |delete: 0>
[FMWK][13:22:26][INFO] Fetcher <create: 699 update: 0 |delete: 0>
...
[FMWK][23:22:28][INF0] [oRXQwYYBLhXTs-qYpJ9i] Sync done: 3864 indexed, 0 deleted.
(27 seconds)
```

This confirms the connector has fetched records from your PostgreSQL tables and transformed them into documents in your {{es}} index.

If you verify your {{es}} documents and you’re happy with the results, set a recurring sync schedule.
This will ensure your searchable data in {{es}} is always up to date with changes to your PostgreSQL data source.

In **{{connectors-app}}**, click on the connector, and then click **Scheduling**.
For example, you can schedule your content to be synchronized at the top of every hour, as long as the connector is up and running.

## Next steps

You just learned how to synchronize data from an external database to {{es}}.
For an overview of how to start searching and analyzing your data in Kibana, go to [Explore and analyze](docs-content://explore-analyze/index.md).

Learn more:

* [Overview of self-managed connectors and frameworks](/reference/search-connectors/self-managed-connectors.md)
* [Elastic connector framework repository](https://github.com/elastic/connectors/tree/main)
* [Elastic PostgreSQL connector reference](/reference/search-connectors/es-connectors-postgresql.md)
* [Overview of all Elastic connectors](/reference/search-connectors/index.md)
