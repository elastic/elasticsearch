---
navigation_title: "Tutorial"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-postgresql-connector-client-tutorial.html
  - https://www.elastic.co/guide/en/starting-with-the-elasticsearch-platform-and-its-solutions/current/getting-started-appsearch.html
---

# PostgreSQL self-managed connector tutorial [es-postgresql-connector-client-tutorial]


This tutorial walks you through the process of creating a self-managed connector for a PostgreSQL data source. You’ll be using the [self-managed connector](/reference/search-connectors/self-managed-connectors.md) workflow in the Kibana UI. This means you’ll be deploying the connector on your own infrastructure. Refer to the [Elastic PostgreSQL connector reference](/reference/search-connectors/es-connectors-postgresql.md) for more information about this connector.

In this exercise, you’ll be working in both the terminal (or your IDE) and the Kibana UI.

If you want to deploy a self-managed connector for another data source, use this tutorial as a blueprint. Refer to the list of available [connectors](/reference/search-connectors/index.md).

::::{tip}
Want to get started quickly testing a self-managed connector using Docker Compose? Refer to this [guide](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.
::::

## Prerequisites [es-postgresql-connector-client-tutorial-prerequisites]


### Elastic prerequisites [es-postgresql-connector-client-tutorial-prerequisites-elastic]

First, ensure you satisfy the [prerequisites](/reference/search-connectors/self-managed-connectors.md#es-build-connector-prerequisites) for self-managed connectors.


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



## Steps [es-postgresql-connector-client-tutorial-steps]

To complete this tutorial, you’ll need to complete the following steps:

1. [Create an Elasticsearch index](#es-postgresql-connector-client-tutorial-create-index)
2. [Set up the connector](#es-postgresql-connector-client-tutorial-setup-connector)
3. [Run the `connectors` connector service](#es-postgresql-connector-client-tutorial-run-connector-service)
4. [Sync your PostgreSQL data source](#es-postgresql-connector-client-tutorial-sync-data-source)


## Create an Elasticsearch index [es-postgresql-connector-client-tutorial-create-index]

Elastic connectors enable you to create searchable, read-only replicas of your data sources in Elasticsearch. The first step in setting up your self-managed connector is to create an index.

In the [Kibana^](docs-content://get-started/the-stack.md) UI, navigate to **Search > Content > Elasticsearch indices** from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).

Create a new connector index:

1. Under **Select an ingestion method** choose **Connector**.
2. Choose **PostgreSQL** from the list of connectors.
3. Name your index and optionally change the language analyzer to match the human language of your data source. (The index name you provide is automatically prefixed with `search-`.)
4. Save your changes.

The index is created and ready to configure.

::::{admonition} Gather Elastic details
:name: es-postgresql-connector-client-tutorial-gather-elastic-details

Before you can configure the connector, you need to gather some details about your Elastic deployment:

* **Elasticsearch endpoint**.

    * If you’re an Elastic Cloud user, find your deployment’s Elasticsearch endpoint in the Cloud UI under **Cloud > Deployments > <your-deployment> > Elasticsearch**.
    * If you’re running your Elastic deployment and the connector service in Docker, the default Elasticsearch endpoint is `http://host.docker.internal:9200`.

* **API key.** You’ll need this key to configure the connector. Use an existing key or create a new one.
* **Connector ID**. Your unique connector ID is automatically generated when you create the connector. Find this in the Kibana UI.

::::



## Set up the connector [es-postgresql-connector-client-tutorial-setup-connector]

Once you’ve created an index, you can set up the connector. You will be guided through this process in the UI.

1. **Edit the name and description for the connector.** This will help your team identify the connector.
2. **Clone and edit the connector service code.** For this example, we’ll use the [Python framework](https://github.com/elastic/connectors/tree/main). Follow these steps:

    * Clone or fork that repository locally with the following command: `git clone https://github.com/elastic/connectors`.
    * Open the `config.yml` configuration file in your editor of choice.
    * Replace the values for `host`, `api_key`, and `connector_id` with the values you gathered [earlier](#es-postgresql-connector-client-tutorial-gather-elastic-details). Use the `service_type` value `postgresql` for this connector.

        ::::{dropdown} Expand to see an example config.yml file
        Replace the values for `host`, `api_key`, and `connector_id` with your own values. Use the `service_type` value `postgresql` for this connector.

        ```yaml
        elasticsearch:
          host: <https://<my-elastic-deployment.es.us-west2.gcp.elastic-cloud.com>> # Your Elasticsearch endpoint
          api_key: '<YOUR-API-KEY>' # Your top-level Elasticsearch API key
        ...
        connectors:
          -
            connector_id: "<YOUR-CONNECTOR-ID>"
            api_key: "'<YOUR-API-KEY>" # Your scoped connector index API key (optional). If not provided, the top-level API key is used.
            service_type: "postgresql"



        # Self-managed connector settings
        connector_id: '<YOUR-CONNECTOR-ID>' # Your connector ID
        service_type: 'postgresql'  # The service type for your connector

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

        ::::



## Run the connector service [es-postgresql-connector-client-tutorial-run-connector-service]

Now that you’ve configured the connector code, you can run the connector service.

In your terminal or IDE:

1. `cd` into the root of your `connectors` clone/fork.
2. Run the following command: `make run`.

The connector service should now be running. The UI will let you know that the connector has successfully connected to Elasticsearch.

Here we’re working locally. In production setups, you’ll deploy the connector service to your own infrastructure. If you prefer to use Docker, refer to the [repo docs](https://github.com/elastic/connectors/tree/main/docs/DOCKER.md) for instructions.


## Sync your PostgreSQL data source [es-postgresql-connector-client-tutorial-sync-data-source]


### Enter your PostgreSQL data source details [es-postgresql-connector-client-tutorial-sync-data-source-details]

Once you’ve configured the connector, you can use it to index your data source.

You can now enter your PostgreSQL instance details in the Kibana UI.

Enter the following information:

* **Host**. Server host address for your PostgreSQL instance.
* **Port**. Port number for your PostgreSQL instance.
* **Username**. Username of the PostgreSQL account.
* **Password**. Password for that user.
* **Database**. Name of the PostgreSQL database.
* **Comma-separated list of tables**. `*` will fetch data from all tables in the configured database.

Once you’ve entered all these details, select **Save configuration**.


### Launch a sync [es-postgresql-connector-client-tutorial-sync-data-source-launch-sync]

If you navigate to the **Overview** tab in the Kibana UI, you can see the connector’s *ingestion status*. This should now have changed to **Configured**.

It’s time to launch a sync by selecting the **Sync** button.

If you navigate to the terminal window where you’re running the connector service, you should see output like the following:

```shell
[FMWK][13:22:26][INFO] Fetcher <create: 499 update: 0 |delete: 0>
[FMWK][13:22:26][INF0] Fetcher <create: 599 update: 0 |delete: 0>
[FMWK][13:22:26][INFO] Fetcher <create: 699 update: 0 |delete: 0>
...
[FMWK][23:22:28][INF0] [oRXQwYYBLhXTs-qYpJ9i] Sync done: 3864 indexed, 0 deleted.
(27 seconds)
```

This confirms the connector has fetched records from your PostgreSQL table(s) and transformed them into documents in your Elasticsearch index.

Verify your Elasticsearch documents in the **Documents** tab in the Kibana UI.

If you’re happy with the results, set a recurring sync schedule in the **Scheduling** tab. This will ensure your *searchable* data in Elasticsearch is always up to date with changes to your PostgreSQL data source.


## Learn more [es-postgresql-connector-client-tutorial-learn-more]

* [Overview of self-managed connectors and frameworks](/reference/search-connectors/self-managed-connectors.md)
* [Elastic connector framework repository](https://github.com/elastic/connectors/tree/main)
* [Elastic PostgreSQL connector reference](/reference/search-connectors/es-connectors-postgresql.md)
* [Overview of all Elastic connectors](/reference/search-connectors/index.md)
