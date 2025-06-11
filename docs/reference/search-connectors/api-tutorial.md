---
navigation_title: "API tutorial"
applies_to:
  stack: ga
  serverless:
    elasticsearch: ga
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-tutorial-api.html
---

# Connector API tutorial [es-connectors-tutorial-api]

Learn how to set up a self-managed connector using the [{{es}} Connector APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector).

For this example we’ll use the connectors-postgresql,PostgreSQL connector to sync data from a PostgreSQL database to {{es}}. We’ll spin up a simple PostgreSQL instance in Docker with some example data, create a connector, and sync the data to {{es}}. You can follow the same steps to set up a connector for another data source.

::::{tip}
This tutorial focuses on running a self-managed connector on your own infrastructure, and managing syncs using the Connector APIs. See connectors for an overview of how connectors work.

If you’re just getting started with {{es}}, this tutorial might be a bit advanced. Refer to [quickstart](docs-content://solutions/search/get-started.md) for a more beginner-friendly introduction to {{es}}.

If you’re just getting started with connectors, you might want to start in the UI first. Check out this tutorial that focuses on managing connectors using the UI:

* [Self-managed connector tutorial](/reference/search-connectors/es-postgresql-connector-client-tutorial.md). Set up a self-managed PostgreSQL connector.

::::


### Prerequisites [es-connectors-tutorial-api-prerequisites]

* You should be familiar with how connectors, connectors work, to understand how the API calls relate to the overall connector setup.
* You need to have [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed.
* You need to have {{es}} running, and an API key to access it. Refer to the next section for details, if you don’t have an {{es}} deployment yet.


### Set up {{es}} [es-connectors-tutorial-api-setup-es]

If you already have an {{es}} deployment on Elastic Cloud (*Hosted deployment* or *Serverless project*), you’re good to go. To spin up {{es}} in local dev mode in Docker for testing purposes, open the collapsible section below.

:::::{dropdown} Run local {{es}} in Docker
```sh
docker run -p 9200:9200 -d --name elasticsearch \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "xpack.security.http.ssl.enabled=false" \
  -e "xpack.license.self_generated.type=trial" \
  docker.elastic.co/elasticsearch/elasticsearch:9.0.0
```

::::{warning}
This {{es}} setup is for development purposes only. Never use this configuration in production. Refer to [Set up {{es}}](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md) for production-grade installation instructions, including Docker.

::::


We will use the default password `changeme` for the `elastic` user. For production environments, always ensure your cluster runs with security enabled.

```sh
export ELASTIC_PASSWORD="changeme"
```

Since we run our cluster locally with security disabled, we won’t use API keys to authenticate against the {{es}}. Instead, in each cURL request, we will use the `-u` flag for authentication.

Let’s test that we can access {{es}}:

```sh
curl -s -X GET -u elastic:$ELASTIC_PASSWORD http://localhost:9200
```

Note: With {{es}} running locally, you will need to pass the username and password to authenticate against {{es}} in the configuration file for the connector service.

:::::


::::{admonition} Running API calls
You can run API calls using the [Dev Tools Console](docs-content://explore-analyze/query-filter/tools/console.md) in Kibana, using `curl` in your terminal, or with our programming language clients. Our example widget allows you to copy code examples in both Dev Tools Console syntax and curl syntax. To use curl, you’ll need to add authentication headers to your request.

Here’s an example of how to do that. Note that if you want the connector ID to be auto-generated, use the `POST _connector` endpoint.

```sh
curl -s -X PUT http://localhost:9200/_connector/my-connector-id \
-H "Authorization: APIKey $APIKEY" \
-H "Content-Type: application/json" \
-d '{
  "name": "Music catalog",
  "index_name":  "music",
  "service_type": "postgresql"
}'
```

Refer to connectors-tutorial-api-create-api-key for instructions on creating an API key.

::::



### Run PostgreSQL instance in Docker (optional) [es-connectors-tutorial-api-setup-postgres]

For this tutorial, we’ll set up a PostgreSQL instance in Docker with some example data. Of course, you can **skip this step and use your own existing PostgreSQL instance** if you have one. Keep in mind that using a different instance might require adjustments to the connector configuration described in the next steps.

::::{dropdown} Expand to run simple PostgreSQL instance in Docker and import example data
Let’s launch a PostgreSQL container with a user and password, exposed at port `5432`:

```sh
docker run --name postgres -e POSTGRES_USER=myuser -e POSTGRES_PASSWORD=mypassword -p 5432:5432 -d postgres
```

**Download and import example data**

Next we need to create a directory to store our example dataset for this tutorial. In your terminal, run the following command:

```sh
mkdir -p ~/data
```

We will use the [Chinook dataset](https://github.com/lerocha/chinook-database/blob/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql) example data.

Run the following command to download the file to the `~/data` directory:

```sh
curl -L https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql -o ~/data/Chinook_PostgreSql.sql
```

Now we need to import the example data into the PostgreSQL container and create the tables.

Run the following Docker commands to copy our sample data into the container and execute the `psql` script:

```sh
docker cp ~/data/Chinook_PostgreSql.sql postgres:/
docker exec -it postgres psql -U myuser -f /Chinook_PostgreSql.sql
```

Let’s verify that the tables are created correctly in the `chinook` database:

```sh
docker exec -it postgres psql -U myuser -d chinook -c "\dt"
```

The `album` table should contain **347** entries and the `artist` table should contain **275** entries.

::::


This tutorial uses a very basic setup. To use advanced functionality such as filtering rules and incremental syncs, enable `track_commit_timestamp` on your PostgreSQL database. Refer to postgresql-connector-client-tutorial for more details.

Now it’s time for the real fun! We’ll set up a connector to create a searchable mirror of our PostgreSQL data in {{es}}.


### Create a connector [es-connectors-tutorial-api-create-connector]

We’ll use the [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-put) to create a PostgreSQL connector instance.

Run the following API call, using the [Dev Tools Console](docs-content://explore-analyze/query-filter/tools/console.md) or `curl`:

```console
PUT _connector/my-connector-id
{
  "name": "Music catalog",
  "index_name":  "music",
  "service_type": "postgresql"
}
```

::::{tip}
`service_type` refers to the third-party data source you’re connecting to.

::::


Note that we specified the `my-connector-id` ID as a part of the `PUT` request. We’ll need the connector ID to set up and run the connector service locally.

If you’d prefer to use an autogenerated ID, replace `PUT _connector/my-connector-id` with `POST _connector`.


### Run connector service [es-connectors-tutorial-api-deploy-connector]

Now we’ll run the connector service so we can start syncing data from our PostgreSQL instance to {{es}}. We’ll use the steps outlined in connectors-run-from-docker.

When running the connectors service on your own infrastructure, you need to provide a configuration file with the following details:

* Your {{es}} endpoint (`elasticsearch.host`)
* An {{es}} API key (`elasticsearch.api_key`)
* Your third-party data source type (`service_type`)
* Your connector ID (`connector_id`)


#### Create an API key [es-connectors-tutorial-api-create-api-key]

If you haven’t already created an API key to access {{es}}, you can use the [_security/api_key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key) endpoint.

Here, we assume your target {{es}} index name is `music`. If you use a different index name, adjust the request body accordingly.

```console
POST /_security/api_key
{
  "name": "music-connector",
  "role_descriptors": {
    "music-connector-role": {
      "cluster": [
        "monitor",
        "manage_connector"
      ],
      "indices": [
        {
          "names": [
            "music",
            ".search-acl-filter-music",
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

You’ll need to use the `encoded` value from the response as the `elasticsearch.api_key` in your configuration file.

::::{tip}
You can also create an API key in the {{kib}} and Serverless UIs.

::::



#### Prepare the configuration file [es-connectors-tutorial-api-prepare-configuration-file]

Let’s create a directory and a `config.yml` file to store the connector configuration:

```sh
mkdir -p ~/connectors-config
touch ~/connectors-config/config.yml
```

Now, let’s add our connector details to the config file. Open `config.yml` and paste the following configuration, replacing placeholders with your own values:

```yaml
elasticsearch.host: <ELASTICSEARCH_ENDPOINT> # Your Elasticsearch endpoint
elasticsearch.api_key: <ELASTICSEARCH_API_KEY> # Your Elasticsearch API key

connectors:
  - connector_id: "my-connector-id"
    service_type: "postgresql"
```

We provide an [example configuration file](https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example) in the `elastic/connectors` repository for reference.


#### Run the connector service [es-connectors-tutorial-api-run-connector-service]

Now that we have the configuration file set up, we can run the connector service locally. This will point your connector instance at your {{es}} deployment.

Run the following Docker command to start the connector service:

```sh
docker run \
-v "$HOME/connectors-config:/config" \
--rm \
--tty -i \
--network host \
docker.elastic.co/integrations/elastic-connectors:9.0.0 \
/app/bin/elastic-ingest \
-c /config/config.yml
```

Verify your connector is connected by getting the connector status (should be `needs_configuration`) and `last_seen` field (note that time is reported in UTC). The `last_seen` field indicates that the connector successfully connected to {{es}}.

```console
GET _connector/my-connector-id
```


### Configure connector [es-connectors-tutorial-api-update-connector-configuration]

Now our connector instance is up and running, but it doesn’t yet know *where* to sync data from. The final piece of the puzzle is to configure our connector with details about our PostgreSQL instance. When setting up a connector in the Elastic Cloud or Serverless UIs, you’re prompted to add these details in the user interface.

But because this tutorial is all about working with connectors *programmatically*, we’ll use the [Update connector configuration API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-configuration) to add our configuration details.

::::{tip}
Before configuring the connector, ensure that the configuration schema is registered by the service. For self-managed connectors, the schema registers on service startup (once the `config.yml` is populated).

Configuration updates via the API are possible only *after schema registration*. Verify this by checking the configuration property returned by the `GET _connector/my-connector-id` request. It should be non-empty.

::::


Run the following API call to configure the connector with our connectors-postgresql-client-configuration,PostgreSQL configuration details:

```console
PUT _connector/my-connector-id/_configuration
{
  "values": {
    "host": "127.0.0.1",
    "port": 5432,
    "username": "myuser",
    "password": "mypassword",
    "database": "chinook",
    "schema": "public",
    "tables": "album,artist"
  }
}
```

::::{note}
Configuration details are specific to the connector type. The keys and values will differ depending on which third-party data source you’re connecting to. Refer to the individual connectors-references,connector references for these configuration details.

::::



### Sync data [es-connectors-tutorial-api-sync]

We’re now ready to sync our PostgreSQL data to {{es}}. Run the following API call to start a full sync job:

```console
POST _connector/_sync_job
{
    "id": "my-connector-id",
    "job_type": "full"
}
```

To store data in {{es}}, the connector needs to create an index. When we created the connector, we specified the `music` index. The connector will create and configure this {{es}} index before launching the sync job.

::::{tip}
In the approach we’ve used here, the connector will use [dynamic mappings](docs-content://manage-data/data-store/mapping.md#mapping-dynamic) to automatically infer the data types of your fields. In a real-world scenario you would use the {{es}} [Create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) to first create the index with the desired field mappings and index settings. Defining your own mappings upfront gives you more control over how your data is indexed.

::::



#### Check sync status [es-connectors-tutorial-api-check-sync-status]

Use the [Get sync job API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-get) to track the status and progress of the sync job. By default, the most recent job statuses are returned first. Run the following API call to check the status of the sync job:

```console
GET _connector/_sync_job?connector_id=my-connector-id&size=1
```

The job document will be updated as the sync progresses, you can check it as often as you’d like to poll for updates.

Once the job completes, the status should be `completed` and `indexed_document_count` should be **622**.

Verify that data is present in the `music` index with the following API call:

```console
GET music/_count
```

{{es}} stores data in documents, which are JSON objects. List the individual documents with the following API call:

```console
GET music/_search
```


## Troubleshooting [es-connectors-tutorial-api-troubleshooting]

Use the following command to inspect the latest sync job’s status:

```console
GET _connector/_sync_job?connector_id=my-connector-id&size=1
```

If the connector encountered any errors during the sync, you’ll find these in the `error` field.


### Cleaning up [es-connectors-tutorial-api-cleanup]

To delete the connector and its associated sync jobs run this command:

```console
DELETE _connector/my-connector-id&delete_sync_jobs=true
```

This won’t delete the Elasticsearch index that was created by the connector to store the data. Delete the `music` index by running the following command:

```console
DELETE music
```

To remove the PostgreSQL container, run the following commands:

```sh
docker stop postgres
docker rm postgres
```

To remove the connector service, run the following commands:

```sh
docker stop <container_id>
docker rm <container_id>
```


### Next steps [es-connectors-tutorial-api-next-steps]

Congratulations! You’ve successfully set up a self-managed connector using the Connector APIs.

Here are some next steps to explore:

* Learn more about the [Connector APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector).
* Learn how to deploy {{es}}, {{kib}}, and the connectors service using Docker Compose in our [quickstart guide](https://github.com/elastic/connectors/tree/main/scripts/stack#readme).

test
