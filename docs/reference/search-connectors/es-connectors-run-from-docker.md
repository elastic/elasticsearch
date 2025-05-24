---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-run-from-docker.html
---

# Running from a Docker container [es-connectors-run-from-docker]

::::{tip}
Use our [Docker Compose quickstart](/reference/search-connectors/es-connectors-docker-compose-quickstart.md) to quickly get started with a full Elastic Stack deployment using Connectors.
::::

Instead of running the Connectors Service from source, you can use the official Docker image to run the service in a container.

As a prerequisite, you need to have an Elasticsearch and Kibana instance running. From inside your Kibana UI, You will need to [follow the initial setup](/reference/search-connectors/es-connectors-run-from-source.md#es-connectors-run-from-source-setup-kibana) in the same manner as if you are running the service from source.

When you are ready to run Connectors:

**Step 1: Download sample configuration file**

Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example --output </absolute/path/to>/connectors-config/config.yml
```
% NOTCONSOLE

Don’t forget to change the `--output` argument value to the path where you want to save the `config.yml` file on your local system. But keep note of where you wrote this file, as it is required in the `docker run` step below.

**Step 2: Update the configuration file for your self-managed connector**

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
    service_type: sharepoint_online # Example value — update this for service type you are connecting to
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

**Step 3: Run the Docker image**

Run the Docker image with the Connector Service using the following command:

```sh
docker run \
-v "</absolute/path/to>/connectors-config:/config" \ # NOTE: you must change this path to match where the config.yml is located
--rm \
--tty -i \
--network host \
docker.elastic.co/integrations/elastic-connectors:9.0.0 \
/app/bin/elastic-ingest \
-c /config/config.yml
```

::::{tip}
For unreleased versions, append the `-SNAPSHOT` suffix to the version number. For example, `docker.elastic.co/integrations/elastic-connectors:9.0.0-SNAPSHOT`.

::::


Find all available Docker images in the [official registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).


## Enter data source details in Kibana [es-build-connector-finalizes-kibana]

Once the connector service is running, it’s time to head back to the Kibana UI to finalize the connector configuration. In this step, you need to add the specific connection details about your data source instance, like URL, authorization credentials, etc. As mentioned above, these details will vary based on the third-party data source you’re connecting to.

For example, the PostgreSQL connector requires the following details:

* **Host**
* **Port**
* **Username**
* **Password**
* **Database**
* **Comma-separated list of tables**

You’re now ready to run a sync. Select the **Full sync** button in the Kibana UI to start ingesting documents into Elasticsearch.

