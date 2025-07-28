---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-run-from-source.html
---

# Running from the source code [es-connectors-run-from-source]

The basic way to run connectors is to clone the repository and run the code locally. This is a good option if you are comfortable with Python and want to iterate quickly.


## Initial setup in Kibana [es-connectors-run-from-source-setup-kibana]

Follow the **Connector** workflow in the Kibana UI to select the **Connector** ingestion method.

Next, complete these steps:

1. Choose which third-party service you’d like to use by selecting a **data source**.
2. Create and name a new **Elasticsearch index**.
3. Generate a new **API key** and save it somewhere safe.
4. Name your connector and provide an optional description
5. **Convert** managed connector to a self-managed connector (*Only applicable if connector is also available natively*). This action is irreversible.
6. Copy the configuration block from the example shown on the screen. You’ll use this in a later step:

    ```yaml
    # ...
    connectors:
      - connector_id: <CONNECTOR-ID>
        api_key: <API-KEY> # Scoped API key for this connector (optional). If not specified, the top-level `elasticsearch.api_key` value is used.
        service_type: gmail # example
    ```



#### Clone the repository and edit `config.yml` [es-connectors-run-from-source-source-clone]

Once you’ve created an index, and entered the access details for your data source, you’re ready to deploy the connector service.

First, you need to clone the `elastic/connectors` repository.

Follow these steps:

* Clone or fork the `connectors` repository locally with the following command: `git clone https://github.com/elastic/connectors`.
* Run `make config` to generate your initial `config.yml` file
* Open the `config.yml` configuration file in your editor of choice.
* Replace the values for `host` (your Elasticsearch endpoint), `api_key`, `connector_id`, and `service_type`.

    :::::{dropdown} Expand to see an example config.yml file
    Replace the values for `api_key`, `connector_id`, and `service_type` with the values you copied earlier.

    ```yaml
    elasticsearch:
      api_key: <key1> # Used to write data to .elastic-connectors and .elastic-connectors-sync-jobs
                    # Any connectors without a specific `api_key` value will default to using this key
    connectors:
      - connector_id: 1234
        api_key: <key2> # Used to write data to the `search-*` index associated with connector 1234
                        # You may have multiple connectors in your config file!
      - connector_id: 5678
        api_key: <key3> # Used to write data to the `search-*` index associated with connector 5678
      - connector_id: abcd # No explicit api key specified, so this connector will use <key1>
    ```

    ::::{note}
    :name: es-connectors-run-from-source-api-keys

    **API keys for connectors**

    You can configure multiple connectors in your `config.yml` file.

    The Kibana UI enables you to create API keys that are scoped to a specific index/connector. If you don’t create an API key for a specific connector, the top-level `elasticsearch.api_key` or `elasticsearch.username:elasticsearch.password` value is used.

    If these top-level Elasticsearch credentials are not sufficiently privileged to write to individual connector indices, you’ll need to create these additional, scoped API keys.

    Use the example above as a guide.

    ::::


    :::::



#### Run the connector service [es-connectors-run-from-source-run]

::::{note}
You need Python version `3.10` or `3.11` to run the connectors service from source.

::::


Once you’ve configured the connector code, you can run the connector service.

In your terminal or IDE:

1. `cd` into the root of your `connectors` clone/fork.
2. Run the following commands to compile and run the connector service:

    ```shell
    make install
    make run
    ```


The connector service should now be running. The UI will let you know that the connector has successfully connected to your Elasticsearch instance.

As a reminder, here we’re working locally. In a production setup, you’ll deploy the connector service to your own infrastructure.
