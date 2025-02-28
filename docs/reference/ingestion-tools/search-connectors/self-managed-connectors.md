---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-build-connector.html
---

# Self-managed connectors [es-build-connector]

::::{admonition} Naming history
Self-managed connectors were initially known as "connector clients". You might find this term in older documentation.

::::


Self-managed [Elastic connectors](/reference/ingestion-tools/search-connectors/index.md) are run on your own infrastructure. This means they run outside of your Elastic deployment.

You can run the [connectors service](#es-connectors-deploy-connector-service) from source or from a Docker container.

We also have a quickstart option using **Docker Compose**, to spin up all the required services at once: Elasticsearch, Kibana, and the connectors service. Refer to [Docker Compose quickstart](/reference/ingestion-tools/search-connectors/es-connectors-docker-compose-quickstart.md) for more information.


## Availability and Elastic prerequisites [es-build-connector-prerequisites]

::::{note}
Self-managed connectors currently don’t support Windows. Use this [compatibility matrix](https://www.elastic.co/support/matrix#matrix_os) to check which operating systems are supported by self-managed connectors. Find this information under **self-managed connectors** on that page.

::::


:::::{dropdown} Expand for Elastic prerequisites information
Your Elastic deployment must include the following Elastic services:

* **Elasticsearch**
* **Kibana**

(A new Elastic Cloud deployment includes these services by default.)

To run self-managed connectors, your self-deployed connector service version must match your Elasticsearch version. For example, if you’re running Elasticsearch 8.10.1, your connector service should be version 8.10.1.x. Elastic does not support deployments running mismatched versions (except during upgrades).

::::{note}
As of 8.10.0 *new* self-managed connectors no longer require the Enterprise Search service. However, if you are upgrading connectors from versions earlier than 8.9, you’ll need to run Enterprise Search once to migrate your connectors to the new format. In future releases, you may still need to run Enterprise Search for the purpose of migrations or upgrades.

Please note that Enterprise Search is not available in versions 9.0+.
::::


You must have access to Kibana and have `write` [indices privileges^](/reference/elasticsearch/security-privileges.md) for the `.elastic-connectors` index.

To use connector clients in a self-managed environment, you must deploy the [connectors service](#es-connectors-deploy-connector-service).

**Support and licensing requirements**

Depending on how you use self-managed connectors, support and licensing requirements will vary.

Refer to the following subscriptions pages for details. Find your connector of interest in the **Elastic Search** section under **Client Integrations**:

* [Elastic self-managed subscriptions page](https://www.elastic.co/subscriptions/)
* [Elastic Cloud subscriptions page](https://www.elastic.co/subscriptions/cloud)

Note the following information regarding support for self-managed connectors:

* A converted but *unmodified* managed connector is supported by Elastic.
* A converted but *customized* managed connector is *not* supported by Elastic.

:::::


::::{admonition} Data source prerequisites
:name: es-build-connector-data-source-prerequisites

The first decision you need to make before deploying a connector is which third party service (data source) you want to sync to Elasticsearch. Note that each data source will have specific prerequisites you’ll need to meet to authorize the connector to access its data. For example, certain data sources may require you to create an OAuth application, or create a service account.

You’ll need to check the individual connector documentation for these details.

::::



## Deploy the connector service [es-connectors-deploy-connector-service]

The connector service is a Python application that you must run on your own infrastructure when using self-managed connectors. The source code is hosted in the [elastic/connectors](https://github.com/elastic/connectors) repository.

You can run the connector service from source or use Docker:

* [Run the connectors from source](/reference/ingestion-tools/search-connectors/es-connectors-run-from-source.md). Use this option if you’re comfortable working with Python and want to iterate quickly locally.
* [Run the connectors from Docker](/reference/ingestion-tools/search-connectors/es-connectors-run-from-docker.md). Use this option if you want to deploy the connectors to a server, or use a container orchestration platform.

    * Refer to our [Docker Compose quickstart](/reference/ingestion-tools/search-connectors/es-connectors-docker-compose-quickstart.md) for a quick way to spin up all the required services at once.



## Tutorials [es-build-connector-example]

* Follow our [UI-based tutorial](/reference/ingestion-tools/search-connectors/es-postgresql-connector-client-tutorial.md) to learn how run the self-managed connector service and a set up a self-managed connector, **using the UI**.
* Follow our [API-based tutorial](/reference/ingestion-tools/search-connectors/api-tutorial.md) to learn how to set up a self-managed connector **using the** [**connector APIs**](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector).

These examples use the PostgreSQL connector but the basic process is the same for all self-managed connectors.


## Connector testing [es-build-connector-testing]

The connector framework enables you to run end-to-end (E2E) tests on your self-managed connectors, against a real data source.

To avoid tampering with a real Elasticsearch instance, E2E tests run an isolated Elasticsearch instance in Docker. Configuration values are set in your `docker-compose.yml` file. Docker Compose manages the setup of the development environment, including both the mock Elastic instance and mock data source.

E2E tests use **default** configuration values for the connector. Find instructions about testing in each connector’s documentation.


## Connector framework [es-build-connector-framework]

The Elastic connector framework enables you to:

* Customize existing self-managed connectors.
* Build your own self-managed connectors.

Refer to [Build and customize connectors](/reference/ingestion-tools/search-connectors/build-customize-connectors.md) for more information.





