---
applies_to:
  stack: ga
  serverless: ga
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-build-connector.html
---

# Self-managed connectors [es-build-connector]

::::{admonition} Naming history
Self-managed connectors were initially known as "connector clients". You might find this term in older documentation.

::::

Self-managed [Elastic connectors](/reference/search-connectors/index.md) are run on your own infrastructure. This means they run outside of your Elastic deployment.

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

## Workflow

In order to set up, configure, and run a connector you’ll be moving between your third-party service, the Elastic UI, and your terminal. At a high-level, the workflow looks like this:

1. Satisfy any data source prerequisites (e.g., create an OAuth application).
2. Create a connector in the UI (or via the API).
3. Deploy the connector service:
    - [Option 1: Run with Docker](es-connectors-run-from-docker.md) (recommended)
    - [Option 2: Run from source](es-connectors-run-from-source.md)
4. Enter data source configuration details in the UI.

## Deploy the connector service [es-connectors-deploy-connector-service]

The connector service is a Python application that you must run on your own infrastructure when using self-managed connectors. The source code is hosted in the [elastic/connectors](https://github.com/elastic/connectors) repository.

You can run the connector service from source or use Docker:

* [Run the connectors from source](/reference/search-connectors/es-connectors-run-from-source.md). Use this option if you’re comfortable working with Python and want to iterate quickly locally.
* [Run the connectors from Docker](/reference/search-connectors/es-connectors-run-from-docker.md). Use this option if you want to deploy the connectors to a server, or use a container orchestration platform.

    * Refer to our [Docker Compose quickstart](/reference/search-connectors/es-connectors-docker-compose-quickstart.md) for a quick way to spin up all the required services at once.


## Tutorials [es-build-connector-example]

* Follow our [UI-based tutorial](/reference/search-connectors/es-postgresql-connector-client-tutorial.md) to learn how run the self-managed connector service and a set up a self-managed connector, **using the UI**.
* Follow our [API-based tutorial](/reference/search-connectors/api-tutorial.md) to learn how to set up a self-managed connector **using the** [**connector APIs**](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector).

These examples use the PostgreSQL connector but the basic process is the same for all self-managed connectors.

## E2E testing [es-build-connector-testing]

The connector framework enables you to run end-to-end (E2E) tests on your self-managed connectors, against a real data source.

To avoid tampering with a real Elasticsearch instance, E2E tests run an isolated Elasticsearch instance in Docker. Configuration values are set in your `docker-compose.yml` file. Docker Compose manages the setup of the development environment, including both the mock Elastic instance and mock data source.

E2E tests use **default** configuration values for the connector. Find instructions about testing in each connector’s documentation.


## Build or customize connectors [es-build-connector-framework]

The Elastic connector framework enables you to:

* Customize existing self-managed connectors.
* Build your own self-managed connectors.

Refer to [Build and customize connectors](/reference/search-connectors/build-customize-connectors.md) for more information.





