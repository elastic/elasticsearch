---
navigation_title: "Build and customize connectors"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-framework.html
---

# Elastic connector framework: build and customize connectors [es-connectors-framework]


The Elastic connector framework enables developers to build Elastic-supported self-managed connectors which sync third-party data sources to Elasticsearch. The framework implements common functionalities out of the box, so developers can focus on the logic specific to integrating their chosen data source.

The framework ensures compatibility, makes it easier for our team to review PRs, and help out in the development process. When you build using our framework, we provide a pathway for the connector to be officially supported by Elastic.


## Use cases [es-connectors-framework-use-cases]

The framework serves two distinct, but related use cases:

* Customizing an existing Elastic [self-managed connector](/reference/search-connectors/self-managed-connectors.md)
* Building a new self-managed connector


## Learn more [es-connectors-framework-learn-more]

To learn how to contribute connectors using the framework, refer to our [contributing guide](https://github.com/elastic/connectors/blob/main/docs/CONTRIBUTING.md) in the `connectors` repository. This guide explains how to get started and includes a contribution checklist and pull request guidelines.

This repo contains all the source code for existing Elastic connectors.

