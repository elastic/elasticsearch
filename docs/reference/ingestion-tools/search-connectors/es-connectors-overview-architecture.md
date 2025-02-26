---
navigation_title: "Internal knowledge search"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-overview-architecture.html
---

# Internal knowledge search architecture [es-connectors-overview-architecture]


The following section provides a high-level overview of common architecture approaches for the internal knowledge search use case (AKA workplace search).


## Hybrid architecture [es-connectors-overview-architecture-hybrid]

Data is synced to an Elastic Cloud deployment through managed connectors and/or self-managed connectors. A self-managed search application exposes the relevant data that your end users are authorized to see in a search experience.

Summary:

* The best combination in terms of flexibility and out-of-the box functionality
* Integrates with Elastic Cloud hosted managed connectors to bring data to Elasticsearch with minimal operational overhead
* Self-managed connectors allow enterprises to adhere to strict access policies when using firewalls that don’t allow incoming connections to data sources, while outgoing traffic is easier to control
* Provides additional functionality available for self-managed connectors such as the [Extraction Service](/reference/ingestion-tools/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local)
* Basic functionality available for Standard licenses, advanced features for Platinum licenses

The following diagram provides a high-level overview of the hybrid internal knowledge search architecture.

:::{image} ../../../images/hybrid-architecture.png
:alt: hybrid architecture
:class: screenshot
:::


## Self-managed architecture [es-connectors-overview-architecture-self-managed]

Data is synced to an Elastic deployment through self-managed connectors. A self-managed search application exposes the relevant data that your end users are authorized to see in a search experience.

Summary:

* Gives flexibility to build custom solutions tailored to specific business requirements and internal processes
* Allows enterprises to adhere to strict access policies when using firewalls that don’t allow incoming connections to data sources, while outgoing traffic is easier to control
* Provides additional functionality available for self-managed connectors such as the [Extraction Service](/reference/ingestion-tools/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local)
* Feasible for air-gapped environments
* Requires Platinum license for full spectrum of features and self-managed connectors

The following diagram provides a high-level overview of the self-managed internal knowledge search architecture.

:::{image} ../../../images/self-managed-architecture.png
:alt: self managed architecture
:class: screenshot
:::

