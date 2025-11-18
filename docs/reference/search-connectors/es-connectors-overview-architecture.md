---
navigation_title: "Internal knowledge search"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-overview-architecture.html
---

# Internal knowledge search architecture [es-connectors-overview-architecture]


The following section provides a high-level overview of common architecture approaches for the internal knowledge search use case (AKA workplace search).


$$$es-connectors-overview-architecture-hybrid$$$

% ^^ discontinued 9.0.0


## Self-managed architecture [es-connectors-overview-architecture-self-managed]

Data is synced to an Elastic deployment through self-managed connectors. A self-managed search application exposes the relevant data that your end users are authorized to see in a search experience.

Summary:

* Gives flexibility to build custom solutions tailored to specific business requirements and internal processes
* Allows enterprises to adhere to strict access policies when using firewalls that don’t allow incoming connections to data sources, while outgoing traffic is easier to control
* Provides additional functionality available for self-managed connectors such as the [Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local)
* Feasible for air-gapped environments
* Requires Platinum license for full spectrum of features and self-managed connectors

The following diagram provides a high-level overview of the self-managed internal knowledge search architecture.

:::{image} images/self-managed-architecture.png
:alt: self managed architecture
:class: screenshot
:::

