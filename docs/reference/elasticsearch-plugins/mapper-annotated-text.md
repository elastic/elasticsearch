---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-annotated-text.html
sub:
  plugin-id: mapper-annotated-text
---

# Mapper annotated text plugin [mapper-annotated-text]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The mapper-annotated-text plugin provides the ability to index text that is a combination of free-text and special markup that is typically used to identify items of interest such as people or organisations (see NER or Named Entity Recognition tools).

The elasticsearch markup allows one or more additional tokens to be injected, unchanged, into the token stream at the same position as the underlying text it annotates.


## Installation [mapper-annotated-text-install]

:::{include} _snippets/plugin-installation.md
:::

:::{include} _snippets/plugin-deployment-types.md
:::


## Removal [mapper-annotated-text-remove]

:::{include} _snippets/plugin-removal.md
:::





