---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-stempel.html
sub:
  plugin-name: analysis-stempel
---

# Stempel Polish analysis plugin [analysis-stempel]

The Stempel analysis plugin integrates Lucene’s Stempel analysis module for Polish into elasticsearch.


## Installation [analysis-stempel-install]

:::{include} _snippets/plugin-install.md
:::


## Removal [analysis-stempel-remove]

:::{include} _snippets/plugin-remove.md
:::


## `stempel` tokenizer and token filters [analysis-stempel-tokenizer]

The plugin provides the `polish` analyzer and the `polish_stem` and `polish_stop` token filters, which are not configurable.



