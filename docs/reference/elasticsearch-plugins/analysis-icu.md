---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu.html
sub:
  plugin-name: analysis-icu
---

# ICU analysis plugin [analysis-icu]

The ICU Analysis plugin integrates the Lucene ICU module into {{es}}, adding extended Unicode support using the [ICU](https://icu.unicode.org/) libraries, including better analysis of Asian languages, Unicode normalization, Unicode-aware case folding, collation support, and transliteration.

::::{admonition} ICU analysis and backwards compatibility
:class: important

From time to time, the ICU library receives updates such as adding new characters and emojis, and improving collation (sort) orders. These changes may or may not affect search and sort orders, depending on which characters sets you are using.

While we restrict ICU upgrades to major versions, you may find that an index created in the previous major version will need to be reindexed in order to return correct (and correctly ordered) results, and to take advantage of new characters.

::::



## Installation [analysis-icu-install]

:::{include} _snippets/plugin-install.md
:::


## Removal [analysis-icu-remove]

:::{include} _snippets/plugin-remove.md
:::









