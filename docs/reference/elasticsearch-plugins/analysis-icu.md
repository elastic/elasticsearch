---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu.html
---

# ICU analysis plugin [analysis-icu]

The ICU Analysis plugin integrates the Lucene ICU module into {{es}}, adding extended Unicode support using the [ICU](https://icu.unicode.org/) libraries, including better analysis of Asian languages, Unicode normalization, Unicode-aware case folding, collation support, and transliteration.

::::{admonition} ICU analysis and backwards compatibility
:class: important

From time to time, the ICU library receives updates such as adding new characters and emojis, and improving collation (sort) orders. These changes may or may not affect search and sort orders, depending on which characters sets you are using.

While we restrict ICU upgrades to major versions, you may find that an index created in the previous major version will need to be reindexed in order to return correct (and correctly ordered) results, and to take advantage of new characters.

::::



## Installation [analysis-icu-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install analysis-icu
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-{{version}}.zip.asc).


## Removal [analysis-icu-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove analysis-icu
```

The node must be stopped before removing the plugin.









