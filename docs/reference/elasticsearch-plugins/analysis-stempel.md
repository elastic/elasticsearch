---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-stempel.html
---

# Stempel Polish analysis plugin [analysis-stempel]

The Stempel analysis plugin integrates Lucene’s Stempel analysis module for Polish into elasticsearch.


## Installation [analysis-stempel-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install analysis-stempel
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version}}.zip.asc).


## Removal [analysis-stempel-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove analysis-stempel
```

The node must be stopped before removing the plugin.


## `stempel` tokenizer and token filters [analysis-stempel-tokenizer]

The plugin provides the `polish` analyzer and the `polish_stem` and `polish_stop` token filters, which are not configurable.



