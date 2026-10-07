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

You can download this plugin for [offline install](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md#elasticsearch-plugin-ids) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version.stack}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version.stack}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version.stack}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-stempel/analysis-stempel-{{version.stack}}.zip.asc).

The plugin manager installs plugins on a self-managed cluster. To install this plugin on {{ech}}, {{ece}}, or {{eck}}, refer to the [plugin installation instructions for your deployment type](docs-content://deploy-manage/plugins-and-custom-configuration-files.md#plugins-by-deployment-type).


## Removal [analysis-stempel-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove analysis-stempel
```

The node must be stopped before removing the plugin.


## `stempel` tokenizer and token filters [analysis-stempel-tokenizer]

The plugin provides the `polish` analyzer and the `polish_stem` and `polish_stop` token filters, which are not configurable.



