---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-ukrainian.html
---

# Ukrainian analysis plugin [analysis-ukrainian]

The Ukrainian analysis plugin integrates Lucene’s UkrainianMorfologikAnalyzer into elasticsearch.

It provides stemming for Ukrainian using the [Morfologik project](https://github.com/morfologik/morfologik-stemming).


## Installation [analysis-ukrainian-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install analysis-ukrainian
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-ukrainian/analysis-ukrainian-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-ukrainian/analysis-ukrainian-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-ukrainian/analysis-ukrainian-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-ukrainian/analysis-ukrainian-{{version}}.zip.asc).


## Removal [analysis-ukrainian-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove analysis-ukrainian
```

The node must be stopped before removing the plugin.


## `ukrainian` analyzer [analysis-ukrainian-analyzer]

The plugin provides the `ukrainian` analyzer.

