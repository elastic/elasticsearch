---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-kuromoji.html
---

# Japanese (kuromoji) analysis plugin [analysis-kuromoji]

The Japanese (kuromoji) analysis plugin integrates Lucene kuromoji analysis module into {{es}}.


## Installation [analysis-kuromoji-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install analysis-kuromoji
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-kuromoji/analysis-kuromoji-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-kuromoji/analysis-kuromoji-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-kuromoji/analysis-kuromoji-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-kuromoji/analysis-kuromoji-{{version}}.zip.asc).


## Removal [analysis-kuromoji-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove analysis-kuromoji
```

The node must be stopped before removing the plugin.













