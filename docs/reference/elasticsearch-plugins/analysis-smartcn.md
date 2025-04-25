---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-smartcn.html
---

# Smart Chinese analysis plugin [analysis-smartcn]

The Smart Chinese Analysis plugin integrates Lucene’s Smart Chinese analysis module into elasticsearch.

It provides an analyzer for Chinese or mixed Chinese-English text. This analyzer uses probabilistic knowledge to find the optimal word segmentation for Simplified Chinese text. The text is first broken into sentences, then each sentence is segmented into words.


## Installation [analysis-smartcn-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install analysis-smartcn
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-smartcn/analysis-smartcn-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-smartcn/analysis-smartcn-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-smartcn/analysis-smartcn-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-smartcn/analysis-smartcn-{{version}}.zip.asc).


## Removal [analysis-smartcn-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove analysis-smartcn
```

The node must be stopped before removing the plugin.


## `smartcn` tokenizer and token filter [analysis-smartcn-tokenizer]

The plugin provides the `smartcn` analyzer, `smartcn_tokenizer` tokenizer, and `smartcn_stop` token filter which are not configurable.

::::{note}
The `smartcn_word` token filter and `smartcn_sentence` have been deprecated.
::::




