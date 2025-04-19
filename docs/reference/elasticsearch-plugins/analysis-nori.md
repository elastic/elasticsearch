---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-nori.html
---

# Korean (nori) analysis plugin [analysis-nori]

The Korean (nori) Analysis plugin integrates Lucene nori analysis module into elasticsearch. It uses the [mecab-ko-dic dictionary](https://bitbucket.org/eunjeon/mecab-ko-dic) to perform morphological analysis of Korean texts.


## Installation [analysis-nori-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install analysis-nori
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-nori/analysis-nori-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-nori/analysis-nori-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-nori/analysis-nori-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-nori/analysis-nori-{{version}}.zip.asc).


## Removal [analysis-nori-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove analysis-nori
```

The node must be stopped before removing the plugin.






