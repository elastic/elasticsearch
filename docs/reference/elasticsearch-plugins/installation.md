---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/installation.html
applies_to:
  deployment:
    self: ga
---

# Installing plugins [installation]

The documentation for each plugin usually includes specific installation instructions for that plugin, but below we document the various available options:


## Core Elasticsearch plugins [_core_elasticsearch_plugins]

Core Elasticsearch plugins can be installed as follows:

```shell
sudo bin/elasticsearch-plugin install [plugin_name]
```

For instance, to install the core [ICU plugin](/reference/elasticsearch-plugins/analysis-icu.md), just run the following command:

```shell
sudo bin/elasticsearch-plugin install analysis-icu
```

This command will install the version of the plugin that matches your Elasticsearch version and also show a progress bar while downloading.

