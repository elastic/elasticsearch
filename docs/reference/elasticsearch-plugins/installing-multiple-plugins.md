---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/installing-multiple-plugins.html
applies_to:
  deployment:
    self: ga
---

# Installing multiple plugins [installing-multiple-plugins]

Multiple plugins can be installed in one invocation as follows:

```shell
sudo bin/elasticsearch-plugin install [plugin_id] [plugin_id] ... [plugin_id]
```

Each `plugin_id` can be any valid form for installing a single plugin (e.g., the name of a core plugin, or a custom URL).

For instance, to install the core [ICU plugin](/reference/elasticsearch-plugins/analysis-icu.md), run the following command:

```shell
sudo bin/elasticsearch-plugin install analysis-icu
```

This command will install the versions of the plugins that matches your Elasticsearch version. The installation will be treated as a transaction, so that all the plugins will be installed, or none of the plugins will be installed if any installation fails.

