---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-azure-classic.html
---

# Azure Classic discovery plugin [discovery-azure-classic]

The Azure Classic Discovery plugin uses the Azure Classic API to identify the addresses of seed hosts.

::::{admonition} Deprecated in 5.0.0.
:class: warning

This plugin will be removed in the future
::::



## Installation [discovery-azure-classic-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install discovery-azure-classic
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-azure-classic/discovery-azure-classic-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-azure-classic/discovery-azure-classic-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-azure-classic/discovery-azure-classic-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-azure-classic/discovery-azure-classic-{{version}}.zip.asc).


## Removal [discovery-azure-classic-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove discovery-azure-classic
```

The node must be stopped before removing the plugin.




