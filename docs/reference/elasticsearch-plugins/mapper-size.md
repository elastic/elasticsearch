---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-size.html
---

# Mapper size plugin [mapper-size]

The mapper-size plugin provides the `_size` metadata field which, when enabled, indexes the size in bytes of the original [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field.


## Installation [mapper-size-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install mapper-size
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md#elasticsearch-plugin-ids) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-size/mapper-size-{{version.stack}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-size/mapper-size-{{version.stack}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-size/mapper-size-{{version.stack}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-size/mapper-size-{{version.stack}}.zip.asc).

The plugin manager installs plugins on a self-managed cluster. To install this plugin on {{ech}}, {{ece}}, or {{eck}}, refer to the [plugin installation instructions for your deployment type](docs-content://deploy-manage/plugins-and-custom-configuration-files.md#plugins-by-deployment-type).


## Removal [mapper-size-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove mapper-size
```

The node must be stopped before removing the plugin.


