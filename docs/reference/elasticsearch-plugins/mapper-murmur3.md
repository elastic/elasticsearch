---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-murmur3.html
---

# Mapper murmur3 plugin [mapper-murmur3]

The mapper-murmur3 plugin provides the ability to compute hash of field values at index-time and store them in the index. This can sometimes be helpful when running cardinality aggregations on high-cardinality and large string fields.


## Installation [mapper-murmur3-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install mapper-murmur3
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-murmur3/mapper-murmur3-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-murmur3/mapper-murmur3-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-murmur3/mapper-murmur3-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-murmur3/mapper-murmur3-{{version}}.zip.asc).


## Removal [mapper-murmur3-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove mapper-murmur3
```

The node must be stopped before removing the plugin.


