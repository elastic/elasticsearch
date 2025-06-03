---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-annotated-text.html
---

# Mapper annotated text plugin [mapper-annotated-text]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The mapper-annotated-text plugin provides the ability to index text that is a combination of free-text and special markup that is typically used to identify items of interest such as people or organisations (see NER or Named Entity Recognition tools).

The elasticsearch markup allows one or more additional tokens to be injected, unchanged, into the token stream at the same position as the underlying text it annotates.


## Installation [mapper-annotated-text-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install mapper-annotated-text
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-annotated-text/mapper-annotated-text-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-annotated-text/mapper-annotated-text-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-annotated-text/mapper-annotated-text-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/mapper-annotated-text/mapper-annotated-text-{{version}}.zip.asc).


## Removal [mapper-annotated-text-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove mapper-annotated-text
```

The node must be stopped before removing the plugin.





