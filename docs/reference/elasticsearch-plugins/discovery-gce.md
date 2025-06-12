---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce.html
---

# GCE Discovery plugin [discovery-gce]

The Google Compute Engine Discovery plugin uses the GCE API to identify the addresses of seed hosts.


## Installation [discovery-gce-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install discovery-gce
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-gce/discovery-gce-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-gce/discovery-gce-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-gce/discovery-gce-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-gce/discovery-gce-{{version}}.zip.asc).


## Removal [discovery-gce-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove discovery-gce
```

The node must be stopped before removing the plugin.










