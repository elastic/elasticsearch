---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/store-smb.html
---

# Store SMB plugin [store-smb]

The Store SMB plugin works around for a bug in Windows SMB and Java on windows.


## Installation [store-smb-install]

::::{warning}
Version 9.0.0-beta1 of the Elastic Stack has not yet been released. The plugin might not be available.
::::


This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install store-smb
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-9.0.0-beta1.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-9.0.0-beta1.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-9.0.0-beta1.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-9.0.0-beta1.zip.asc).


## Removal [store-smb-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove store-smb
```

The node must be stopped before removing the plugin.


