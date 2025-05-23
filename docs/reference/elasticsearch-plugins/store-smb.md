---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/store-smb.html
---

# Store SMB plugin [store-smb]

The Store SMB plugin works around for a bug in Windows SMB and Java on windows.


## Installation [store-smb-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install store-smb
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/store-smb/store-smb-{{version}}.zip.asc).


## Removal [store-smb-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove store-smb
```

The node must be stopped before removing the plugin.


