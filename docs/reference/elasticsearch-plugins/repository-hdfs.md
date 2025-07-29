---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs.html
---

# Hadoop HDFS repository plugin [repository-hdfs]

The HDFS repository plugin adds support for using HDFS File System as a repository for [Snapshot/Restore](docs-content://deploy-manage/tools/snapshot-and-restore.md).


## Installation [repository-hdfs-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install repository-hdfs
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-{{version}}.zip.asc).


## Removal [repository-hdfs-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove repository-hdfs
```

The node must be stopped before removing the plugin.




