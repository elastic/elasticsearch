---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs.html
---

# Hadoop HDFS repository plugin [repository-hdfs]

The HDFS repository plugin adds support for using HDFS File System as a repository for [Snapshot/Restore](docs-content://deploy-manage/tools/snapshot-and-restore.md).


## Installation [repository-hdfs-install]

::::{warning}
Version 9.0.0-beta1 of the Elastic Stack has not yet been released. The plugin might not be available.
::::


This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install repository-hdfs
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-9.0.0-beta1.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-9.0.0-beta1.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-9.0.0-beta1.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/repository-hdfs/repository-hdfs-9.0.0-beta1.zip.asc).


## Removal [repository-hdfs-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove repository-hdfs
```

The node must be stopped before removing the plugin.




