---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs-config.html
---

# Configuration properties [repository-hdfs-config]

Once installed, define the configuration for the `hdfs` repository through the [REST API](docs-content://deploy-manage/tools/snapshot-and-restore.md):

```console
PUT _snapshot/my_hdfs_repository
{
  "type": "hdfs",
  "settings": {
    "uri": "hdfs://namenode:8020/",
    "path": "elasticsearch/repositories/my_hdfs_repository",
    "conf.dfs.client.read.shortcircuit": "true"
  }
}
```

The following settings are supported:

`uri`
:   The uri address for hdfs. ex: "hdfs://<host>:<port>/". (Required)

`path`
:   The file path within the filesystem where data is stored/loaded. ex: "path/to/file". (Required)

`load_defaults`
:   Whether to load the default Hadoop configuration or not. (Enabled by default)

`conf.<key>`
:   Inlined configuration parameter to be added to Hadoop configuration. (Optional) Only client oriented properties from the hadoop [core](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/core-default.xml) and [hdfs](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml) configuration files will be recognized by the plugin.

`compress`
:   Whether to compress the metadata or not. (Enabled by default)

`max_restore_bytes_per_sec`
:   Throttles per node restore rate. Defaults to unlimited. Note that restores are also throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`max_snapshot_bytes_per_sec`
:   Throttles per node snapshot rate. Defaults to `40mb` per second. Note that if the [recovery settings for managed services](/reference/elasticsearch/configuration-reference/index-recovery-settings.md) are set, then it defaults to unlimited, and the rate is additionally throttled through [recovery settings](/reference/elasticsearch/configuration-reference/index-recovery-settings.md).

`readonly`
:   Makes repository read-only. Defaults to `false`.

`chunk_size`
:   Override the chunk size. (Disabled by default)

`security.principal`
:   Kerberos principal to use when connecting to a secured HDFS cluster. If you are using a service principal for your elasticsearch node, you may use the `_HOST` pattern in the principal name and the plugin will replace the pattern with the hostname of the node at runtime (see [Creating the Secure Repository](/reference/elasticsearch-plugins/repository-hdfs-security.md#repository-hdfs-security-runtime)).

`replication_factor`
:   The replication factor for all new HDFS files created by this repository. Must be greater or equal to `dfs.replication.min` and less or equal to `dfs.replication.max` HDFS option. Defaults to using HDFS cluster setting.


## A note on HDFS availability [repository-hdfs-availability]

When you initialize a repository, its settings are persisted in the cluster state. When a node comes online, it will attempt to initialize all repositories for which it has settings. If your cluster has an HDFS repository configured, then all nodes in the cluster must be able to reach HDFS when starting. If not, then the node will fail to initialize the repository at start up and the repository will be unusable. If this happens, you will need to remove and re-add the repository or restart the offending node.

