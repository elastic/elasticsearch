---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/path-settings-overview.html
navigation_title: Path
---
# Path settings [path-settings-overview]

{{es}} writes the data you index to indices and data streams to a `data` directory. {{es}} writes its own application logs, which contain information about cluster health and operations, to a `logs` directory.

For [macOS `.tar.gz`](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-from-archive-on-linux-macos.md), [Linux `.tar.gz`](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-from-archive-on-linux-macos.md), and [Windows `.zip`](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-zip-on-windows.md) installations, `data` and `logs` are subdirectories of `$ES_HOME` by default. However, files in `$ES_HOME` risk deletion during an upgrade.

In production, we strongly recommend you set the `path.data` and `path.logs` in `elasticsearch.yml` to locations outside of `$ES_HOME`. [Docker](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-docker.md), [Debian](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-debian-package.md), and [RPM](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-rpm.md) installations write data and log to locations outside of `$ES_HOME` by default.

Supported `path.data` and `path.logs` values vary by platform:

:::::::{tab-set}

::::::{tab-item} Unix-like systems
Linux and macOS installations support Unix-style paths:

```yaml
path:
  data: /var/data/elasticsearch
  logs: /var/log/elasticsearch
```
::::::

::::::{tab-item} Windows
Windows installations support DOS paths with escaped backslashes:

```yaml
path:
  data: "C:\\Elastic\\Elasticsearch\\data"
  logs: "C:\\Elastic\\Elasticsearch\\logs"
```
::::::

:::::::
::::{warning}
Don’t modify anything within the data directory or run processes that might interfere with its contents. If something other than {{es}} modifies the contents of the data directory, then {{es}} may fail, reporting corruption or other data inconsistencies, or may appear to work correctly having silently lost some of your data. Don’t attempt to take filesystem backups of the data directory; there is no supported way to restore such a backup. Instead, use [Snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md) to take backups safely. Don’t run virus scanners on the data directory. A virus scanner can prevent {{es}} from working correctly and may modify the contents of the data directory. The data directory contains no executables so a virus scan will only find false positives.
::::


## Multiple data paths [multiple-data-paths]

::::{warning}
Deprecated in 7.13.0.
::::


If needed, you can specify multiple paths in `path.data`. {{es}} stores the node’s data across all provided paths but keeps each shard’s data on the same path.

{{es}} does not balance shards across a node’s data paths. High disk usage in a single path can trigger a [high disk usage watermark](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#disk-based-shard-allocation) for the entire node. If triggered, {{es}} will not add shards to the node, even if the node’s other paths have available disk space. If you need additional disk space, we recommend you add a new node rather than additional data paths.

:::::::{tab-set}

::::::{tab-item} Unix-like systems
Linux and macOS installations support multiple Unix-style paths in `path.data`:

```yaml
path:
  data:
    - /mnt/elasticsearch_1
    - /mnt/elasticsearch_2
    - /mnt/elasticsearch_3
```
::::::

::::::{tab-item} Windows
Windows installations support multiple DOS paths in `path.data`:

```yaml
path:
  data:
    - "C:\\Elastic\\Elasticsearch_1"
    - "E:\\Elastic\\Elasticsearch_1"
    - "F:\\Elastic\\Elasticsearch_3"
```
::::::

:::::::
### Migrate from multiple data paths [mdp-migrate]

Support for multiple data paths was deprecated in 7.13 and will be removed in a future release.

As an alternative to multiple data paths, you can create a filesystem which spans multiple disks with a hardware virtualisation layer such as RAID, or a software virtualisation layer such as Logical Volume Manager (LVM) on Linux or Storage Spaces on Windows. If you wish to use multiple data paths on a single machine then you must run one node for each data path.

If you currently use multiple data paths in a [highly available cluster](docs-content://deploy-manage/production-guidance/availability-and-resilience.md) then you can migrate to a setup that uses a single path for each node without downtime using a process similar to a [rolling restart](docs-content://deploy-manage/maintenance/start-stop-services/full-cluster-restart-rolling-restart-procedures.md#restart-cluster-rolling): shut each node down in turn and replace it with one or more nodes each configured to use a single data path. In more detail, for each node that currently has multiple data paths you should follow the following process. In principle you can perform this migration during a rolling upgrade to 8.0, but we recommend migrating to a single-data-path setup before starting to upgrade.

1. Take a snapshot to protect your data in case of disaster.
2. Optionally, migrate the data away from the target node by using an [allocation filter](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-shard-allocation-filtering):

    ```console
    PUT _cluster/settings
    {
      "persistent": {
        "cluster.routing.allocation.exclude._name": "target-node-name"
      }
    }
    ```

    You can use the [cat allocation API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-allocation) to track progress of this data migration. If some shards do not migrate then the [cluster allocation explain API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-allocation-explain) will help you to determine why.

3. Follow the steps in the [rolling restart process](docs-content://deploy-manage/maintenance/start-stop-services/full-cluster-restart-rolling-restart-procedures.md#restart-cluster-rolling) up to and including shutting the target node down.
4. Ensure your cluster health is `yellow` or `green`, so that there is a copy of every shard assigned to at least one of the other nodes in your cluster.
5. If applicable, remove the allocation filter applied in the earlier step.

    ```console
    PUT _cluster/settings
    {
      "persistent": {
        "cluster.routing.allocation.exclude._name": null
      }
    }
    ```

6. Discard the data held by the stopped node by deleting the contents of its data paths.
7. Reconfigure your storage. For instance, combine your disks into a single filesystem using LVM or Storage Spaces. Ensure that your reconfigured storage has sufficient space for the data that it will hold.
8. Reconfigure your node by adjusting the `path.data` setting in its `elasticsearch.yml` file. If needed, install more nodes each with their own `path.data` setting pointing at a separate data path.
9. Start the new nodes and follow the rest of the [rolling restart process](docs-content://deploy-manage/maintenance/start-stop-services/full-cluster-restart-rolling-restart-procedures.md#restart-cluster-rolling) for them.
10. Ensure your cluster health is `green`, so that every shard has been assigned.

You can alternatively add some number of single-data-path nodes to your cluster, migrate all your data over to these new nodes using [allocation filters](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-shard-allocation-filtering), and then remove the old nodes from the cluster. This approach will temporarily double the size of your cluster so it will only work if you have the capacity to expand your cluster like this.

If you currently use multiple data paths but your cluster is not highly available then you can migrate to a non-deprecated configuration by taking a snapshot, creating a new cluster with the desired configuration and restoring the snapshot into it.



