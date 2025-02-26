---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository.html
---

# Snapshot/restore repository plugins [repository]

Repository plugins extend the [Snapshot/Restore](docs-content://deploy-manage/tools/snapshot-and-restore.md) functionality in Elasticsearch by adding repositories backed by the cloud or by distributed file systems:


### Official repository plugins [_official_repository_plugins]

::::{note}
Support for S3, GCS and Azure repositories is now bundled in {{es}} by default.
::::


The official repository plugins are:

[HDFS Repository](/reference/elasticsearch-plugins/repository-hdfs.md)
:   The Hadoop HDFS Repository plugin adds support for using HDFS as a repository.


## Community contributed repository plugins [_community_contributed_repository_plugins]

The following plugin has been contributed by our community:

* [Openstack Swift](https://github.com/BigDataBoutique/elasticsearch-repository-swift) (by Wikimedia Foundation and BigData Boutique)


