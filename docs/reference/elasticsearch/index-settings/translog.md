---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-translog.html
navigation_title: Translog
---

# Translog settings [index-modules-translog]

Changes to Lucene are only persisted to disk during a Lucene commit, which is a relatively expensive operation and so cannot be performed after every index or delete operation. Changes that happen after one commit and before another will be removed from the index by Lucene in the event of process exit or hardware failure.

Lucene commits are too expensive to perform on every individual change, so each shard copy also writes operations into its *transaction log* known as the *translog*. All index and delete operations are written to the translog after being processed by the internal Lucene index but before they are acknowledged. In the event of a crash, recent operations that have been acknowledged but not yet included in the last Lucene commit are instead recovered from the translog when the shard recovers.

An {{es}} [flush](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-flush) is the process of performing a Lucene commit and starting a new translog generation. Flushes are performed automatically in the background in order to make sure the translog does not grow too large, which would make replaying its operations take a considerable amount of time during recovery.  The translog size will never exceed `1%` of the disk size. The ability to perform a flush manually is also exposed through an API, although this is rarely needed.


## Translog settings [_translog_settings]

The data in the translog is only persisted to disk when the translog is `fsync`ed and committed. In the event of a hardware failure or an operating system crash or a JVM crash or a shard failure, any data written since the previous translog commit will be lost.

By default, `index.translog.durability` is set to `request` meaning that Elasticsearch will only report success of an index, delete, update, or bulk request to the client after the translog has been successfully `fsync`ed and committed on the primary and on every allocated replica. If `index.translog.durability` is set to `async` then Elasticsearch `fsync`s and commits the translog only every `index.translog.sync_interval` which means that any operations that were performed just before a crash may be lost when the node recovers.

The following [dynamically updatable](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings) per-index settings control the behaviour of the translog:

`index.translog.sync_interval`
:   How often the translog is `fsync`ed to disk and committed, regardless of write operations. Defaults to `5s`. Values less than `100ms` are not allowed.

`index.translog.durability`
:   Whether or not to `fsync` and commit the translog after every index, delete, update, or bulk request. This setting accepts the following parameters:

`request`
:   (default) `fsync` and commit after every request. In the event of hardware failure, all acknowledged writes will already have been committed to disk.

`async`
:   `fsync` and commit in the background every `sync_interval`. In the event of a failure, all acknowledged writes since the last automatic commit will be discarded.


`index.translog.flush_threshold_size`
:   The translog stores all operations that are not yet safely persisted in Lucene (i.e., are not part of a Lucene commit point). Although these operations are available for reads, they will need to be replayed if the shard was stopped and had to be recovered. This setting controls the maximum total size of these operations to prevent recoveries from taking too long. Once the maximum size has been reached, a flush will happen, generating a new Lucene commit point. Defaults to `10 GB`.

