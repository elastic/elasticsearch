---
navigation_title: "Known issues"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html
---

# Elasticsearch known issues [elasticsearch-known-issues]

Known issues are significant defects or limitations that may impact your implementation. These issues are actively being worked on and will be addressed in a future release. Review the Elasticsearch known issues to help you make informed decisions, such as upgrading to a new version.

## 9.1.1 [elasticsearch-9.1.1-known-issues]

* An [optimization](https://github.com/elastic/elasticsearch/pull/125403) introduced in 9.1.0 contains a [bug](https://github.com/elastic/elasticsearch/pull/132597) that causes merges to fail for shrunk TSDB and LogsDB indices.
  
  Possible *temporary* workarounds include:
  * Configure the ILM policy to not perform force merges after shrinking TSDB or LogsDB indices.
  * Add `-Dorg.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesConsumer.enableOptimizedMerge=false` as a Java system property to all data nodes in the cluster and perform a rolling restart.
    * *Important:* Remove this property when upgrading to the fixed version to re-enable merge optimization. Otherwise, merges will be slower.  

The bug is addressed in version 9.1.2.  

## 9.1.0 [elasticsearch-9.1.0-known-issues]

* An [optimization](https://github.com/elastic/elasticsearch/pull/125403) introduced in 9.1.0 contains a [bug](https://github.com/elastic/elasticsearch/pull/132597) that causes merges to fail for shrunk TSDB and LogsDB indices.
  
  Possible *temporary* workarounds include:
  * Configure the ILM policy to not perform force merges after shrinking TSDB or LogsDB indices.
  * Add `-Dorg.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesConsumer.enableOptimizedMerge=false` as a Java system property to all data nodes in the cluster and perform a rolling restart.
    * *Important:* Remove this property when upgrading to the fixed version to re-enable merge optimization. Otherwise, merges will be slower.  

The bug is addressed in version 9.1.2.  

* The `-Dvector.rescoring.directio` JVM option is enabled (set to `true`) by default. When used with `bbq_hnsw` type vector indices, this can cause significant search performance degradation; particularly when enough memory is available to hold all vector data. In some cases, kNN search latency can increase by as much as 10x. To mitigate this, set the JVM option `-Dvector.rescoring.directio=false` on all search nodes and restart them. This option can be removed in 9.1.1.

**How do I know if my index vector type is `bbq_hnsw`?**

* Prior to 9.1, the vector type had to be explicitly set to `bbq_hnsw`. Starting with 9.1, `bbq_hnsw` is the default vector type for dense vectors with more than 384 dimensions in new indices, unless another type is specified.

## 9.0.3 [elasticsearch-9.0.3-known-issues]

* A bug in the merge scheduler in Elasticsearch 9.0.3 may prevent shards from closing when there isn’t enough disk space to complete a merge. As a result, operations such as closing or relocating an index may hang until sufficient disk space becomes available.
To mitigate this issue, the disk space checker is disabled by default in 9.0.3 by setting `indices.merge.disk.check_interval` to `0` seconds. Manually enabling this setting is not recommended.

  This issue is planned to be fixed in future patch release [#129613](https://github.com/elastic/elasticsearch/pull/129613)

* A bug in the ES|QL STATS command may yield incorrect results. The bug only happens in very specific cases that follow this pattern: `STATS ... BY keyword1, keyword2`, i.e. the command must have exactly two grouping fields, both keywords, where the first field has high cardinality (more than 65k distinct values).

  The bug is described in detail in [this issue](https://github.com/elastic/elasticsearch/issues/130644).
  The problem was introduced in 8.16.0 and [fixed](https://github.com/elastic/elasticsearch/pull/130705) in 8.17.9, 8.18.7, 9.0.4.

  Possible workarounds include:
  * switching the order of the grouping keys (eg. `STATS ... BY keyword2, keyword1`, if the `keyword2` has a lower cardinality)
  * reducing the grouping key cardinality, by filtering out values before STATS

## 9.0.0 [elasticsearch-9.0.0-known-issues]
* Elasticsearch on Windows might fail to start, or might forbid some file-related operations, when referencing paths with a case different from the one stored by the filesystem. Windows treats paths as case-insensitive, but the filesystem stores them with case. Entitlements, the new security system used by Elasticsearch, treat all paths as case-sensitive, and can therefore prevent access to a path that should be accessible.

  For example: If Elasticsearch is installed in  `C:\ELK\elasticsearch`, and you try to launch it as `c:\elk\elasticsearch\bin\elasticsearch.bat`, you will get a `NotEntitledException` while booting. This is because Elasticsearch blocks access to `c:\elk\elasticsearch`, because does not match `C:\ELK\elasticsearch`. \
This issue will be fixed in a future patch release (see [PR #126990](https://github.com/elastic/elasticsearch/pull/126990)).

  As a workaround, make sure that all paths you specify have the same casing as the paths stored in the filesystem. Files and directory names should be entered as they appear in Windows Explorer or in a command prompt. This applies to paths specified in the command line, config files, environment variables and secure settings.

* Active Directory authentication is blocked by default. Entitlements, the new security system used by Elasticsearch, has a policy for the `x-pack-core` module that is too restrictive, and does not allow the LDAP library used for AD authentication to perform outbound network connections. This issue will be fixed in a future patch release (see [PR #126992](https://github.com/elastic/elasticsearch/pull/126992)).

  As a workaround, you can temporarily patch the policy using a JVM option:

  1. Create a file called `${ES_CONF_PATH}/jvm_options/workaround-127061.options`.
  2. Add the following line to the new file:

     ```
     -Des.entitlements.policy.x-pack-core=dmVyc2lvbnM6CiAgLSA4LjE4LjAKICAtIDkuMC4wCnBvbGljeToKICB1bmJvdW5kaWQubGRhcHNkazoKICAgIC0gc2V0X2h0dHBzX2Nvbm5lY3Rpb25fcHJvcGVydGllcwogICAgLSBvdXRib3VuZF9uZXR3b3Jr
     ```

  For information about editing your JVM settings, refer to [JVM settings](https://www.elastic.co/docs/reference/elasticsearch/jvm-settings).

* Users upgrading from an Elasticsearch cluster that had previously been on a version between 7.10.0 and 7.12.1 may see that Watcher will not start on 9.x. The solution is to run the following commands in Kibana Dev Tools (or the equivalent using curl):
     ```
     DELETE _index_template/.triggered_watches
     DELETE _index_template/.watches
     POST /_watcher/_start
     ```

* A bug in the ES|QL STATS command may yield incorrect results. The bug only happens in very specific cases that follow this pattern: `STATS ... BY keyword1, keyword2`, i.e. the command must have exactly two grouping fields, both keywords, where the first field has high cardinality (more than 65k distinct values).

  The bug is described in detail in [this issue](https://github.com/elastic/elasticsearch/issues/130644).
  The problem was introduced in 8.16.0 and [fixed](https://github.com/elastic/elasticsearch/pull/130705) in 8.17.9, 8.18.7, 9.0.4.

  Possible workarounds include:
    * switching the order of the grouping keys (eg. `STATS ... BY keyword2, keyword1`, if the `keyword2` has a lower cardinality)
    * reducing the grouping key cardinality, by filtering out values before STATS

* Repository analyses of snapshot repositories based on AWS S3 include some checks that the APIs which relate to multipart uploads have linearizable (strongly-consistent) semantics, based on guarantees offered by representatives from AWS on this subject. Further investigation has determined that these guarantees do not hold under all conditions as previously claimed. If you are analyzing a snapshot repository based on AWS S3 using an affected version of {{es}} and you encounter a failure related to linearizable register operations, you may work around the issue and suppress these checks by setting the query parameter `?register_operation_count=1` and running the analysis using a one-node cluster. This issue currently affects all supported versions of {{es}}. The plan to address it is described in [#137197](https://github.com/elastic/elasticsearch/issues/137197).
