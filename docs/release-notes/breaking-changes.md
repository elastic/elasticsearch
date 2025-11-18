---
navigation_title: "Breaking changes"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes.html
---

# Elasticsearch breaking changes [elasticsearch-breaking-changes]

Breaking changes can impact your Elastic applications, potentially disrupting normal operations. Before you upgrade, carefully review the Elasticsearch breaking changes and take the necessary steps to mitigate any issues.

If you are migrating from a version prior to version 9.0, you must first upgrade to the last 8.x version available. To learn how to upgrade, check out [Upgrade](docs-content://deploy-manage/upgrade.md).

% ## Next version [elasticsearch-nextversion-breaking-changes]

## 9.2.1 [elasticsearch-9.2.1-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.7 [elasticsearch-9.1.7-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.6 [elasticsearch-9.1.6-breaking-changes]

There are no breaking changes associated with this release.

## 9.2.0 [elasticsearch-9.2.0-breaking-changes]

Ingest Node:
* Simulate API: Return 400 on invalid processor(s) [#130325](https://github.com/elastic/elasticsearch/pull/130325) (issue: [#120731](https://github.com/elastic/elasticsearch/issues/120731))

Mapping:
* Don't enable norms for fields of type text when the index mode is LogsDB or TSDB [#131317](https://github.com/elastic/elasticsearch/pull/131317)

Vector Search:
* Enable `exclude_source_vectors` by default for new indices [#131907](https://github.com/elastic/elasticsearch/pull/131907)



## 9.0.8 [elasticsearch-9.0.8-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.5 [elasticsearch-9.1.5-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.4 [elasticsearch-9.1.4-breaking-changes]

There are no breaking changes associated with this release.

## 9.0.7 [elasticsearch-9.0.7-breaking-changes]

There are no breaking changes associated with this release.

## 9.0.6 [elasticsearch-9.0.6-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.3 [elasticsearch-9.1.3-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.2 [elasticsearch-9.1.2-breaking-changes]

There are no breaking changes associated with this release.

## 9.0.5 [elasticsearch-9.0.5-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.1 [elasticsearch-9.1.1-breaking-changes]

There are no breaking changes associated with this release.

## 9.1.0 [elasticsearch-9.1.0-breaking-changes]

Discovery-Plugins:
:::{dropdown} Migrates `discovery-ec2` plugin to AWS SDK v2
The `discovery-ec2` plugin now uses AWS SDK v2 instead of v1, as AWS plans to deprecate SDK v1 before the end of Elasticsearch 8.19’s support period. AWS SDK v2 introduces several behavior changes that affect configuration.

**Impact:**
If you use the `discovery-ec2` plugin, your existing settings may no longer be compatible. Notable changes include, but may not be limited to:
- AWS SDK v2 does not support the EC2 IMDSv1 protocol.
- AWS SDK v2 does not support the `aws.secretKey` or `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` system properties.
- AWS SDK v2 does not permit specifying a choice between HTTP and HTTPS so the `discovery.ec2.protocol` setting is no longer effective.
- AWS SDK v2 does not accept an access key without a secret key or vice versa.

**Action:**
Test the upgrade in a non-production environment. Adapt your configuration to the new SDK functionality. This includes, but may not be limited to, the following items:
- If you use IMDS to determine the availability zone of a node or to obtain credentials for accessing the EC2 API, ensure that it supports the IMDSv2 protocol.
- If applicable, discontinue use of the `aws.secretKey` and `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` system properties.
- If applicable, specify that you wish to use the insecure HTTP protocol to access the EC2 API by setting `discovery.ec2.endpoint` to a URL which starts with `http://`.
- Either supply both an access key and a secret key using the `discovery.ec2.access_key` and `discovery.ec2.secret_key` keystore settings, or configure neither of these settings.

For more information, view [#122062](https://github.com/elastic/elasticsearch/pull/122062).
:::


ES|QL:
:::{dropdown} ES|QL now returns partial results by default
In previous versions, ES|QL queries failed entirely when any error occurred. As of 8.19.0, ES|QL returns partial results instead.

**Impact:**
Callers must check the `is_partial` flag in the response to determine whether the result is complete. Relying on full results without checking this flag may lead to incorrect assumptions about the response.

**Action:**
If partial results are not acceptable for your use case, you can disable this behavior by:
* Setting `allow_partial_results=false` in the query URL per request, or
* Setting the `esql.query.allow_partial_results` cluster setting to `false`.

For more information, view [#127351](https://github.com/elastic/elasticsearch/pull/127351) (issue: [#122802](https://github.com/elastic/elasticsearch/issues/122802))
:::

:::{dropdown} Disallows parentheses in unquoted index patterns in ES|QL
To avoid ambiguity with subquery syntax, ES|QL no longer allows the use of `(` and `)` in unquoted index patterns.

**Impact:**
Queries that include parentheses in unquoted index names will now result in a parsing exception.

**Action:**
Update affected queries to quote index names that contain parentheses. For example, use `FROM "("foo")"` instead of `FROM (foo)`.
For more information, view [#130427](https://github.com/elastic/elasticsearch/pull/130427) (issue: [#130378](https://github.com/elastic/elasticsearch/issues/130378))
:::

:::{dropdown} Disallows mixing quoted and unquoted components in `FROM` index patterns
ES|QL no longer allows mixing quoted and unquoted parts in `FROM` index patterns (e.g. `FROM remote:"index"`). Previously, such patterns were parsed inconsistently and could result in misleading runtime errors.

**Impact:**
Queries using partially quoted index patterns—such as quoting only the index or only the remote cluster—will now be rejected at parse time. This change simplifies grammar handling and avoids confusing validation failures.

**Action:**
Ensure index patterns are either fully quoted or fully unquoted. For example:
* Valid: `FROM "remote:index"` or `FROM remote:index`
* Invalid: `FROM remote:"index"`, `FROM "remote":index`

For more information, view [#127636](https://github.com/elastic/elasticsearch/pull/127636) (issue: [#122651](https://github.com/elastic/elasticsearch/issues/122651))
:::

:::{dropdown} `skip_unavailable` now catches all remote cluster runtime errors in ES|QL
When `skip_unavailable` is set to `true`, ES|QL now treats all runtime errors from that cluster as non-fatal. Previously, this setting only applied to connectivity issues (i.e. when a cluster was unavailable).

**Impact:**
Errors such as missing indices on a remote cluster will no longer cause the query to fail. Instead, the cluster will appear in the response metadata as `skipped` or `partial`.

**Action:**
If your workflows rely on detecting remote cluster errors, review your use of `skip_unavailable` and adjust error handling as needed.

For more information, view [#128163](https://github.com/elastic/elasticsearch/pull/128163)
:::


Snapshot/Restore:
:::{dropdown} Upgrades `repository-s3` plugin to AWS SDK v2
The `repository-s3` plugin now uses AWS SDK v2 instead of v1, as AWS will deprecate SDK v1 before the end of Elasticsearch 8.19’s support period. The two SDKs differ in behavior, which may require updates to your configuration.

**Impact:**
Existing `repository-s3` configurations may no longer be compatible. Notable differences in AWS SDK v2 include, but may not be limited to:
- AWS SDK v2 requires users to specify the region to use for signing requests, or else to run in an environment in which it can determine the correct region automatically. The older SDK used to determine the region based on the endpoint URL as specified with the `s3.client.${CLIENT_NAME}.endpoint` setting, together with other data drawn from the operating environment, and fell back to `us-east-1` if no better value was found.
- AWS SDK v2 does not support the EC2 IMDSv1 protocol.
- AWS SDK v2 does not support the `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` system property.
- AWS SDK v2 does not permit specifying a choice between HTTP and HTTPS so the `s3.client.${CLIENT_NAME}.protocol` setting is deprecated and no longer has any effect.
- AWS SDK v2 does not permit control over throttling for retries, so the `s3.client.${CLIENT_NAME}.use_throttle_retries` setting is deprecated and no longer has any effect.
- AWS SDK v2 requires the use of the V4 signature algorithm, therefore, the `s3.client.${CLIENT_NAME}.signer_override` setting is deprecated and no longer has any effect.
- AWS SDK v2 does not support the `log-delivery-write` canned ACL.
- AWS SDK v2 counts 4xx responses differently in its metrics reporting.
- AWS SDK v2 always uses the regional STS endpoint, whereas AWS SDK v2 could use either a regional endpoint or the global `https://sts.amazonaws.com` one.

**Action:**
Test the upgrade in a non-production environment. Adapt your configuration to the new SDK functionality. This includes, but may not be limited to, the following items:
- Specify the correct signing region using the `s3.client.${CLIENT_NAME}.region` setting on each node. {es} will try to determine the correct region based on the endpoint URL and other data drawn from the operating environment, but might not do so correctly in all cases.
- If you use IMDS to determine the availability zone of a node or to obtain credentials for accessing the EC2 API, ensure that it supports the IMDSv2 protocol.
- If applicable, discontinue use of the `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` system property.
- If applicable, specify that you wish to use the insecure HTTP protocol to access the S3 API by setting `s3.client.${CLIENT_NAME}.endpoint` to a URL which starts with `http://`.
- If applicable, discontinue use of the `log-delivery-write` canned ACL.

For more information, view [#126843](https://github.com/elastic/elasticsearch/pull/126843) (issue: [#120993](https://github.com/elastic/elasticsearch/issues/120993))
:::




## 9.0.4 [elasticsearch-9.0.4-breaking-changes]

There are no breaking changes associated with this release.

## 9.0.3 [elasticsearch-9.0.3-breaking-changes]

There are no breaking changes associated with this release.

## 9.0.2 [elasticsearch-9.0.2-breaking-changes]

Snapshot/Restore:
* Make S3 custom query parameter optional [#128043](https://github.com/elastic/elasticsearch/pull/128043)



## 9.0.1 [elasticsearch-9.0.1-breaking-changes]

There are no breaking changes associated with this release.

## 9.0.0 [elasticsearch-900-breaking-changes]

Aggregations:
* Remove date histogram boolean support [#118484](https://github.com/elastic/elasticsearch/pull/118484)

Allocation:
* Increase minimum threshold in shard balancer [#115831](https://github.com/elastic/elasticsearch/pull/115831)
* Remove `cluster.routing.allocation.disk.watermark.enable_for_single_data_node` setting [#114207](https://github.com/elastic/elasticsearch/pull/114207)
* Remove cluster state from `/_cluster/reroute` response [#114231](https://github.com/elastic/elasticsearch/pull/114231) (issue: [#88978](https://github.com/elastic/elasticsearch/issues/88978))

Analysis:
* Snowball stemmers have been upgraded [#114146](https://github.com/elastic/elasticsearch/pull/114146)
* The 'german2' stemmer is now an alias for the 'german' snowball stemmer [#113614](https://github.com/elastic/elasticsearch/pull/113614)
* The 'persian' analyzer has stemmer by default [#113482](https://github.com/elastic/elasticsearch/pull/113482) (issue: [#113050](https://github.com/elastic/elasticsearch/issues/113050))
* The Korean dictionary for Nori has been updated [#114124](https://github.com/elastic/elasticsearch/pull/114124)

Authentication:
* Configuring a bind DN in an LDAP or Active Directory (AD) realm without a corresponding bind password
will prevent node from starting [#118366](https://github.com/elastic/elasticsearch/pull/118366)

Cluster Coordination:
* Remove unsupported legacy value for `discovery.type` [#112903](https://github.com/elastic/elasticsearch/pull/112903)

EQL:
* Set allow_partial_search_results=true by default [#120267](https://github.com/elastic/elasticsearch/pull/120267)

Extract&Transform:
* Restrict Connector APIs to manage/monitor_connector privileges [#119863](https://github.com/elastic/elasticsearch/pull/119863)

Highlighting:
* Remove support for deprecated `force_source` highlighting parameter [#116943](https://github.com/elastic/elasticsearch/pull/116943)

Indices APIs:
* Apply more strict parsing of actions in bulk API [#115923](https://github.com/elastic/elasticsearch/pull/115923)
* Remove deprecated local attribute from alias APIs [#115393](https://github.com/elastic/elasticsearch/pull/115393)
* Remove the ability to read frozen indices [#120108](https://github.com/elastic/elasticsearch/pull/120108)
* Remove unfreeze REST endpoint [#119227](https://github.com/elastic/elasticsearch/pull/119227)

Infra/Core:
* Change Elasticsearch timeouts to 429 response instead of 5xx [#116026](https://github.com/elastic/elasticsearch/pull/116026)
* Limit `ByteSizeUnit` to 2 decimals [#120142](https://github.com/elastic/elasticsearch/pull/120142)
* Remove `client.type` setting [#118192](https://github.com/elastic/elasticsearch/pull/118192) (issue: [#104574](https://github.com/elastic/elasticsearch/issues/104574))
* Remove any references to org.elasticsearch.core.RestApiVersion#V_7 [#118103](https://github.com/elastic/elasticsearch/pull/118103)

Infra/Logging:
* Change `deprecation.elasticsearch` keyword to `elasticsearch.deprecation` [#117933](https://github.com/elastic/elasticsearch/pull/117933) (issue: [#83251](https://github.com/elastic/elasticsearch/issues/83251))
* Rename deprecation index template [#125606](https://github.com/elastic/elasticsearch/pull/125606) (issue: [#125445](https://github.com/elastic/elasticsearch/issues/125445))

Infra/Metrics:
* Deprecated tracing.apm.* settings got removed. [#119926](https://github.com/elastic/elasticsearch/pull/119926)

Infra/REST API:
* Output a consistent format when generating error json [#90529](https://github.com/elastic/elasticsearch/pull/90529) (issue: [#89387](https://github.com/elastic/elasticsearch/issues/89387))

Ingest Node:
* Remove `ecs` option on `user_agent` processor [#116077](https://github.com/elastic/elasticsearch/pull/116077)
* Remove ignored fallback option on GeoIP processor [#116112](https://github.com/elastic/elasticsearch/pull/116112)

Logs:
* Conditionally enable logsdb by default for data streams matching with logs-*-* pattern. [#121049](https://github.com/elastic/elasticsearch/pull/121049) (issue: [#106489](https://github.com/elastic/elasticsearch/issues/106489))

Machine Learning:
* Disable machine learning on macOS x86_64 [#104125](https://github.com/elastic/elasticsearch/pull/104125)

Mapping:
* Remove support for type, fields, `copy_to` and boost in metadata field definition [#118825](https://github.com/elastic/elasticsearch/pull/118825)
* Turn `_source` meta fieldmapper's mode attribute into a no-op [#119072](https://github.com/elastic/elasticsearch/pull/119072) (issue: [#118596](https://github.com/elastic/elasticsearch/issues/118596))

Search:
* Adjust `random_score` default field to `_seq_no` field [#118671](https://github.com/elastic/elasticsearch/pull/118671)
* Change Semantic Text To Act Like A Normal Text Field [#120813](https://github.com/elastic/elasticsearch/pull/120813)
* Remove legacy params from range query [#116970](https://github.com/elastic/elasticsearch/pull/116970)

Snapshot/Restore:
* Remove deprecated `xpack.searchable.snapshot.allocate_on_rolling_restart` setting [#114202](https://github.com/elastic/elasticsearch/pull/114202)

TLS:
* Drop `TLS_RSA` cipher support for JDK 24 [#123600](https://github.com/elastic/elasticsearch/pull/123600)
* Remove TLSv1.1 from default protocols [#121731](https://github.com/elastic/elasticsearch/pull/121731)

Transform:
* Remove `data_frame_transforms` roles [#117519](https://github.com/elastic/elasticsearch/pull/117519)

Vector Search:
* Remove old `_knn_search` tech preview API in v9 [#118104](https://github.com/elastic/elasticsearch/pull/118104)

Watcher:
* Removing support for types field in watcher search [#120748](https://github.com/elastic/elasticsearch/pull/120748)


