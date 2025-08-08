---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html
---

# Elasticsearch release notes [elasticsearch-release-notes]

Review the changes, fixes, and more in each version of Elasticsearch.

To check for security updates, go to [Security announcements for the Elastic stack](https://discuss.elastic.co/c/announcements/security-announcements/31).

% Release notes include only features, enhancements, and fixes. Add breaking changes, deprecations, and known issues to the applicable release notes sections.

% ## version.next [elasticsearch-next-release-notes]

% ### Features and enhancements [elasticsearch-next-features-enhancements]
% *

% ### Fixes [elasticsearch-next-fixes]
% *

## 9.1.1 [elasticsearch-9.1.1-release-notes]
```{applies_to}
stack: coming 9.1.1
```

### Fixes [elasticsearch-9.1.1-fixes]

Data streams:
:::{dropdown} Disables auto-sharding for LOOKUP index mode
Auto-sharding for data streams caused unsupported replica scaling when the index mode was set to `LOOKUP`. 
This happened because lookup mappers do not support scaling beyond one replica.
[#131429](https://github.com/elastic/elasticsearch/pull/131429) resolves this issue by disabling auto-sharding for data streams with `LOOKUP` index mode, avoiding unsupported replica settings.
:::

EQL:
:::{dropdown} Resolves EQL parsing failure for IP-mapped fields in `OR` expressions
Parsing EQL queries failed when comparing the same IP-mapped field to multiple values joined by an `OR` expression.
This occurred because lookup operators were internally rewritten into `IN` expressions, which are unsupported for IP-type fields.
[#132167](https://github.com/elastic/elasticsearch/pull/132167) resolves the issue and ensures EQL can now successfully parse and execute such or queries involving IP fields. (issue: [#118621](https://github.com/elastic/elasticsearch/issues/118621))
:::

ES|QL:
:::{dropdown} Fixes inconsistent equality and hashcode behavior for `ConstantNullBlock`
Inconsistent equality checks caused `constantNullBlock.equals(anyDoubleBlock)` to return false, even when `doubleBlock.equals(constantNullBlock)` returned true.
This asymmetry led to unreliable comparisons and mismatched hashcodes when `ConstantNullBlock` was functionally equivalent to other standard blocks.
[#131817](https://github.com/elastic/elasticsearch/pull/131817) resolves the issue and ensures both equality and hashcode functions are symmetric for these block types.
:::

:::{dropdown} Fixes `ConcurrentModificationException` caused by live operator list
A `ConcurrentModificationException` caused test failures in `CrossClusterAsyncEnrichStopIT.testEnrichAfterStop` under certain conditions.  
This happened because the ES|QL driver added a live operator list to the `DriverStatus` object, which could be modified while the status was being serialized.  
[#132260](https://github.com/elastic/elasticsearch/pull/132260) fixes the issue by copying the operator list before storing it, preventing concurrent changes during status reads.  
(issue: [#131564](https://github.com/elastic/elasticsearch/issues/131564))
:::

:::{dropdown} Prevents null pointer exception for `to_lower` and `to_upper` with no parameters
Calling the `to_lower` or `to_upper` functions with no parameters caused a null pointer exception (NPE), instead of returning a clear error.
This behavior was a result of an older implementation of these functions.
[#131917](https://github.com/elastic/elasticsearch/pull/131917) resolves the issue and ensures that empty parameter calls now return the correct error message. (issue: [#131913](https://github.com/elastic/elasticsearch/issues/131913))
:::

:::{dropdown} Fixes `aggregate_metric_double` decoding and `mv_expand` behavior on multi-index queries
Sorting across multiple indices failed when one index contained an `aggregate_metric_double` field and another did not.
In this case, the missing field was encoded as `NullBlock` but later incorrectly decoded as `AggregateMetricDoubleBlock`, which expects four values. This mismatch caused decoding errors.
[#131658](https://github.com/elastic/elasticsearch/pull/131658) resolves the issue and also improves `mv_expand` by returning the input block unchanged for unsupported `AggregateMetricDoubleBlock` values, avoiding unnecessary errors.
:::

:::{dropdown} Fixes incorrect `ingest_took` value when combining bulk responses
Combining two `BulkResponse` objects with `ingest_took` set to `NO_INGEST_TOOK` resulted in a combined `ingest_took` value of `-2`, which was invalid.
This occurred because the combination logic failed to preserve the sentinel `NO_INGEST_TOOK` constant.
[#132088](https://github.com/elastic/elasticsearch/pull/132088) resolves the issue and ensures the result is correctly set to `NO_INGEST_TOOK` when applicable.
:::

:::{dropdown} Disallows remote ENRICH after FORK in query plans
An invalid combination of `FORK` followed by a remote-only `ENRICH` caused incorrect query planning and failed executions. [#131945](https://github.com/elastic/elasticsearch/pull/131945) resolves the issue by explicitly disallowing this combination, preventing invalid plans from being executed. (issue: [#131445](https://github.com/elastic/elasticsearch/issues/131445))
:::

:::{dropdown} Adds support for splitting large pages on load to avoid memory pressure
Loading large rows from a single segment occasionally created oversized pages when decoding values row-by-row, particularly for text and geo fields.
This could cause memory pressure or degraded performance.
[#131053](https://github.com/elastic/elasticsearch/pull/131053) resolves the issue by estimating the size of each page as rows are loaded.
If the estimated size exceeds a configurable `jumbo` threshold (defaulting to one megabyte), row loading stops early, the page is returned, and remaining rows are processed in subsequent iterations.
This prevents loading incomplete or oversized pages during data aggregation.
:::

Infra/Core:
:::{dropdown} Grants server module read/write permissions for deprecated `path.shared_data` setting
Grants the server module read/write access to the deprecated `path.shared_data` setting.  
[#131680](https://github.com/elastic/elasticsearch/pull/131680) resolves issues surfaced in internal testing and ensures compatibility with legacy configurations.
:::

Ingest Node:
:::{dropdown} Fixes incorrect mapping resolution in simulate ingest API when `mapping_addition` is provided
When using the simulate ingest API with a `mapping_addition`, the system incorrectly ignored the existing mapping of the target index and instead applied the mapping from a matching index template, if one existed.
This caused mismatches between the index and simulation behavior.
[#132101](https://github.com/elastic/elasticsearch/pull/132101) resolves the issue and ensures that the index’s actual mapping is used when available, preserving consistency between simulation and execution.
:::

Machine Learning:
:::{dropdown} Prevents double-counting of allocations in trained model deployment memory estimation
A recent refactor introduced a bug that caused the trained model memory estimation to double-count the number of allocations, leading to inflated memory usage projections.
[#131990](https://github.com/elastic/elasticsearch/pull/131990) resolves the issue by reverting the change and restoring accurate memory estimation for trained model deployments.
:::

Mapping:
:::{dropdown} Fixes decoding failure for non-ASCII field names in `_ignored_source`
A decoding error occurred when field names in `_ignored_source` contained non-ASCII characters. 
This happened because `String.length()` was used to calculate the byte length of the field name, which only works correctly for ASCII characters.
[#132018](https://github.com/elastic/elasticsearch/pull/132018) resolves the issue by using the actual byte array length of the encoded field name, ensuring proper decoding regardless of character encoding.
:::

Search:
:::{dropdown} Fixes index sort compatibility for `date_nanos` fields in indices created before 7.14
Indices created prior to version 7.14 that used an index sort on a `date_nanos` field could not be opened in more recent versions due to a mismatch in the default `index.sort.missing` value.
A change in version 7.14 modified the default from `Long.MIN_VALUE` to `0L`, which caused newer versions to reject those older indices.
[#132162](https://github.com/elastic/elasticsearch/pull/132162) resolves the issue by restoring the expected default value for indices created before 7.14, allowing them to open successfully in newer versions. (issue: [#132040](https://github.com/elastic/elasticsearch/issues/132040))
:::

:::{dropdown} Fix missing removal of query cancellation callback in QueryPhase
The timeout cancellation callback registered in `QueryPhase` via `addQueryCancellation` was not removed after the query phase completed.
This caused unintended timeouts or cancellations during subsequent phases under specific conditions (such as large datasets, low timeouts, and partial search results enabled).
[#130279](https://github.com/elastic/elasticsearch/pull/130279) resolves the issue and ensures predictable behavior by reintroducing the cleanup logic. (issue: [#130071](https://github.com/elastic/elasticsearch/issues/130071))
:::


## 9.1.0 [elasticsearch-9.1.0-release-notes]

### Highlights [elasticsearch-9.1.0-highlights]

::::{dropdown} Upgrade `repository-s3` to AWS SDK v2
In earlier versions of {{es}} the `repository-s3` plugin was based on the AWS SDK v1. AWS will withdraw support for this SDK before the end of the life of {{es}} 9.1 so we have migrated this plugin to the newer AWS SDK v2.
The two SDKs are not quite compatible, so please check the breaking changes documentation and test the new version thoroughly before upgrading any production workloads.
::::

::::{dropdown} Add ability to redirect ingestion failures on data streams to a failure store
Documents that encountered ingest pipeline failures or mapping conflicts
would previously be returned to the client as errors in the bulk and
index operations. Many client applications are not equipped to respond
to these failures. This leads to the failed documents often being
dropped by the client which cannot hold the broken documents
indefinitely. In many end user workloads, these failed documents
represent events that could be critical signals for observability or
security use cases.

To help mitigate this problem, data streams can now maintain a "failure
store" which is used to accept and hold documents that fail to be
ingested due to preventable configuration errors. The data stream's
failure store operates like a separate set of backing indices with their
own mappings and access patterns that allow Elasticsearch to accept
documents that would otherwise be rejected due to unhandled ingest
pipeline exceptions or mapping conflicts.

Users can enable redirection of ingest failures to the failure store on
new data streams by specifying it in the new `data_stream_options` field
inside of a component or index template:

```yaml
PUT _index_template/my-template
{
  "index_patterns": ["logs-test-*"],
  "data_stream": {},
  "template": {
    "data_stream_options": {
      "failure_store": {
        "enabled": true
      }
    }
  }
}
```

Existing data streams can be configured with the new data stream
`_options` endpoint:

```yaml
PUT _data_stream/logs-test-apache/_options
{
  "failure_store": {
    "enabled": "true"
  }
}
```

When redirection is enabled, any ingestion related failures will be
captured in the failure store if the cluster is able to, along with the
timestamp that the failure occurred, details about the error
encountered, and the document that could not be ingested. Since failure
stores are a kind of Elasticsearch index, we can search the data stream
for the failures that it has collected. The failures are not shown by
default as they are stored in different indices than the normal data
stream data. In order to retrieve the failures, we use the `_search` API
along with a new bit of index pattern syntax, the `::` selector.

```yaml
POST logs-test-apache::failures/_search
```

This index syntax informs the search operation to target the indices in
its failure store instead of its backing indices. It can be mixed in a
number of ways with other index patterns to include their failure store
indices in the search operation:

```yaml
POST logs-*::failures/_search
POST logs-*,logs-*::failures/_search
POST *::failures/_search
POST _query
{
  "query": "FROM my_data_stream*::failures"
}
```
::::

::::{dropdown} Mark Token Pruning for Sparse Vector as GA
Token pruning for sparse_vector queries has been live since 8.13 as tech preview.
As of 8.19.0 and 9.1.0, this is now generally available.
::::

::::{dropdown} Upgrade to lucene 10.2.2
* Reduce NeighborArray on-heap memory during HNSW graph building
* Fix IndexSortSortedNumericDocValuesRangeQuery for integer sorting
* ValueSource.fromDoubleValuesSource(dvs).getSortField() would throw errors when used if the DoubleValuesSource needed scores
----
::::

::::{dropdown} Release FORK in tech preview
Fork is a foundational building block that allows multiple branches of execution.
Conceptually, fork is:
- a bifurcation of the stream, with all data going to each fork branch, followed by
- a merge of the branches, enhanced with a discriminator column called FORK:

Example:

```yaml
FROM test
| FORK
( WHERE content:"fox" )
( WHERE content:"dog" )
| SORT _fork
```

The FORK command add a discriminator column called `_fork`:

```yaml
| id  | content   | _fork |
|-----|-----------|-------|
| 3   | brown fox | fork1 |
| 4   | white dog | fork2 |
```

::::

::::{dropdown} ES|QL cross-cluster querying is now generally available
The ES|QL Cross-Cluster querying feature has been in technical preview since 8.13.
As of releases 8.19.0 and 9.1.0 this is now generally available.
This feature allows you to run ES|QL queries across multiple clusters.
::::

### Features and enhancements [elasticsearch-9.1.0-features-enhancements]

Allocation:
* Accumulate compute() calls and iterations between convergences [#126008](https://github.com/elastic/elasticsearch/pull/126008) (issue: [#100850](https://github.com/elastic/elasticsearch/issues/100850))
* Add `FailedShardEntry` info to shard-failed task source string [#125520](https://github.com/elastic/elasticsearch/pull/125520) (issue: [#102606](https://github.com/elastic/elasticsearch/issues/102606))
* Add cache support in `TransportGetAllocationStatsAction` [#124898](https://github.com/elastic/elasticsearch/pull/124898) (issue: [#110716](https://github.com/elastic/elasticsearch/issues/110716))
* Add cancellation support in `TransportGetAllocationStatsAction` [#127371](https://github.com/elastic/elasticsearch/pull/127371) (issue: [#123248](https://github.com/elastic/elasticsearch/issues/123248))
* Allow balancing weights to be set per tier [#126091](https://github.com/elastic/elasticsearch/pull/126091)
* Introduce `AllocationBalancingRoundSummaryService` [#120957](https://github.com/elastic/elasticsearch/pull/120957)
* More efficient sort in `tryRelocateShard` [#128063](https://github.com/elastic/elasticsearch/pull/128063)

Analysis:
* Synonyms API - Add refresh parameter to check synonyms index and reload analyzers [#126935](https://github.com/elastic/elasticsearch/pull/126935) (issue: [#121441](https://github.com/elastic/elasticsearch/issues/121441))

Authentication:
* Add Support for Providing a custom `ServiceAccountTokenStore` through `SecurityExtensions` [#126612](https://github.com/elastic/elasticsearch/pull/126612)
* Implement SAML custom attributes support for Identity Provider [#128176](https://github.com/elastic/elasticsearch/pull/128176)
* Permit at+jwt typ header value in jwt access tokens [#126687](https://github.com/elastic/elasticsearch/pull/126687) (issue: [#119370](https://github.com/elastic/elasticsearch/issues/119370))

Authorization:
* Add Microsoft Graph Delegated Authorization Realm Plugin [#127910](https://github.com/elastic/elasticsearch/pull/127910)
* Check `TooComplex` exception for `HasPrivileges` body [#128870](https://github.com/elastic/elasticsearch/pull/128870)
* Delegated authorization using Microsoft Graph (SDK) [#128396](https://github.com/elastic/elasticsearch/pull/128396)
* Fix unsupported privileges error message during role and API key crea… [#128858](https://github.com/elastic/elasticsearch/pull/128858) (issue: [#128132](https://github.com/elastic/elasticsearch/issues/128132))
* Granting `kibana_system` reserved role access to "all" privileges to `.adhoc.alerts*` and `.internal.adhoc.alerts*` indices [#127321](https://github.com/elastic/elasticsearch/pull/127321)
* [Security Solution] Add `read` index privileges to `kibana_system` role for Microsoft Defender integration indexes [#126803](https://github.com/elastic/elasticsearch/pull/126803)

CCS:
* Check if index patterns conform to valid format before validation [#122497](https://github.com/elastic/elasticsearch/pull/122497)

CRUD:
* Add `IndexingPressureMonitor` to monitor large indexing operations [#126372](https://github.com/elastic/elasticsearch/pull/126372)
* Enhance memory accounting for document expansion and introduce max document size limit [#123543](https://github.com/elastic/elasticsearch/pull/123543)

Codec:
* First step optimizing tsdb doc values codec merging [#125403](https://github.com/elastic/elasticsearch/pull/125403)
* Use default Lucene postings format when index mode is standard. [#128509](https://github.com/elastic/elasticsearch/pull/128509)

Data streams:
* Add ability to redirect ingestion failures on data streams to a failure store [#126973](https://github.com/elastic/elasticsearch/pull/126973)
* Add index mode to get data stream API [#122486](https://github.com/elastic/elasticsearch/pull/122486)
* Run `TransportGetDataStreamLifecycleAction` on local node [#125214](https://github.com/elastic/elasticsearch/pull/125214)
* Run `TransportGetDataStreamOptionsAction` on local node [#125213](https://github.com/elastic/elasticsearch/pull/125213)
* Run `TransportGetDataStreamsAction` on local node [#122852](https://github.com/elastic/elasticsearch/pull/122852)
* Update ecs@mappings.json with new GenAI fields [#129122](https://github.com/elastic/elasticsearch/pull/129122)
* [Failure store] Introduce dedicated failure store lifecycle configuration [#127314](https://github.com/elastic/elasticsearch/pull/127314)
* [Failure store] Introduce default retention for failure indices [#127573](https://github.com/elastic/elasticsearch/pull/127573)
* [apm-data] Enable 'date_detection' for all apm data streams [#128913](https://github.com/elastic/elasticsearch/pull/128913)

Distributed:
* Account for time taken to write index buffers in `IndexingMemoryController` [#126786](https://github.com/elastic/elasticsearch/pull/126786)

ES|QL:
* Add MATCH_PHRASE [#127661](https://github.com/elastic/elasticsearch/pull/127661)
* Add Support for LIKE (LIST) [#129170](https://github.com/elastic/elasticsearch/pull/129170)
* Add `documents_found` and `values_loaded` [#125631](https://github.com/elastic/elasticsearch/pull/125631)
* Add `suggested_cast` [#127139](https://github.com/elastic/elasticsearch/pull/127139)
* Add emit time to hash aggregation status [#127988](https://github.com/elastic/elasticsearch/pull/127988)
* Add initial grammar and changes for FORK [#121948](https://github.com/elastic/elasticsearch/pull/121948)
* Add initial grammar and planning for RRF (snapshot) [#123396](https://github.com/elastic/elasticsearch/pull/123396)
* Add local optimizations for `constant_keyword` [#127549](https://github.com/elastic/elasticsearch/pull/127549)
* Add optimization to purge join on null merge key [#127583](https://github.com/elastic/elasticsearch/pull/127583) (issue: [#125577](https://github.com/elastic/elasticsearch/issues/125577))
* Add support for LOOKUP JOIN on aliases [#128519](https://github.com/elastic/elasticsearch/pull/128519)
* Add support for parameters in LIMIT command [#128464](https://github.com/elastic/elasticsearch/pull/128464)
* Aggressive release of shard contexts [#129454](https://github.com/elastic/elasticsearch/pull/129454)
* Allow lookup join on mixed numeric fields [#128263](https://github.com/elastic/elasticsearch/pull/128263)
* Allow partial results in ES|QL [#121942](https://github.com/elastic/elasticsearch/pull/121942)
* Avoid `NamedWritable` in block serialization [#124394](https://github.com/elastic/elasticsearch/pull/124394)
* COMPLETION command grammar and logical plan [#126319](https://github.com/elastic/elasticsearch/pull/126319)
* Calculate concurrent node limit [#124901](https://github.com/elastic/elasticsearch/pull/124901)
* Change queries ID to be the same as the async [#127472](https://github.com/elastic/elasticsearch/pull/127472) (issue: [#127187](https://github.com/elastic/elasticsearch/issues/127187))
* Double parameter markers for identifiers [#122459](https://github.com/elastic/elasticsearch/pull/122459)
* ESQL: Enhanced `DATE_TRUNC` with arbitrary intervals [#120302](https://github.com/elastic/elasticsearch/pull/120302) (issue: [#120094](https://github.com/elastic/elasticsearch/issues/120094))
* ES|QL - Add COMPLETION command as a tech preview feature [#128948](https://github.com/elastic/elasticsearch/pull/128948) (issue: [#124405](https://github.com/elastic/elasticsearch/issues/124405))
* ES|QL - Add `match_phrase` full text function (tech preview) [#128925](https://github.com/elastic/elasticsearch/pull/128925)
* ES|QL - Allow full text functions to be used in STATS [#125479](https://github.com/elastic/elasticsearch/pull/125479) (issue: [#125481](https://github.com/elastic/elasticsearch/issues/125481))
* ES|QL cross-cluster querying is now generally available [#130032](https://github.com/elastic/elasticsearch/pull/130032)
* ES|QL slow log [#124094](https://github.com/elastic/elasticsearch/pull/124094)
* ES|QL: Support `::date` in inline cast [#123460](https://github.com/elastic/elasticsearch/pull/123460) (issue: [#116746](https://github.com/elastic/elasticsearch/issues/116746))
* Emit ordinal output block for values aggregate [#127201](https://github.com/elastic/elasticsearch/pull/127201)
* Fix sorting when `aggregate_metric_double` present [#125191](https://github.com/elastic/elasticsearch/pull/125191)
* Heuristics to pick efficient partitioning [#125739](https://github.com/elastic/elasticsearch/pull/125739)
* Implement runtime skip_unavailable=true [#121240](https://github.com/elastic/elasticsearch/pull/121240)
* Include failures in partial response [#124929](https://github.com/elastic/elasticsearch/pull/124929)
* Infer the score mode to use from the Lucene collector [#125930](https://github.com/elastic/elasticsearch/pull/125930)
* Introduce `AggregateMetricDoubleBlock` [#127299](https://github.com/elastic/elasticsearch/pull/127299)
* Introduce `allow_partial_results` setting in ES|QL [#122890](https://github.com/elastic/elasticsearch/pull/122890)
* Introduce a pre-mapping logical plan processing step [#121260](https://github.com/elastic/elasticsearch/pull/121260)
* Keep ordinals in conversion functions [#125357](https://github.com/elastic/elasticsearch/pull/125357)
* List/get query API [#124832](https://github.com/elastic/elasticsearch/pull/124832) (issue: [#124827](https://github.com/elastic/elasticsearch/issues/124827))
* Log partial failures [#129164](https://github.com/elastic/elasticsearch/pull/129164)
* Optimize ordinal inputs in Values aggregation [#127849](https://github.com/elastic/elasticsearch/pull/127849)
* Pragma to load from stored fields [#122891](https://github.com/elastic/elasticsearch/pull/122891)
* Push more `==`s on text fields to lucene [#126641](https://github.com/elastic/elasticsearch/pull/126641)
* Pushdown Lookup Join past Project [#129503](https://github.com/elastic/elasticsearch/pull/129503) (issue: [#119082](https://github.com/elastic/elasticsearch/issues/119082))
* Pushdown constructs doing case-insensitive regexes [#128393](https://github.com/elastic/elasticsearch/pull/128393) (issue: [#127479](https://github.com/elastic/elasticsearch/issues/127479))
* Pushdown for LIKE (LIST) [#129557](https://github.com/elastic/elasticsearch/pull/129557)
* ROUND_TO function [#128278](https://github.com/elastic/elasticsearch/pull/128278)
* Release FORK in tech preview [#129606](https://github.com/elastic/elasticsearch/pull/129606)
* Remove page alignment in exchange sink [#124610](https://github.com/elastic/elasticsearch/pull/124610)
* Render `aggregate_metric_double` [#122660](https://github.com/elastic/elasticsearch/pull/122660)
* Report `original_types` [#124913](https://github.com/elastic/elasticsearch/pull/124913)
* Report failures on partial results [#124823](https://github.com/elastic/elasticsearch/pull/124823)
* Retry ES|QL node requests on shard level failures [#120774](https://github.com/elastic/elasticsearch/pull/120774)
* Retry shard movements during ESQL query [#126653](https://github.com/elastic/elasticsearch/pull/126653)
* Run coordinating `can_match` in field-caps [#127734](https://github.com/elastic/elasticsearch/pull/127734)
* Skip unused STATS groups by adding a Top N `BlockHash` implementation [#127148](https://github.com/elastic/elasticsearch/pull/127148)
* Specialize ags `AddInput` for each block type [#127582](https://github.com/elastic/elasticsearch/pull/127582)
* Speed loading stored fields [#127348](https://github.com/elastic/elasticsearch/pull/127348)
* Support partial results in CCS in ES|QL [#122708](https://github.com/elastic/elasticsearch/pull/122708)
* Support subset of metrics in aggregate metric double [#121805](https://github.com/elastic/elasticsearch/pull/121805)
* Take double parameter markers for identifiers out of snapshot [#125690](https://github.com/elastic/elasticsearch/pull/125690)
* `ToAggregateMetricDouble` function [#124595](https://github.com/elastic/elasticsearch/pull/124595)
* `text ==` and `text !=` pushdown [#127355](https://github.com/elastic/elasticsearch/pull/127355)

Engine:
* Throttle indexing when disk IO throttling is disabled [#129245](https://github.com/elastic/elasticsearch/pull/129245)
* Track & log when there is insufficient disk space available to execute merges [#131711](https://github.com/elastic/elasticsearch/pull/131711)

Geo:
* Support explicit Z/M attributes using WKT geometry [#125896](https://github.com/elastic/elasticsearch/pull/125896) (issue: [#123111](https://github.com/elastic/elasticsearch/issues/123111))

Health:
* Add health indicator impact to `HealthPeriodicLogger` [#122390](https://github.com/elastic/elasticsearch/pull/122390)

ILM+SLM:
* Add `index.lifecycle.skip` index-scoped setting to instruct ILM to skip processing specific indices [#128736](https://github.com/elastic/elasticsearch/pull/128736)
* Batch ILM policy cluster state updates [#122917] [#126529](https://github.com/elastic/elasticsearch/pull/126529) (issue: [#122917](https://github.com/elastic/elasticsearch/issues/122917))
* Improve SLM Health Indicator to cover missing snapshot [#121370](https://github.com/elastic/elasticsearch/pull/121370)
* Optimize usage calculation in ILM policies retrieval API [#106953](https://github.com/elastic/elasticsearch/pull/106953) (issue: [#105773](https://github.com/elastic/elasticsearch/issues/105773))
* Process ILM cluster state updates on another thread [#123712](https://github.com/elastic/elasticsearch/pull/123712)
* Run `TransportExplainLifecycleAction` on local node [#122885](https://github.com/elastic/elasticsearch/pull/122885)
* Run `TransportGetLifecycleAction` on local node [#126002](https://github.com/elastic/elasticsearch/pull/126002)
* Run `TransportGetStatusAction` on local node [#129367](https://github.com/elastic/elasticsearch/pull/129367)
* Truncate `step_info` and error reason in ILM execution state and history [#125054](https://github.com/elastic/elasticsearch/pull/125054) (issue: [#124181](https://github.com/elastic/elasticsearch/issues/124181))

IdentityProvider:
* Add "extension" attribute validation to IdP SPs [#128805](https://github.com/elastic/elasticsearch/pull/128805)
* Add transport version support for IDP_CUSTOM_SAML_ATTRIBUTES_ADDED_8_19 [#128798](https://github.com/elastic/elasticsearch/pull/128798)

Indices APIs:
* Add RemoveBlock API to allow `DELETE /{index}/_block/{block}` [#129128](https://github.com/elastic/elasticsearch/pull/129128)
* Avoid creating known_fields for every check in Alias [#124690](https://github.com/elastic/elasticsearch/pull/124690)
* Run `TransportGetIndexAction` on local node [#125652](https://github.com/elastic/elasticsearch/pull/125652)
* Run `TransportGetMappingsAction` on local node [#122921](https://github.com/elastic/elasticsearch/pull/122921)
* Run `TransportGetSettingsAction` on local node [#126051](https://github.com/elastic/elasticsearch/pull/126051)
* Throw exception for unknown token in RestIndexPutAliasAction [#124708](https://github.com/elastic/elasticsearch/pull/124708)
* Throw exception for unsupported values type in Alias [#124737](https://github.com/elastic/elasticsearch/pull/124737)

Inference:
* Adding Google VertexAI chat completion integration [#128105](https://github.com/elastic/elasticsearch/pull/128105)
* Adding Google VertexAI completion integration [#128694](https://github.com/elastic/elasticsearch/pull/128694)
* [Inference API] Rename `model_id` prop to model in EIS sparse inference request body [#122272](https://github.com/elastic/elasticsearch/pull/122272)

Infra/CLI:
* Use logs dir as working directory [#124966](https://github.com/elastic/elasticsearch/pull/124966)

Infra/Core:
* Give Kibana user 'all' permissions for .entity_analytics.* indices [#123588](https://github.com/elastic/elasticsearch/pull/123588)
* Improve support for bytecode patching signed jars [#128613](https://github.com/elastic/elasticsearch/pull/128613)
* Permanently switch from Java SecurityManager to Entitlements. The Java SecurityManager has been deprecated since Java 17, and it is now completely disabled in Java 24. In order to retain an similar level of protection, Elasticsearch implemented its own protection mechanism, Entitlements. Starting with this version, Entitlements will permanently replace the Java SecurityManager. [#125117](https://github.com/elastic/elasticsearch/pull/125117)

Infra/Metrics:
* Add thread pool utilization metric [#120363](https://github.com/elastic/elasticsearch/pull/120363)
* Publish queue latency metrics from tracked thread pools [#120488](https://github.com/elastic/elasticsearch/pull/120488)

Infra/Settings:
* Allow float settings to be configured with other settings as default [#126751](https://github.com/elastic/elasticsearch/pull/126751)
* Allow passing several reserved state chunks in single process call [#124574](https://github.com/elastic/elasticsearch/pull/124574)
* Ensure config reload on ..data symlink switch for CSI driver support [#127628](https://github.com/elastic/elasticsearch/pull/127628)
* `FileWatchingService` shoudld not throw for missing file [#126264](https://github.com/elastic/elasticsearch/pull/126264)

Ingest Node:
* Adding `NormalizeForStreamProcessor` [#125699](https://github.com/elastic/elasticsearch/pull/125699)
* Run `TransportEnrichStatsAction` on local node [#121256](https://github.com/elastic/elasticsearch/pull/121256)

Logs:
* Conditionally force sequential reading in `LuceneSyntheticSourceChangesSnapshot` [#128473](https://github.com/elastic/elasticsearch/pull/128473)

Machine Learning:
* Add Custom inference service [#127939](https://github.com/elastic/elasticsearch/pull/127939)
* Add Telemetry for models without adaptive allocations [#129161](https://github.com/elastic/elasticsearch/pull/129161)
* Add `ModelRegistryMetadata` to Cluster State [#121106](https://github.com/elastic/elasticsearch/pull/121106)
* Add `none` chunking strategy to disable automatic chunking for inference endpoints [#129150](https://github.com/elastic/elasticsearch/pull/129150)
* Add recursive chunker [#126866](https://github.com/elastic/elasticsearch/pull/126866)
* Added Mistral Chat Completion support to the Inference Plugin [#128538](https://github.com/elastic/elasticsearch/pull/128538)
* Adding VoyageAI's v3.5 models [#128241](https://github.com/elastic/elasticsearch/pull/128241)
* Adding common rerank options to Perform Inference API [#125239](https://github.com/elastic/elasticsearch/pull/125239) (issue: [#111273](https://github.com/elastic/elasticsearch/issues/111273))
* Adding elser default endpoint for EIS [#122066](https://github.com/elastic/elasticsearch/pull/122066)
* Adding endpoint creation validation to `ElasticInferenceService` [#117642](https://github.com/elastic/elasticsearch/pull/117642)
* Adding integration for VoyageAI embeddings and rerank models [#122134](https://github.com/elastic/elasticsearch/pull/122134)
* Adding support for binary embedding type to Cohere service embedding type [#120751](https://github.com/elastic/elasticsearch/pull/120751)
* Adding support for specifying embedding type to Jina AI service settings [#121548](https://github.com/elastic/elasticsearch/pull/121548)
* Adding validation to `ElasticsearchInternalService` [#123044](https://github.com/elastic/elasticsearch/pull/123044)
* Bedrock Cohere Task Settings Support [#126493](https://github.com/elastic/elasticsearch/pull/126493) (issue: [#126156](https://github.com/elastic/elasticsearch/issues/126156))
* ES|QL SAMPLE aggregation function [#127629](https://github.com/elastic/elasticsearch/pull/127629)
* ES|QL `change_point` processing command [#120998](https://github.com/elastic/elasticsearch/pull/120998)
* ES|QL random sampling [#125570](https://github.com/elastic/elasticsearch/pull/125570)
* Expose `input_type` option at root level for `text_embedding` task type in Perform Inference API [#122638](https://github.com/elastic/elasticsearch/pull/122638) (issue: [#117856](https://github.com/elastic/elasticsearch/issues/117856))
* Improve exception for trained model deployment scale up timeout [#128218](https://github.com/elastic/elasticsearch/pull/128218)
* Increment inference stats counter for shard bulk inference calls [#129140](https://github.com/elastic/elasticsearch/pull/129140)
* Integrate `OpenAi` Chat Completion in `SageMaker` [#127767](https://github.com/elastic/elasticsearch/pull/127767)
* Integrate with `DeepSeek` API [#122218](https://github.com/elastic/elasticsearch/pull/122218)
* Limit the number of chunks for semantic text to prevent high memory usage [#123150](https://github.com/elastic/elasticsearch/pull/123150)
* Make Adaptive Allocations Scale to Zero configurable and set default to 24h [#128914](https://github.com/elastic/elasticsearch/pull/128914)
* Mark token pruning for sparse vector as GA [#128854](https://github.com/elastic/elasticsearch/pull/128854)
* Move to the Cohere V2 API for new inference endpoints [#129884](https://github.com/elastic/elasticsearch/pull/129884)
* Semantic Text Chunking Indexing Pressure [#125517](https://github.com/elastic/elasticsearch/pull/125517)
* Track memory used in the hierarchical results normalizer [#2831](https://github.com/elastic/ml-cpp/pull/2831)
* Upgrade AWS v2 SDK to 2.30.38 [#124738](https://github.com/elastic/elasticsearch/pull/124738)
* [Inference API] Propagate product use case http header to EIS [#124025](https://github.com/elastic/elasticsearch/pull/124025)
* [ML] Add HuggingFace Chat Completion support to the Inference Plugin [#127254](https://github.com/elastic/elasticsearch/pull/127254)
* [ML] Add Rerank support to the Inference Plugin [#127966](https://github.com/elastic/elasticsearch/pull/127966)
* [ML] Integrate SageMaker with OpenAI Embeddings [#126856](https://github.com/elastic/elasticsearch/pull/126856)
* `InferenceService` support aliases [#128584](https://github.com/elastic/elasticsearch/pull/128584)
* `SageMaker` Elastic Payload [#129413](https://github.com/elastic/elasticsearch/pull/129413)

Mapping:
* Add `index_options` to `semantic_text` field mappings [#119967](https://github.com/elastic/elasticsearch/pull/119967)
* Add block loader from stored field and source for ip field [#126644](https://github.com/elastic/elasticsearch/pull/126644)
* Do not respect synthetic_source_keep=arrays if type parses arrays [#127796](https://github.com/elastic/elasticsearch/pull/127796) (issue: [#126155](https://github.com/elastic/elasticsearch/issues/126155))
* Enable synthetic recovery source by default when synthetic source is enabled. Using synthetic recovery source significantly improves indexing performance compared to regular recovery source. [#122615](https://github.com/elastic/elasticsearch/pull/122615) (issue: [#116726](https://github.com/elastic/elasticsearch/issues/116726))
* Enable the use of nested field type with index.mode=time_series [#122224](https://github.com/elastic/elasticsearch/pull/122224) (issue: [#120874](https://github.com/elastic/elasticsearch/issues/120874))
* Exclude `semantic_text` subfields from field capabilities API [#127664](https://github.com/elastic/elasticsearch/pull/127664)
* Improved error message when index field type is invalid [#122860](https://github.com/elastic/elasticsearch/pull/122860)
* Introduce `FallbackSyntheticSourceBlockLoader` and apply it to keyword fields [#119546](https://github.com/elastic/elasticsearch/pull/119546)
* Refactor `SourceProvider` creation to consistently use `MappingLookup` [#128213](https://github.com/elastic/elasticsearch/pull/128213)
* Skip indexing points for `seq_no` in tsdb and logsdb [#128139](https://github.com/elastic/elasticsearch/pull/128139)
* Store arrays offsets for boolean fields natively with synthetic source [#125529](https://github.com/elastic/elasticsearch/pull/125529)
* Store arrays offsets for ip fields natively with synthetic source [#122999](https://github.com/elastic/elasticsearch/pull/122999)
* Store arrays offsets for keyword fields natively with synthetic source instead of falling back to ignored source. [#113757](https://github.com/elastic/elasticsearch/pull/113757)
* Store arrays offsets for numeric fields natively with synthetic source [#124594](https://github.com/elastic/elasticsearch/pull/124594)
* Store arrays offsets for unsigned long fields natively with synthetic source [#125709](https://github.com/elastic/elasticsearch/pull/125709)
* Update `sparse_vector` field mapping to include default setting for token pruning [#129089](https://github.com/elastic/elasticsearch/pull/129089)
* Use `FallbackSyntheticSourceBlockLoader` for `shape` and `geo_shape` [#124927](https://github.com/elastic/elasticsearch/pull/124927)
* Use `FallbackSyntheticSourceBlockLoader` for `unsigned_long` and `scaled_float` fields [#122637](https://github.com/elastic/elasticsearch/pull/122637)
* Use `FallbackSyntheticSourceBlockLoader` for boolean and date fields [#124050](https://github.com/elastic/elasticsearch/pull/124050)
* Use `FallbackSyntheticSourceBlockLoader` for number fields [#122280](https://github.com/elastic/elasticsearch/pull/122280)
* Use `FallbackSyntheticSourceBlockLoader` for point and `geo_point` [#125816](https://github.com/elastic/elasticsearch/pull/125816)
* Use `FallbackSyntheticSourceBlockLoader` for text fields [#126237](https://github.com/elastic/elasticsearch/pull/126237)

Network:
* Move HTTP content aggregation from Netty into `RestController` [#129302](https://github.com/elastic/elasticsearch/pull/129302) (issue: [#120746](https://github.com/elastic/elasticsearch/issues/120746))
* Remove first `FlowControlHandler` from HTTP pipeline [#128099](https://github.com/elastic/elasticsearch/pull/128099)
* Replace auto-read with proper flow-control in HTTP pipeline [#127817](https://github.com/elastic/elasticsearch/pull/127817)
* Set `connection: close` header on shutdown [#128025](https://github.com/elastic/elasticsearch/pull/128025) (issue: [#127984](https://github.com/elastic/elasticsearch/issues/127984))

Ranking:
* Adding ES|QL Reranker command in snapshot builds [#123074](https://github.com/elastic/elasticsearch/pull/123074)
* Leverage scorer supplier in `QueryFeatureExtractor` [#125259](https://github.com/elastic/elasticsearch/pull/125259)

Recovery:
* Move unpromotable relocations to its own transport action [#127330](https://github.com/elastic/elasticsearch/pull/127330)

Relevance:
* Add l2_norm normalization support to linear retriever [#128504](https://github.com/elastic/elasticsearch/pull/128504)
* Add pinned retriever [#126401](https://github.com/elastic/elasticsearch/pull/126401)
* Default new `semantic_text` fields to use BBQ when models are compatible [#126629](https://github.com/elastic/elasticsearch/pull/126629)
* Skip semantic_text embedding generation when no content is provided. [#123763](https://github.com/elastic/elasticsearch/pull/123763)
* Support configurable chunking in `semantic_text` fields [#121041](https://github.com/elastic/elasticsearch/pull/121041)

Search:
* Account for the `SearchHit` source in circuit breaker [#121920](https://github.com/elastic/elasticsearch/pull/121920) (issue: [#89656](https://github.com/elastic/elasticsearch/issues/89656))
* Add `bucketedSort` based on int [#128848](https://github.com/elastic/elasticsearch/pull/128848)
* Add initial version (behind snapshot) of `multi_match` function #121525 [#125062](https://github.com/elastic/elasticsearch/pull/125062) (issue: [#121525](https://github.com/elastic/elasticsearch/issues/121525))
* Add min score linear retriever [#129359](https://github.com/elastic/elasticsearch/pull/129359)
* ESQL - Enable telemetry for COMPLETION command [#127731](https://github.com/elastic/elasticsearch/pull/127731)
* Enable sort optimization on int, short and byte fields [#127968](https://github.com/elastic/elasticsearch/pull/127968) (issue: [#127965](https://github.com/elastic/elasticsearch/issues/127965))
* Introduce batched query execution and data-node side reduce [#121885](https://github.com/elastic/elasticsearch/pull/121885)
* Optimize memory usage in `ShardBulkInferenceActionFilter` [#124313](https://github.com/elastic/elasticsearch/pull/124313)
* Optionally allow text similarity reranking to fail [#121784](https://github.com/elastic/elasticsearch/pull/121784)
* Restore model registry validation for the semantic text field [#127285](https://github.com/elastic/elasticsearch/pull/127285)
* Return float[] instead of List<Double> in `valueFetcher` [#126702](https://github.com/elastic/elasticsearch/pull/126702)
* Simplified Linear Retriever [#129200](https://github.com/elastic/elasticsearch/pull/129200)
* Simplified RRF Retriever [#129659](https://github.com/elastic/elasticsearch/pull/129659)
* Upgrade to Lucene 10.2.0 [#126594](https://github.com/elastic/elasticsearch/pull/126594)
* Upgrade to Lucene 10.2.1 [#127343](https://github.com/elastic/elasticsearch/pull/127343)
* Upgrade to Lucene 10.2.2 [#129546](https://github.com/elastic/elasticsearch/pull/129546)
* Wrap remote errors with cluster name to provide more context [#123156](https://github.com/elastic/elasticsearch/pull/123156)

Snapshot/Restore:
* Add GCS telemetry with `ThreadLocal` [#125452](https://github.com/elastic/elasticsearch/pull/125452)
* Add `state` query param to Get snapshots API [#128635](https://github.com/elastic/elasticsearch/pull/128635) (issue: [#97446](https://github.com/elastic/elasticsearch/issues/97446))
* Allow missing shard stats for restarted nodes for `_snapshot/_status` [#128399](https://github.com/elastic/elasticsearch/pull/128399)
* GCS blob store: add `OperationPurpose/Operation` stats counters [#122991](https://github.com/elastic/elasticsearch/pull/122991)
* Improve get-snapshots message for unreadable repository [#128273](https://github.com/elastic/elasticsearch/pull/128273)
* Optimize shared blob cache evictions on shard removal Shared blob cache evictions occur on the cluster applier thread when shards are removed from a node. These can be expensive if a large number of shards are being removed. This change uses the context of the removal to avoid unnecessary evictions that might hold up the applier thread.  [#126581](https://github.com/elastic/elasticsearch/pull/126581)
* Retry when the server can't be resolved (Google Cloud Storage) [#123852](https://github.com/elastic/elasticsearch/pull/123852)
* Upgrade AWS Java SDK to 2.31.78 [#131050](https://github.com/elastic/elasticsearch/pull/131050)
* Upgrade to repository-gcs to use com.google.cloud:google-cloud-storage-bom:2.50.0 [#126087](https://github.com/elastic/elasticsearch/pull/126087)
* [Draft] Support concurrent multipart uploads in Azure [#128449](https://github.com/elastic/elasticsearch/pull/128449)

Stats:
* Run XPack usage actions on local node [#122933](https://github.com/elastic/elasticsearch/pull/122933)

Task Management:
* React more prompty to task cancellation while waiting for the cluster to unblock [#128737](https://github.com/elastic/elasticsearch/pull/128737) (issue: [#117971](https://github.com/elastic/elasticsearch/issues/117971))

Vector Search:
* Add bit vector support to semantic text [#123187](https://github.com/elastic/elasticsearch/pull/123187)
* Add dense vector off-heap stats to Node stats and Index stats APIs [#126704](https://github.com/elastic/elasticsearch/pull/126704)
* Add option to include or exclude vectors from `_source` retrieval [#128735](https://github.com/elastic/elasticsearch/pull/128735)
* Add panama implementations of byte-bit and float-bit script operations [#124722](https://github.com/elastic/elasticsearch/pull/124722) (issue: [#117096](https://github.com/elastic/elasticsearch/issues/117096))
* Adds implementations of dotProduct and cosineSimilarity painless methods to operate on float vectors for byte fields [#122381](https://github.com/elastic/elasticsearch/pull/122381) (issue: [#117274](https://github.com/elastic/elasticsearch/issues/117274))
* Allow zero for `rescore_vector.oversample` to indicate by-passing oversample and rescoring [#125599](https://github.com/elastic/elasticsearch/pull/125599)
* Define a default oversample value for dense vectors with bbq_hnsw/bbq_flat [#127134](https://github.com/elastic/elasticsearch/pull/127134)
* Improve HNSW filtered search speed through new heuristic [#126876](https://github.com/elastic/elasticsearch/pull/126876)
* Make `dense_vector` fields updatable to bbq_flat/bbq_hnsw [#128291](https://github.com/elastic/elasticsearch/pull/128291)
* Mark `rescore_vector` as generally available [#126038](https://github.com/elastic/elasticsearch/pull/126038)
* New `vector_rescore` parameter as a quantized index type option [#124581](https://github.com/elastic/elasticsearch/pull/124581)
* Panama vector accelerated optimized scalar quantization [#127118](https://github.com/elastic/elasticsearch/pull/127118)

Watcher:
* Run `TransportGetWatcherSettingsAction` on local node [#122857](https://github.com/elastic/elasticsearch/pull/122857)

### Fixes [elasticsearch-9.1.0-fixes]

Aggregations:
* Bypass competitive iteration in single filter bucket case [#127267](https://github.com/elastic/elasticsearch/pull/127267) (issue: [#127262](https://github.com/elastic/elasticsearch/issues/127262))
* Temporarily bypass competitive iteration for filters aggregation [#126956](https://github.com/elastic/elasticsearch/pull/126956)

Allocation:
* `DesiredBalanceReconciler` always returns `AllocationStats` [#122458](https://github.com/elastic/elasticsearch/pull/122458)

Analysis:
* Add refresh to synonyms put / delete APIs to wait for synonyms to be accessible and reload analyzers [#126314](https://github.com/elastic/elasticsearch/pull/126314) (issue: [#121441](https://github.com/elastic/elasticsearch/issues/121441))

Cluster Coordination:
* Disable logging in `ClusterFormationFailureHelper` on shutdown [#125244](https://github.com/elastic/elasticsearch/pull/125244) (issue: [#105559](https://github.com/elastic/elasticsearch/issues/105559))

Data streams:
* Move streams status actions to cluster:monitor group [#131015](https://github.com/elastic/elasticsearch/pull/131015)
* [apm-data] Set `event.dataset` if empty for logs [#129074](https://github.com/elastic/elasticsearch/pull/129074)

Distributed:
* Fix incorrect accounting of semantic text indexing memory pressure [#130221](https://github.com/elastic/elasticsearch/pull/130221)
* Modify the mechanism to pause indexing [#128405](https://github.com/elastic/elasticsearch/pull/128405)
* Pass `IndexReshardingMetadata` over the wire [#124841](https://github.com/elastic/elasticsearch/pull/124841)

ES|QL:
* Added Sample operator `NamedWritable` to plugin [#131541](https://github.com/elastic/elasticsearch/pull/131541)
* Disable a bugged commit [#127199](https://github.com/elastic/elasticsearch/pull/127199) (issue: [#127197](https://github.com/elastic/elasticsearch/issues/127197))
* Disallow remote enrich after lu join [#131426](https://github.com/elastic/elasticsearch/pull/131426) (issue: [#129372](https://github.com/elastic/elasticsearch/issues/129372))
* ESQL: Fix `NULL` handling in `IN` clause [#125832](https://github.com/elastic/elasticsearch/pull/125832) (issue: [#119950](https://github.com/elastic/elasticsearch/issues/119950))
* ESQL: Fix `mv_expand` inconsistent column order [#129745](https://github.com/elastic/elasticsearch/pull/129745) (issue: [#129000](https://github.com/elastic/elasticsearch/issues/129000))
* ESQL: Fix inconsistent results in using scaled_float field [#122586](https://github.com/elastic/elasticsearch/pull/122586) (issue: [#122547](https://github.com/elastic/elasticsearch/issues/122547))
* ESQL: Preserve single aggregate when all attributes are pruned [#126397](https://github.com/elastic/elasticsearch/pull/126397) (issue: [#126392](https://github.com/elastic/elasticsearch/issues/126392))
* ESQL: Retain aggregate when grouping [#126598](https://github.com/elastic/elasticsearch/pull/126598) (issue: [#126026](https://github.com/elastic/elasticsearch/issues/126026))
* Fail with 500 not 400 for `ValueExtractor` bugs [#126296](https://github.com/elastic/elasticsearch/pull/126296)
* Fix LIMIT NPE with null value [#130914](https://github.com/elastic/elasticsearch/pull/130914) (issue: [#130908](https://github.com/elastic/elasticsearch/issues/130908))
* Fix `PushQueriesIT.testLike()` fails [#129647](https://github.com/elastic/elasticsearch/pull/129647)
* Fix `PushQueryIT#testEqualityOrTooBig` [#129657](https://github.com/elastic/elasticsearch/pull/129657) (issue: [#129545](https://github.com/elastic/elasticsearch/issues/129545))
* Fix behavior for `_index` LIKE for ESQL [#130849](https://github.com/elastic/elasticsearch/pull/130849) (issue: [#129511](https://github.com/elastic/elasticsearch/issues/129511))
* Fix constant keyword optimization [#129278](https://github.com/elastic/elasticsearch/pull/129278)
* Fix conversion of a Lucene wildcard pattern to a regexp [#128750](https://github.com/elastic/elasticsearch/pull/128750) (issues: [#128677](https://github.com/elastic/elasticsearch/issues/128677), [#128676](https://github.com/elastic/elasticsearch/issues/128676))
* Fix functions emitting warnings with no source [#122821](https://github.com/elastic/elasticsearch/pull/122821) (issue: [#122588](https://github.com/elastic/elasticsearch/issues/122588))
* Fix queries with missing index, `skip_unavailable` and filters [#130344](https://github.com/elastic/elasticsearch/pull/130344)
* Fix transport versions [#127668](https://github.com/elastic/elasticsearch/pull/127668) (issue: [#127667](https://github.com/elastic/elasticsearch/issues/127667))
* Handle unavailable MD5 in ES|QL [#130158](https://github.com/elastic/elasticsearch/pull/130158)
* Improve error message for ( and [ [#124177](https://github.com/elastic/elasticsearch/pull/124177) (issue: [#124145](https://github.com/elastic/elasticsearch/issues/124145))
* Prevent search functions work with a non-STANDARD index [#130638](https://github.com/elastic/elasticsearch/pull/130638) (issues: [#130561](https://github.com/elastic/elasticsearch/issues/130561), [#129778](https://github.com/elastic/elasticsearch/issues/129778))
* Remove duplicated nested commands [#123085](https://github.com/elastic/elasticsearch/pull/123085)
* Resolve groupings in aggregate before resolving references to groupings in the aggregations [#127524](https://github.com/elastic/elasticsearch/pull/127524)
* Retrieve token text only when necessary [#126578](https://github.com/elastic/elasticsearch/pull/126578)
* Support avg on aggregate metric double [#130421](https://github.com/elastic/elasticsearch/pull/130421)
* TO_IP can handle leading zeros [#126532](https://github.com/elastic/elasticsearch/pull/126532) (issue: [#125460](https://github.com/elastic/elasticsearch/issues/125460))
* TO_LOWER processes all values [#124676](https://github.com/elastic/elasticsearch/pull/124676) (issue: [#124002](https://github.com/elastic/elasticsearch/issues/124002))
* Workaround for RLike handling of empty lang pattern [#128895](https://github.com/elastic/elasticsearch/pull/128895) (issue: [#128813](https://github.com/elastic/elasticsearch/issues/128813))

Highlighting:
* Fix semantic highlighting bug on flat quantized fields [#131525](https://github.com/elastic/elasticsearch/pull/131525) (issue: [#131443](https://github.com/elastic/elasticsearch/issues/131443))

ILM+SLM:
* Fix `PolicyStepsRegistry` cache concurrency issue [#126840](https://github.com/elastic/elasticsearch/pull/126840) (issue: [#118406](https://github.com/elastic/elasticsearch/issues/118406))
* Inject an unfollow action before executing a downsample action in ILM [#105773](https://github.com/elastic/elasticsearch/pull/105773) (issue: [#105773](https://github.com/elastic/elasticsearch/issues/105773))
* Prevent ILM from processing shrunken index before its execution state is copied over [#129455](https://github.com/elastic/elasticsearch/pull/129455) (issue: [#109206](https://github.com/elastic/elasticsearch/issues/109206))
* The follower index should wait until the time series end time passes before unfollowing the leader index. [#128361](https://github.com/elastic/elasticsearch/pull/128361) (issue: [#128129](https://github.com/elastic/elasticsearch/issues/128129))

Indices APIs:
* Using a temp `IndexService` for template validation [#129507](https://github.com/elastic/elasticsearch/pull/129507) (issue: [#129473](https://github.com/elastic/elasticsearch/issues/129473))

Infra/Core:
* Reduce Data Loss in System Indices Migration [#121327](https://github.com/elastic/elasticsearch/pull/121327)
* System data streams are not being upgraded in the feature migration API [#126409](https://github.com/elastic/elasticsearch/pull/126409) (issue: [#122949](https://github.com/elastic/elasticsearch/issues/122949))

Infra/Node Lifecycle:
* Better handling of node ids from shutdown metadata (avoid NPE on already removed nodes) [#128298](https://github.com/elastic/elasticsearch/pull/128298) (issue: [#100201](https://github.com/elastic/elasticsearch/issues/100201))

Infra/REST API:
* Fix NPE in APMTracer through `RestController` [#128314](https://github.com/elastic/elasticsearch/pull/128314)
* Improve handling of empty response [#125562](https://github.com/elastic/elasticsearch/pull/125562) (issue: [#57639](https://github.com/elastic/elasticsearch/issues/57639))

Infra/Scripting:
* Add a custom `toString` to `DynamicMap` [#126562](https://github.com/elastic/elasticsearch/pull/126562) (issue: [#70262](https://github.com/elastic/elasticsearch/issues/70262))
* Add leniency to missing array values in mustache [#126550](https://github.com/elastic/elasticsearch/pull/126550) (issue: [#55200](https://github.com/elastic/elasticsearch/issues/55200))
* Fix painless return type cast for list shortcut [#126724](https://github.com/elastic/elasticsearch/pull/126724)

Infra/Settings:
* Add retry for `AccessDeniedException` in `AbstractFileWatchingService` [#128653](https://github.com/elastic/elasticsearch/pull/128653)

Ingest Node:
* Correctly handle non-integers in nested paths in the remove processor [#127006](https://github.com/elastic/elasticsearch/pull/127006)
* Correctly handle nulls in nested paths in the remove processor [#126417](https://github.com/elastic/elasticsearch/pull/126417)
* Correctly handling `download_database_on_pipeline_creation` within a pipeline processor within a default or final pipeline [#131236](https://github.com/elastic/elasticsearch/pull/131236)
* apm-data: Use representative count as event.success_count if available [#119995](https://github.com/elastic/elasticsearch/pull/119995)

Logs:
* Force niofs for fdt tmp file read access when flushing stored fields [#130308](https://github.com/elastic/elasticsearch/pull/130308)

Machine Learning:
* Adding timeout to request for creating inference endpoint [#126805](https://github.com/elastic/elasticsearch/pull/126805)
* Change ModelLoaderUtils.split to return the correct number of chunks and ranges. [#126009](https://github.com/elastic/elasticsearch/pull/126009) (issue: [#121799](https://github.com/elastic/elasticsearch/issues/121799))
* Fix ELAND endpoints not updating dimensions [#126537](https://github.com/elastic/elasticsearch/pull/126537)
* Fix memory usage estimation for ELSER models [#131630](https://github.com/elastic/elasticsearch/pull/131630)
* Prevent get datafeeds stats API returning an error when local tasks are slow to stop [#125477](https://github.com/elastic/elasticsearch/pull/125477) (issue: [#104160](https://github.com/elastic/elasticsearch/issues/104160))
* Provide model size statistics as soon as an anomaly detection job is opened [#124638](https://github.com/elastic/elasticsearch/pull/124638) (issue: [#121168](https://github.com/elastic/elasticsearch/issues/121168))
* Return a Conflict status code if the model deployment is stopped by a user [#125204](https://github.com/elastic/elasticsearch/pull/125204) (issue: [#123745](https://github.com/elastic/elasticsearch/issues/123745))
* Revert endpoint creation validation for ELSER and E5 [#126792](https://github.com/elastic/elasticsearch/pull/126792)
* Updates to allow using Cohere binary embedding response in semantic search queries [#121827](https://github.com/elastic/elasticsearch/pull/121827)
* Use INTERNAL_INGEST for Inference [#127522](https://github.com/elastic/elasticsearch/pull/127522) (issue: [#127519](https://github.com/elastic/elasticsearch/issues/127519))

Mapping:
* Synthetic source: avoid storing multi fields of type text and `match_only_text` by default [#129126](https://github.com/elastic/elasticsearch/pull/129126)

Ranking:
* Restore `TextSimilarityRankBuilder` XContent output [#124564](https://github.com/elastic/elasticsearch/pull/124564)
* Return BAD_REQUEST when a field scorer references a missing field [#127229](https://github.com/elastic/elasticsearch/pull/127229) (issue: [#127162](https://github.com/elastic/elasticsearch/issues/127162))

Relevance:
* Fix: Allow non-score secondary sorts in pinned retriever sub-retrievers [#128323](https://github.com/elastic/elasticsearch/pull/128323)
* Prevent Query Rule Creation with Invalid Numeric Match Criteria [#122823](https://github.com/elastic/elasticsearch/pull/122823)

Search:
* Add Cluster Feature for L2 Norm [#129181](https://github.com/elastic/elasticsearch/pull/129181)
* Check positions on `MultiPhraseQueries` as well as phrase queries [#129326](https://github.com/elastic/elasticsearch/pull/129326) (issue: [#123871](https://github.com/elastic/elasticsearch/issues/123871))
* Filter out empty top docs results before merging [#126385](https://github.com/elastic/elasticsearch/pull/126385) (issue: [#126118](https://github.com/elastic/elasticsearch/issues/126118))
* Fix NPE in `SemanticTextHighlighter` [#129509](https://github.com/elastic/elasticsearch/pull/129509) (issue: [#129501](https://github.com/elastic/elasticsearch/issues/129501))
* Fix bug in point in time response [#131391](https://github.com/elastic/elasticsearch/pull/131391) (issue: [#131026](https://github.com/elastic/elasticsearch/issues/131026))
* Fix handling of auto expand replicas for stateless indices [#122365](https://github.com/elastic/elasticsearch/pull/122365)
* Fix query rewrite logic to preserve `boosts` and `queryName` for `match`, `knn`, and `sparse_vector` queries on semantic_text fields [#129282](https://github.com/elastic/elasticsearch/pull/129282)
* Improve execution of terms queries over wildcard fields [#128986](https://github.com/elastic/elasticsearch/pull/128986) (issue: [#128201](https://github.com/elastic/elasticsearch/issues/128201))
* Remove empty results before merging [#126770](https://github.com/elastic/elasticsearch/pull/126770) (issue: [#126742](https://github.com/elastic/elasticsearch/issues/126742))
* Simplified Linear & RRF Retrievers - Return error on empty fields param [#129962](https://github.com/elastic/elasticsearch/pull/129962)

Snapshot/Restore:
* Do not apply further shard snapshot status updates after shard snapshot is complete [#127250](https://github.com/elastic/elasticsearch/pull/127250)
* Fix computation of last block size in Azure concurrent multipart uploads [#128746](https://github.com/elastic/elasticsearch/pull/128746)
* Limit number of suppressed S3 deletion errors [#123630](https://github.com/elastic/elasticsearch/pull/123630) (issue: [#123354](https://github.com/elastic/elasticsearch/issues/123354))
* Run `newShardSnapshotTask` tasks concurrently [#126452](https://github.com/elastic/elasticsearch/pull/126452)
* Throw better exception if verifying empty repo [#131677](https://github.com/elastic/elasticsearch/pull/131677)

Suggesters:
* Support duplicate suggestions in completion field [#121324](https://github.com/elastic/elasticsearch/pull/121324) (issue: [#82432](https://github.com/elastic/elasticsearch/issues/82432))

TLS:
* Watch SSL files instead of directories [#129738](https://github.com/elastic/elasticsearch/pull/129738)

Transform:
* Check alias during update [#124825](https://github.com/elastic/elasticsearch/pull/124825)

Vector Search:
* Fix and test off-heap stats when using direct IO for accessing the raw vectors [#128615](https://github.com/elastic/elasticsearch/pull/128615)
* Fix filtered knn vector search when query timeouts are enabled [#129440](https://github.com/elastic/elasticsearch/pull/129440)
* Fix top level knn search with scroll [#126035](https://github.com/elastic/elasticsearch/pull/126035)


## 9.0.4 [elasticsearch-9.0.4-release-notes]

### Fixes [elasticsearch-9.0.4-fixes]

Aggregations:
* Aggs: Add cancellation checks to `FilterByFilter` aggregator [#130452](https://github.com/elastic/elasticsearch/pull/130452)

Distributed:
* Drain responses on completion for `TransportNodesAction` [#130303](https://github.com/elastic/elasticsearch/pull/130303)

ES|QL:
* Avoid O(N^2) in VALUES with ordinals grouping [#130576](https://github.com/elastic/elasticsearch/pull/130576)
* Avoid dropping aggregate groupings in local plans [#129370](https://github.com/elastic/elasticsearch/pull/129370) (issues: [#129811](https://github.com/elastic/elasticsearch/issues/129811), [#128054](https://github.com/elastic/elasticsearch/issues/128054))
* Fix `BytesRef2BlockHash` [#130705](https://github.com/elastic/elasticsearch/pull/130705)
* Fix wildcard drop after lookup join [#130448](https://github.com/elastic/elasticsearch/pull/130448) (issue: [#129561](https://github.com/elastic/elasticsearch/issues/129561))

Infra/Core:
* Reverse disordered-version warning message [#129904](https://github.com/elastic/elasticsearch/pull/129904)

Machine Learning:
* Check for model deployment in inference endpoints before stopping [#129325](https://github.com/elastic/elasticsearch/pull/129325) (issue: [#128549](https://github.com/elastic/elasticsearch/issues/128549))
* Fix timeout bug in DBQ deletion of unused and orphan ML data [#130083](https://github.com/elastic/elasticsearch/pull/130083)
* Including `max_tokens` through the Service API for Anthropic [#131113](https://github.com/elastic/elasticsearch/pull/131113)

Mapping:
* Make flattened synthetic source concatenate object keys on scalar/object mismatch [#129600](https://github.com/elastic/elasticsearch/pull/129600) (issue: [#122936](https://github.com/elastic/elasticsearch/issues/122936))

Relevance:
* Fix: `GET _synonyms` returns synonyms with empty rules [#131032](https://github.com/elastic/elasticsearch/pull/131032)

Search:
* Check field data type before casting when applying geo distance sort [#130924](https://github.com/elastic/elasticsearch/pull/130924) (issue: [#129500](https://github.com/elastic/elasticsearch/issues/129500))
* Fix msearch request parsing when index expression is null [#130776](https://github.com/elastic/elasticsearch/pull/130776) (issue: [#129631](https://github.com/elastic/elasticsearch/issues/129631))
* Fix text similarity reranker does not propagate min score correctly [#129223](https://github.com/elastic/elasticsearch/pull/129223)
* Throw a 400 when sorting for all types of range fields [#129725](https://github.com/elastic/elasticsearch/pull/129725)
* Trim to size lists created in source fetchers [#130521](https://github.com/elastic/elasticsearch/pull/130521)

Vector Search:
* Fix knn search error when dimensions are not set [#131081](https://github.com/elastic/elasticsearch/pull/131081) (issue: [#129550](https://github.com/elastic/elasticsearch/issues/129550))


## 9.0.3 [elasticsearch-9.0.3-release-notes]

### Features and enhancements [elasticsearch-9.0.3-features-enhancements]

Authorization:
* Fix unsupported privileges error message during role and API key creation [#129158](https://github.com/elastic/elasticsearch/pull/129158) (issue: [#128132](https://github.com/elastic/elasticsearch/issues/128132))

Engine:
* Threadpool merge executor is aware of available disk space [#127613](https://github.com/elastic/elasticsearch/pull/127613)
* Threadpool merge scheduler [#120869](https://github.com/elastic/elasticsearch/pull/120869)

Ingest Node:
* Update traces duration mappings with appropriate unit type [#129418](https://github.com/elastic/elasticsearch/pull/129418)

Snapshot/Restore:
* Update shardGenerations for all indices on snapshot finalization [#128650](https://github.com/elastic/elasticsearch/pull/128650) (issue: [#108907](https://github.com/elastic/elasticsearch/issues/108907))

Stats:
* Optimize sparse vector stats collection [#128740](https://github.com/elastic/elasticsearch/pull/128740)

### Fixes [elasticsearch-9.0.3-fixes]

Aggregations:
* Aggs: Fix significant terms not finding background docuemnts for nested fields [#128472](https://github.com/elastic/elasticsearch/pull/128472) (issue: [#101163](https://github.com/elastic/elasticsearch/issues/101163))

Authorization:
* Prevent invalid privileges in manage roles privilege [#128532](https://github.com/elastic/elasticsearch/pull/128532) (issue: [#127496](https://github.com/elastic/elasticsearch/issues/127496))

CCS:
* Handle the indices pattern `["*", "-*"]` when grouping indices by cluster name [#128610](https://github.com/elastic/elasticsearch/pull/128610)

ES|QL:
* Fix `FieldAttribute` name usage in `InferNonNullAggConstraint` [#128910](https://github.com/elastic/elasticsearch/pull/128910)
* Fix case insensitive comparisons to "" [#127532](https://github.com/elastic/elasticsearch/pull/127532) (issue: [#127431](https://github.com/elastic/elasticsearch/issues/127431))
* Support DATE_NANOS in LOOKUP JOIN [#127962](https://github.com/elastic/elasticsearch/pull/127962) (issue: [#127249](https://github.com/elastic/elasticsearch/issues/127249))
* Throw ISE instead of IAE for illegal block in page [#128960](https://github.com/elastic/elasticsearch/pull/128960)

IdentityProvider:
* Improve cache invalidation in IdP SP cache [#128890](https://github.com/elastic/elasticsearch/pull/128890)

Indices APIs:
* Avoid unnecessary determinization in index pattern conflict checks [#128362](https://github.com/elastic/elasticsearch/pull/128362)

Infra/Core:
* Update AbstractXContentParser to support parsers that don't provide text characters [#129005](https://github.com/elastic/elasticsearch/pull/129005)

Infra/Plugins:
* Add complete attribute to .fleet-agents docs [#127651](https://github.com/elastic/elasticsearch/pull/127651)

Machine Learning:
* Account for Java direct memory on machine learning nodes to prevent out-of-memory crashes. [#128742](https://github.com/elastic/elasticsearch/pull/128742)
* Ensure that anomaly detection job state update retries if master node is temoporarily unavailable [#129391](https://github.com/elastic/elasticsearch/pull/129391) (issue: [#126148](https://github.com/elastic/elasticsearch/issues/126148))
* Prevent ML data retention logic from failing when deleting documents in read-only indices [#125408](https://github.com/elastic/elasticsearch/pull/125408)

Mapping:
* Check prefixes when constructing synthetic source for flattened fields [#129580](https://github.com/elastic/elasticsearch/pull/129580) (issue: [#129508](https://github.com/elastic/elasticsearch/issues/129508))

Search:
* Fix NPE in semantic highlighter [#128989](https://github.com/elastic/elasticsearch/pull/128989) (issue: [#128975](https://github.com/elastic/elasticsearch/issues/128975))
* Fix inner hits + aggregations concurrency bug [#128036](https://github.com/elastic/elasticsearch/pull/128036) (issue: [#122419](https://github.com/elastic/elasticsearch/issues/122419))
* Fix minmax normalizer handling of single-doc result sets [#128689](https://github.com/elastic/elasticsearch/pull/128689)
* Fix missing highlighting in `match_all` queries for `semantic_text` fields [#128702](https://github.com/elastic/elasticsearch/pull/128702)

Searchable Snapshots:
* Adjust unpromotable shard refresh request validation to allow `RefreshResult.NO_REFRESH` [#129176](https://github.com/elastic/elasticsearch/pull/129176) (issue: [#129036](https://github.com/elastic/elasticsearch/issues/129036))

Security:
* Fix error message when changing the password for a user in the file realm [#127621](https://github.com/elastic/elasticsearch/pull/127621)


## 9.0.2 [elasticsearch-9.0.2-release-notes]

### Features and enhancements [elasticsearch-9.0.2-features-enhancements]

Authentication:
* Http proxy support in JWT realm [#127337](https://github.com/elastic/elasticsearch/pull/127337) (issue: [#114956](https://github.com/elastic/elasticsearch/issues/114956))

ES|QL:
* Limit Replace function memory usage [#127924](https://github.com/elastic/elasticsearch/pull/127924)

### Fixes [elasticsearch-9.0.2-fixes]

Aggregations:
* Fix a bug in `significant_terms` [#127975](https://github.com/elastic/elasticsearch/pull/127975)

Audit:
* Handle streaming request body in audit log [#127798](https://github.com/elastic/elasticsearch/pull/127798)

Codec:
* Use new source loader when lower `docId` is accessed [#128320](https://github.com/elastic/elasticsearch/pull/128320)

Data streams:
* Fix system data streams incorrectly showing up in the list of template validation problems [#128161](https://github.com/elastic/elasticsearch/pull/128161)

Downsampling:
* Downsampling does not consider passthrough fields as dimensions [#127752](https://github.com/elastic/elasticsearch/pull/127752) (issue: [#125156](https://github.com/elastic/elasticsearch/issues/125156))

ES|QL:
* Consider inlinestats when having `field_caps` check for field names [#127564](https://github.com/elastic/elasticsearch/pull/127564) (issue: [#127236](https://github.com/elastic/elasticsearch/issues/127236))
* Don't push down filters on the right hand side of an inlinejoin [#127383](https://github.com/elastic/elasticsearch/pull/127383)
* ESQL: Avoid unintended attribute removal [#127563](https://github.com/elastic/elasticsearch/pull/127563) (issue: [#127468](https://github.com/elastic/elasticsearch/issues/127468))
* ESQL: Fix alias removal in regex extraction with JOIN [#127687](https://github.com/elastic/elasticsearch/pull/127687) (issue: [#127467](https://github.com/elastic/elasticsearch/issues/127467))
* ESQL: Keep `DROP` attributes when resolving field names [#127009](https://github.com/elastic/elasticsearch/pull/127009) (issue: [#126418](https://github.com/elastic/elasticsearch/issues/126418))
* Ensure ordinal builder emit ordinal blocks [#127949](https://github.com/elastic/elasticsearch/pull/127949)
* Fix union types in CCS [#128111](https://github.com/elastic/elasticsearch/pull/128111)
* Fix validation NPE in Enrich and add extra @Nullable annotations [#128260](https://github.com/elastic/elasticsearch/pull/128260) (issues: [#126297](https://github.com/elastic/elasticsearch/issues/126297), [#126253](https://github.com/elastic/elasticsearch/issues/126253))

Geo:
* Added geometry validation for GEO types to exit early on invalid latitudes [#128259](https://github.com/elastic/elasticsearch/pull/128259) (issue: [#128234](https://github.com/elastic/elasticsearch/issues/128234))

Infra/Core:
* Add missing `outbound_network` entitlement to x-pack-core [#126992](https://github.com/elastic/elasticsearch/pull/126992) (issue: [#127003](https://github.com/elastic/elasticsearch/issues/127003))
* Check hidden frames in entitlements [#127877](https://github.com/elastic/elasticsearch/pull/127877)

Infra/Scripting:
* Avoid nested docs in painless execute api [#127991](https://github.com/elastic/elasticsearch/pull/127991) (issue: [#41004](https://github.com/elastic/elasticsearch/issues/41004))

Machine Learning:
* Append all data to Chat Completion buffer [#127658](https://github.com/elastic/elasticsearch/pull/127658)
* Fix services API Google Vertex AI Rerank location field requirement [#127856](https://github.com/elastic/elasticsearch/pull/127856)
* Pass timeout to chat completion [#128338](https://github.com/elastic/elasticsearch/pull/128338)
* Use internal user for internal inference action [#128327](https://github.com/elastic/elasticsearch/pull/128327)

Relevance:
* Fix: Add `NamedWriteable` for `RuleQueryRankDoc` [#128153](https://github.com/elastic/elasticsearch/pull/128153) (issue: [#126071](https://github.com/elastic/elasticsearch/issues/126071))

Security:
* Remove dangling spaces wherever found [#127475](https://github.com/elastic/elasticsearch/pull/127475)

Snapshot/Restore:
* Add missing entitlement to `repository-azure` [#128047](https://github.com/elastic/elasticsearch/pull/128047) (issue: [#128046](https://github.com/elastic/elasticsearch/issues/128046))

TSDB:
* Skip the validation when retrieving the index mode during reindexing a time series data stream [#127824](https://github.com/elastic/elasticsearch/pull/127824)

Vector Search:
* [9.x] Revert "Enable madvise by default for all builds" [#127921](https://github.com/elastic/elasticsearch/pull/127921)


## 9.0.1 [elasticsearch-9.0.1-release-notes]

### Features and enhancements [elasticsearch-9.0.1-features-enhancements]

Infra/Core:
* Validation checks on paths allowed for 'files' entitlements. Restrict the paths we allow access to, forbidding plugins to specify/request entitlements for reading or writing to specific protected directories. [#126852](https://github.com/elastic/elasticsearch/pull/126852)

Ingest Node:
* Updating tika to 2.9.3 [#127353](https://github.com/elastic/elasticsearch/pull/127353)

Search:
* Enable sort optimization on float and `half_float` [#126342](https://github.com/elastic/elasticsearch/pull/126342)

Security:
* Add Issuer to failed SAML Signature validation logs when available [#126310](https://github.com/elastic/elasticsearch/pull/126310) (issue: [#111022](https://github.com/elastic/elasticsearch/issues/111022))

### Fixes [elasticsearch-9.0.1-fixes]

Aggregations:
* Rare terms aggregation false **positive** fix [#126884](https://github.com/elastic/elasticsearch/pull/126884)

Allocation:
* Fix shard size of initializing restored shard [#126783](https://github.com/elastic/elasticsearch/pull/126783) (issue: [#105331](https://github.com/elastic/elasticsearch/issues/105331))

CCS:
* Cancel expired async search task when a remote returns its results [#126583](https://github.com/elastic/elasticsearch/pull/126583)

Data streams:
* [otel-data] Bump plugin version to release _metric_names_hash changes [#126850](https://github.com/elastic/elasticsearch/pull/126850)

ES|QL:
* Fix count optimization with pushable union types [#127225](https://github.com/elastic/elasticsearch/pull/127225) (issue: [#127200](https://github.com/elastic/elasticsearch/issues/127200))
* Fix join masking eval [#126614](https://github.com/elastic/elasticsearch/pull/126614)
* Fix sneaky bug in single value query [#127146](https://github.com/elastic/elasticsearch/pull/127146)
* No, line noise isn't a valid ip [#127527](https://github.com/elastic/elasticsearch/pull/127527)

ILM+SLM:
* Fix equality bug in `WaitForIndexColorStep` [#126605](https://github.com/elastic/elasticsearch/pull/126605)

Infra/CLI:
* Use terminal reader in keystore add command [#126729](https://github.com/elastic/elasticsearch/pull/126729) (issue: [#98115](https://github.com/elastic/elasticsearch/issues/98115))

Infra/Core:
* Fix: consider case sensitiveness differences in Windows/Unix-like filesystems for files entitlements [#126990](https://github.com/elastic/elasticsearch/pull/126990) (issue: [#127047](https://github.com/elastic/elasticsearch/issues/127047))
* Rework uniquify to not use iterators [#126889](https://github.com/elastic/elasticsearch/pull/126889) (issue: [#126883](https://github.com/elastic/elasticsearch/issues/126883))
* Workaround max name limit imposed by Jackson 2.17 [#126806](https://github.com/elastic/elasticsearch/pull/126806)

Machine Learning:
* Adding missing `onFailure` call for Inference API start model request [#126930](https://github.com/elastic/elasticsearch/pull/126930)
* Fix text structure NPE when fields in list have null value [#125922](https://github.com/elastic/elasticsearch/pull/125922)
* Leverage threadpool schedule for inference api to avoid long running thread [#126858](https://github.com/elastic/elasticsearch/pull/126858) (issue: [#126853](https://github.com/elastic/elasticsearch/issues/126853))

Ranking:
* Fix LTR rescorer with model alias [#126273](https://github.com/elastic/elasticsearch/pull/126273)
* LTR score bounding [#125694](https://github.com/elastic/elasticsearch/pull/125694)

Search:
* Fix npe when using source confirmed text query against missing field [#127414](https://github.com/elastic/elasticsearch/pull/127414)

TSDB:
* Improve resiliency of `UpdateTimeSeriesRangeService` [#126637](https://github.com/elastic/elasticsearch/pull/126637)

Task Management:
* Fix race condition in `RestCancellableNodeClient` [#126686](https://github.com/elastic/elasticsearch/pull/126686) (issue: [#88201](https://github.com/elastic/elasticsearch/issues/88201))

Vector Search:
* Fix `vec_caps` to test for OS support too (on x64) [#126911](https://github.com/elastic/elasticsearch/pull/126911) (issue: [#126809](https://github.com/elastic/elasticsearch/issues/126809))
* Fix bbq quantization algorithm but for differently distributed components [#126778](https://github.com/elastic/elasticsearch/pull/126778)


## 9.0.0 [elasticsearch-900-release-notes]

### Highlights [elasticsearch-900-highlights]

::::{dropdown} rank_vectors field type is now available for late-interaction ranking
[`rank_vectors`](../reference/elasticsearch/mapping-reference/rank-vectors.md) is a new field type released as an experimental feature in Elasticsearch 9.0. It is designed to be used with dense vectors and allows for late-interaction second order ranking.

Late-interaction models are powerful rerankers. While their size and overall cost doesn’t lend itself for HNSW indexing, utilizing them as second order reranking can provide excellent boosts in relevance. The new `rank_vectors` mapping allows for rescoring over new and novel multi-vector late-interaction models like ColBERT or ColPali.
::::

::::{dropdown} ES|QL LOOKUP JOIN is now available in technical preview
[LOOKUP JOIN](../reference/query-languages/esql/esql-commands.md) is now available in technical preview. LOOKUP JOIN combines data from your ES|QL queries with matching records from a lookup index, enabling you to:

- Enrich your search results with reference data
- Speed up root-cause analysis and security investigations
- Join data across indices without complex queries
- Reduce operational overhead when correlating events
::::

::::{dropdown} The semantic_text field type is now GA
[`semantic_text`](../reference/elasticsearch/mapping-reference/semantic-text.md) is now an official GA (generally available) feature! This field type allows you to easily set up and perform semantic search with minimal ramp up time.
::::

### Features and enhancements [elasticsearch-900-features-enhancements]

Allocation:
* Add a not-master state for desired balance [#116904](https://github.com/elastic/elasticsearch/pull/116904)
* Only publish desired balance gauges on master [#115383](https://github.com/elastic/elasticsearch/pull/115383)
* Reset relocation/allocation failure counter on node join/shutdown [#119968](https://github.com/elastic/elasticsearch/pull/119968)

Authentication:
* Allow `SSHA-256` for API key credential hash [#120997](https://github.com/elastic/elasticsearch/pull/120997)

Authorization:
* Allow kibana_system user to manage .reindexed-v8-internal.alerts indices [#118959](https://github.com/elastic/elasticsearch/pull/118959)
* Do not fetch reserved roles from native store when Get Role API is called [#121971](https://github.com/elastic/elasticsearch/pull/121971)
* Grant necessary Kibana application privileges to `reporting_user` role [#118058](https://github.com/elastic/elasticsearch/pull/118058)
* Make reserved built-in roles queryable [#117581](https://github.com/elastic/elasticsearch/pull/117581)
* [Security Solution] Add `create_index` to `kibana_system` role for index/DS `.logs-endpoint.action.responses-*` [#115241](https://github.com/elastic/elasticsearch/pull/115241)
* [Security Solution] allows `kibana_system` user to manage .reindexed-v8-* Security Solution indices [#119054](https://github.com/elastic/elasticsearch/pull/119054)

CCS:
* Resolve/cluster allows querying for cluster info only (no index expression required) [#119898](https://github.com/elastic/elasticsearch/pull/119898)

CRUD:
* Metrics for indexing failures due to version conflicts [#119067](https://github.com/elastic/elasticsearch/pull/119067)
* Remove INDEX_REFRESH_BLOCK after index becomes searchable [#120807](https://github.com/elastic/elasticsearch/pull/120807)
* Suppress merge-on-recovery for older indices [#113462](https://github.com/elastic/elasticsearch/pull/113462)

Cluster Coordination:
* Include `clusterApplyListener` in long cluster apply warnings [#120087](https://github.com/elastic/elasticsearch/pull/120087)

Data streams:
* Add action to create index from a source index [#118890](https://github.com/elastic/elasticsearch/pull/118890)
* Add index and reindex request settings to speed up reindex [#119780](https://github.com/elastic/elasticsearch/pull/119780)
* Add rest endpoint for `create_from_source_index` [#119250](https://github.com/elastic/elasticsearch/pull/119250)
* Add sanity check to `ReindexDatastreamIndexAction` [#120231](https://github.com/elastic/elasticsearch/pull/120231)
* Adding a migration reindex cancel API [#118291](https://github.com/elastic/elasticsearch/pull/118291)
* Adding get migration reindex status [#118267](https://github.com/elastic/elasticsearch/pull/118267)
* Consistent mapping for OTel log and event bodies [#120547](https://github.com/elastic/elasticsearch/pull/120547)
* Filter deprecated settings when making dest index [#120163](https://github.com/elastic/elasticsearch/pull/120163)
* Ignore closed indices for reindex [#120244](https://github.com/elastic/elasticsearch/pull/120244)
* Improve how reindex data stream index action handles api blocks [#120084](https://github.com/elastic/elasticsearch/pull/120084)
* Initial work on `ReindexDatastreamIndexAction` [#116996](https://github.com/elastic/elasticsearch/pull/116996)
* Make `requests_per_second` configurable to throttle reindexing [#120207](https://github.com/elastic/elasticsearch/pull/120207)
* Optimized index sorting for OTel logs [#119504](https://github.com/elastic/elasticsearch/pull/119504)
* Reindex data stream indices on different nodes [#125171](https://github.com/elastic/elasticsearch/pull/125171)
* Report Deprecated Indices That Are Flagged To Ignore Migration Reindex As A Warning [#120629](https://github.com/elastic/elasticsearch/pull/120629)
* Retry ILM async action after reindexing data stream [#124149](https://github.com/elastic/elasticsearch/pull/124149)
* Set cause on create index request in create from action [#124363](https://github.com/elastic/elasticsearch/pull/124363)
* Update data stream deprecations warnings to new format and filter searchable snapshots from response [#118562](https://github.com/elastic/elasticsearch/pull/118562)

Distributed:
* Make various alias retrieval APIs wait for cluster to unblock [#117230](https://github.com/elastic/elasticsearch/pull/117230)
* Metrics for incremental bulk splits [#116765](https://github.com/elastic/elasticsearch/pull/116765)
* Use Azure blob batch API to delete blobs in batches [#114566](https://github.com/elastic/elasticsearch/pull/114566)

Downsampling:
* Improve downsample performance by buffering docids and do bulk processing [#124477](https://github.com/elastic/elasticsearch/pull/124477)
* Improve rolling up metrics [#124739](https://github.com/elastic/elasticsearch/pull/124739)

EQL:
* Add support for partial shard results [#116388](https://github.com/elastic/elasticsearch/pull/116388)
* Optional named arguments for function in map [#118619](https://github.com/elastic/elasticsearch/pull/118619)

ES|QL:
* Add ES|QL cross-cluster query telemetry collection [#119474](https://github.com/elastic/elasticsearch/pull/119474)
* Add a `LicenseAware` interface for licensed Nodes [#118931](https://github.com/elastic/elasticsearch/pull/118931) (issue: [#117405](https://github.com/elastic/elasticsearch/issues/117405))
* Add a `PostAnalysisAware,` distribute verification [#119798](https://github.com/elastic/elasticsearch/pull/119798)
* Add a standard deviation aggregating function: STD_DEV [#116531](https://github.com/elastic/elasticsearch/pull/116531)
* Add cluster level reduction [#117731](https://github.com/elastic/elasticsearch/pull/117731)
* Add nulls support to Categorize [#117655](https://github.com/elastic/elasticsearch/pull/117655)
* Allow skip shards with `_tier` and `_index` in ES|QL [#123728](https://github.com/elastic/elasticsearch/pull/123728)
* Async search responses have CCS metadata while searches are running [#117265](https://github.com/elastic/elasticsearch/pull/117265)
* Check for early termination in Driver [#118188](https://github.com/elastic/elasticsearch/pull/118188)
* Do not serialize `EsIndex` in plan [#119580](https://github.com/elastic/elasticsearch/pull/119580)
* ESQL - Add Match function options [#120360](https://github.com/elastic/elasticsearch/pull/120360)
* ESQL - Allow full text functions disjunctions for non-full text functions [#120291](https://github.com/elastic/elasticsearch/pull/120291)
* ESQL - Remove restrictions for disjunctions in full text functions [#118544](https://github.com/elastic/elasticsearch/pull/118544)
* ESQL - enabling scoring with METADATA `_score` [#113120](https://github.com/elastic/elasticsearch/pull/113120)
* ESQL Add esql hash function [#117989](https://github.com/elastic/elasticsearch/pull/117989)
* ESQL Support IN operator for Date nanos [#119772](https://github.com/elastic/elasticsearch/pull/119772) (issue: [#118578](https://github.com/elastic/elasticsearch/issues/118578))
* ESQL: Align `RENAME` behavior with `EVAL` for sequential processing [#122250](https://github.com/elastic/elasticsearch/pull/122250) (issue: [#121739](https://github.com/elastic/elasticsearch/issues/121739))
* ESQL: CATEGORIZE as a `BlockHash` [#114317](https://github.com/elastic/elasticsearch/pull/114317)
* ESQL: Enable async get to support formatting [#111104](https://github.com/elastic/elasticsearch/pull/111104) (issue: [#110926](https://github.com/elastic/elasticsearch/issues/110926))
* ESQL: Enterprise license enforcement for CCS [#118102](https://github.com/elastic/elasticsearch/pull/118102)
* ES|QL - Add scoring for full text functions disjunctions [#121793](https://github.com/elastic/elasticsearch/pull/121793)
* ES|QL: Partial result on demand for async queries [#118122](https://github.com/elastic/elasticsearch/pull/118122)
* Enable KQL function as a tech preview [#119730](https://github.com/elastic/elasticsearch/pull/119730)
* Enable LOOKUP JOIN in non-snapshot builds [#121193](https://github.com/elastic/elasticsearch/pull/121193) (issue: [#121185](https://github.com/elastic/elasticsearch/issues/121185))
* Enable node-level reduction by default [#119621](https://github.com/elastic/elasticsearch/pull/119621)
* Enable physical plan verification [#118114](https://github.com/elastic/elasticsearch/pull/118114)
* Ensure cluster string could be quoted [#120355](https://github.com/elastic/elasticsearch/pull/120355)
* Esql - Support date nanos in date extract function [#120727](https://github.com/elastic/elasticsearch/pull/120727) (issue: [#110000](https://github.com/elastic/elasticsearch/issues/110000))
* Esql - support date nanos in date format function [#120143](https://github.com/elastic/elasticsearch/pull/120143) (issue: [#109994](https://github.com/elastic/elasticsearch/issues/109994))
* Esql Support date nanos on date diff function [#120645](https://github.com/elastic/elasticsearch/pull/120645) (issue: [#109999](https://github.com/elastic/elasticsearch/issues/109999))
* Esql bucket function for date nanos [#118474](https://github.com/elastic/elasticsearch/pull/118474) (issue: [#118031](https://github.com/elastic/elasticsearch/issues/118031))
* Esql compare nanos and millis [#118027](https://github.com/elastic/elasticsearch/pull/118027) (issue: [#116281](https://github.com/elastic/elasticsearch/issues/116281))
* Esql implicit casting for date nanos [#118697](https://github.com/elastic/elasticsearch/pull/118697) (issue: [#118476](https://github.com/elastic/elasticsearch/issues/118476))
* Expand type compatibility for match function and operator [#117555](https://github.com/elastic/elasticsearch/pull/117555)
* Extend `TranslationAware` to all pushable expressions [#120192](https://github.com/elastic/elasticsearch/pull/120192)
* Fix Driver status iterations and `cpuTime` [#123290](https://github.com/elastic/elasticsearch/pull/123290) (issue: [#122967](https://github.com/elastic/elasticsearch/issues/122967))
* Hash functions [#118938](https://github.com/elastic/elasticsearch/pull/118938)
* Implement a `MetricsAware` interface [#121074](https://github.com/elastic/elasticsearch/pull/121074)
* Initial support for unmapped fields [#119886](https://github.com/elastic/elasticsearch/pull/119886)
* LOOKUP JOIN using field-caps for field mapping [#117246](https://github.com/elastic/elasticsearch/pull/117246)
* Lookup join on multiple join fields not yet supported [#118858](https://github.com/elastic/elasticsearch/pull/118858)
* Move scoring in ES|QL out of snapshot [#120354](https://github.com/elastic/elasticsearch/pull/120354)
* Optimize ST_EXTENT_AGG for `geo_shape` and `cartesian_shape` [#119889](https://github.com/elastic/elasticsearch/pull/119889)
* Push down `StartsWith` and `EndsWith` functions to Lucene [#123381](https://github.com/elastic/elasticsearch/pull/123381) (issue: [#123067](https://github.com/elastic/elasticsearch/issues/123067))
* Push down filter passed lookup join [#118410](https://github.com/elastic/elasticsearch/pull/118410)
* Resume Driver on cancelled or early finished [#120020](https://github.com/elastic/elasticsearch/pull/120020)
* Reuse child `outputSet` inside the plan where possible [#124611](https://github.com/elastic/elasticsearch/pull/124611)
* Rewrite TO_UPPER/TO_LOWER comparisons [#118870](https://github.com/elastic/elasticsearch/pull/118870) (issue: [#118304](https://github.com/elastic/elasticsearch/issues/118304))
* ST_EXTENT aggregation [#117451](https://github.com/elastic/elasticsearch/pull/117451) (issue: [#104659](https://github.com/elastic/elasticsearch/issues/104659))
* ST_EXTENT_AGG optimize envelope extraction from doc-values for cartesian_shape [#118802](https://github.com/elastic/elasticsearch/pull/118802)
* Smarter field caps with subscribable listener [#116755](https://github.com/elastic/elasticsearch/pull/116755)
* Support ST_ENVELOPE and related (ST_XMIN, ST_XMAX, ST_YMIN, ST_YMAX) functions [#116964](https://github.com/elastic/elasticsearch/pull/116964) (issue: [#104875](https://github.com/elastic/elasticsearch/issues/104875))
* Support partial sort fields in TopN pushdown [#116043](https://github.com/elastic/elasticsearch/pull/116043) (issue: [#114515](https://github.com/elastic/elasticsearch/issues/114515))
* Support some stats on aggregate_metric_double [#120343](https://github.com/elastic/elasticsearch/pull/120343) (issue: [#110649](https://github.com/elastic/elasticsearch/issues/110649))
* Take named parameters for identifier and pattern out of snapshot [#121850](https://github.com/elastic/elasticsearch/pull/121850)
* Term query for ES|QL [#117359](https://github.com/elastic/elasticsearch/pull/117359)
* Update grammar to rely on `indexPattern` instead of identifier in join target [#120494](https://github.com/elastic/elasticsearch/pull/120494)
* `_score` should not be a reserved attribute in ES|QL [#118435](https://github.com/elastic/elasticsearch/pull/118435) (issue: [#118460](https://github.com/elastic/elasticsearch/issues/118460))

Engine:
* Defer unpromotable shard refreshes until index refresh blocks are cleared [#120642](https://github.com/elastic/elasticsearch/pull/120642)
* POC mark read-only [#119743](https://github.com/elastic/elasticsearch/pull/119743)

Experiences:
* Integrate IBM watsonx to Inference API for re-ranking task [#117176](https://github.com/elastic/elasticsearch/pull/117176)

Extract&Transform:
* [Connector API] Support hard deletes with new URL param in delete endpoint [#120200](https://github.com/elastic/elasticsearch/pull/120200)
* [Connector API] Support soft-deletes of connectors [#118669](https://github.com/elastic/elasticsearch/pull/118669)
* [Connector APIs] Enforce index prefix for managed connectors [#117778](https://github.com/elastic/elasticsearch/pull/117778)

Geo:
* Optimize indexing points with index and doc values set to true [#120271](https://github.com/elastic/elasticsearch/pull/120271)

Health:
* Increase `replica_unassigned_buffer_time` default from 3s to 5s [#112834](https://github.com/elastic/elasticsearch/pull/112834)

Highlighting:
* Add Highlighter for Semantic Text Fields [#118064](https://github.com/elastic/elasticsearch/pull/118064)

ILM+SLM:
* Add a `replicate_for` option to the ILM `searchable_snapshot` action [#119003](https://github.com/elastic/elasticsearch/pull/119003)

Indices APIs:
* Add `remove_index_block` arg to `_create_from` api [#120548](https://github.com/elastic/elasticsearch/pull/120548)
* Remove index blocks by default in `create_from` [#120643](https://github.com/elastic/elasticsearch/pull/120643)
* Run `TransportGetComponentTemplateAction` on local node [#116868](https://github.com/elastic/elasticsearch/pull/116868)
* Run `TransportGetComposableIndexTemplate` on local node [#119830](https://github.com/elastic/elasticsearch/pull/119830)
* Run `TransportGetIndexTemplateAction` on local node [#119837](https://github.com/elastic/elasticsearch/pull/119837)
* introduce new categories for deprecated resources in deprecation API [#120505](https://github.com/elastic/elasticsearch/pull/120505)

Inference:
* Add version prefix to Inference Service API path [#117095](https://github.com/elastic/elasticsearch/pull/117095)
* Remove Elastic Inference Service feature flag and deprecated setting [#120842](https://github.com/elastic/elasticsearch/pull/120842)
* Update sparse text embeddings API route for Inference Service [#118025](https://github.com/elastic/elasticsearch/pull/118025)
* [Elastic Inference Service] Add ElasticInferenceService Unified ChatCompletions Integration [#118871](https://github.com/elastic/elasticsearch/pull/118871)

Infra/CLI:
* Ignore _JAVA_OPTIONS [#124843](https://github.com/elastic/elasticsearch/pull/124843)
* Strengthen encryption for elasticsearch-keystore tool to AES 256 [#119749](https://github.com/elastic/elasticsearch/pull/119749)

Infra/Circuit Breakers:
* Add link to Circuit Breaker "Data too large" exception message [#113561](https://github.com/elastic/elasticsearch/pull/113561)

Infra/Core:
* Add support for specifying reindexing script for system index migration [#119001](https://github.com/elastic/elasticsearch/pull/119001)
* Bump major version for feature migration system indices [#117243](https://github.com/elastic/elasticsearch/pull/117243)
* Change default Docker image to be based on UBI minimal instead of Ubuntu [#116739](https://github.com/elastic/elasticsearch/pull/116739)
* Improve size limiting string message [#122427](https://github.com/elastic/elasticsearch/pull/122427)
* Infrastructure for assuming cluster features in the next major version [#118143](https://github.com/elastic/elasticsearch/pull/118143)
* Permanently switch from Java SecurityManager to Entitlements. The Java SecurityManager has been deprecated since Java 17, and it is now completely disabled in Java 24. In order to retain an similar level of protection, Elasticsearch implemented its own protection mechanism, Entitlements. Starting with this version, Entitlements will permanently replace the Java SecurityManager. [#124865](https://github.com/elastic/elasticsearch/pull/124865)
* Update ASM 9.7 -> 9.7.1 to support JDK 24 [#118094](https://github.com/elastic/elasticsearch/pull/118094)

Infra/Metrics:
* Add `ensureGreen` test method for use with `adminClient` [#113425](https://github.com/elastic/elasticsearch/pull/113425)

Infra/REST API:
* A new query parameter `?include_source_on_error` was added for create / index, update and bulk REST APIs to control
if to include the document source in the error response in case of parsing errors. The default value is `true`. [#120725](https://github.com/elastic/elasticsearch/pull/120725)
* Indicate when errors represent timeouts [#124936](https://github.com/elastic/elasticsearch/pull/124936)

Infra/Scripting:
* Add a `mustache.max_output_size_bytes` setting to limit the length of results from mustache scripts [#114002](https://github.com/elastic/elasticsearch/pull/114002)

Infra/Settings:
* Introduce `IndexSettingDeprecatedInV8AndRemovedInV9` Setting property [#120334](https://github.com/elastic/elasticsearch/pull/120334)
* Run `TransportClusterGetSettingsAction` on local node [#119831](https://github.com/elastic/elasticsearch/pull/119831)

Ingest Node:
* Allow setting the `type` in the reroute processor [#122409](https://github.com/elastic/elasticsearch/pull/122409) (issue: [#121553](https://github.com/elastic/elasticsearch/issues/121553))
* Optimize `IngestCtxMap` construction [#120833](https://github.com/elastic/elasticsearch/pull/120833)
* Optimize `IngestDocMetadata` `isAvailable` [#120753](https://github.com/elastic/elasticsearch/pull/120753)
* Optimize `IngestDocument` `FieldPath` allocation [#120573](https://github.com/elastic/elasticsearch/pull/120573)
* Optimize some per-document hot paths in the geoip processor [#120824](https://github.com/elastic/elasticsearch/pull/120824)
* Returning ignored fields in the simulate ingest API [#117214](https://github.com/elastic/elasticsearch/pull/117214)
* Run `GetPipelineTransportAction` on local node [#120445](https://github.com/elastic/elasticsearch/pull/120445)
* Run `TransportGetEnrichPolicyAction` on local node [#121124](https://github.com/elastic/elasticsearch/pull/121124)
* Run template simulation actions on local node [#120038](https://github.com/elastic/elasticsearch/pull/120038)

License:
* Bump `TrialLicenseVersion` to allow starting new trial on 9.0 [#120198](https://github.com/elastic/elasticsearch/pull/120198)

Logs:
* Add LogsDB option to route on sort fields [#116687](https://github.com/elastic/elasticsearch/pull/116687)
* Add a new index setting to skip recovery source when synthetic source is enabled [#114618](https://github.com/elastic/elasticsearch/pull/114618)
* Configure index sorting through index settings for logsdb [#118968](https://github.com/elastic/elasticsearch/pull/118968) (issue: [#118686](https://github.com/elastic/elasticsearch/issues/118686))
* Optimize loading mappings when determining synthetic source usage and whether host.name can be sorted on. [#120055](https://github.com/elastic/elasticsearch/pull/120055)

Machine Learning:
* Add DeBERTa-V2/V3 tokenizer [#111852](https://github.com/elastic/elasticsearch/pull/111852)
* Add Inference Unified API for chat completions for OpenAI [#117589](https://github.com/elastic/elasticsearch/pull/117589)
* Add Jina AI API to do inference for Embedding and Rerank models [#118652](https://github.com/elastic/elasticsearch/pull/118652)
* Add enterprise license check for Inference API actions [#119893](https://github.com/elastic/elasticsearch/pull/119893)
* Adding chunking settings to `IbmWatsonxService` [#114914](https://github.com/elastic/elasticsearch/pull/114914)
* Adding default endpoint for Elastic Rerank [#117939](https://github.com/elastic/elasticsearch/pull/117939)
* Adding endpoint creation validation for all task types to remaining services [#115020](https://github.com/elastic/elasticsearch/pull/115020)
* Automatically rollover legacy .ml-anomalies indices [#120913](https://github.com/elastic/elasticsearch/pull/120913)
* Automatically rollover legacy ml indices [#120405](https://github.com/elastic/elasticsearch/pull/120405)
* Change the auditor to write via an alias [#120064](https://github.com/elastic/elasticsearch/pull/120064)
* Check for presence of error object when validating streaming responses from integrations in the inference API [#118375](https://github.com/elastic/elasticsearch/pull/118375)
* Check if the anomaly results index has been rolled over [#125404](https://github.com/elastic/elasticsearch/pull/125404)
* ES|QL categorize with multiple groupings [#118173](https://github.com/elastic/elasticsearch/pull/118173)
* Ignore failures from renormalizing buckets in read-only index [#118674](https://github.com/elastic/elasticsearch/pull/118674)
* Inference duration and error metrics [#115876](https://github.com/elastic/elasticsearch/pull/115876)
* Migrate stream to core error parsing [#120722](https://github.com/elastic/elasticsearch/pull/120722)
* Remove all mentions of eis and gateway and deprecate flags that do [#116692](https://github.com/elastic/elasticsearch/pull/116692)
* Remove deprecated sort from reindex operation within dataframe analytics procedure [#117606](https://github.com/elastic/elasticsearch/pull/117606)
* Retry on `ClusterBlockException` on transform destination index [#118194](https://github.com/elastic/elasticsearch/pull/118194)
* Support mTLS for the Elastic Inference Service integration inside the inference API [#119679](https://github.com/elastic/elasticsearch/pull/119679)
* [Inference API] Add node-local rate limiting for the inference API [#120400](https://github.com/elastic/elasticsearch/pull/120400)
* [Inference API] fix spell words: covertToString to convertToString [#119922](https://github.com/elastic/elasticsearch/pull/119922)

Mapping:
* Add Optional Source Filtering to Source Loaders [#113827](https://github.com/elastic/elasticsearch/pull/113827)
* Add option to store `sparse_vector` outside `_source` [#117917](https://github.com/elastic/elasticsearch/pull/117917)
* Release semantic_text as a GA feature [#124669](https://github.com/elastic/elasticsearch/pull/124669)

Network:
* Allow http unsafe buffers by default [#116115](https://github.com/elastic/elasticsearch/pull/116115)
* Http stream activity tracker and exceptions handling [#119564](https://github.com/elastic/elasticsearch/pull/119564)
* Remove HTTP content copies [#117303](https://github.com/elastic/elasticsearch/pull/117303)
* `ConnectTransportException` returns retryable BAD_GATEWAY [#118681](https://github.com/elastic/elasticsearch/pull/118681) (issue: [#118320](https://github.com/elastic/elasticsearch/issues/118320))

Packaging:
* Update bundled JDK to Java 24 [#125159](https://github.com/elastic/elasticsearch/pull/125159)

Ranking:
* Add a generic `rescorer` retriever based on the search request's rescore functionality [#118585](https://github.com/elastic/elasticsearch/pull/118585) (issue: [#118327](https://github.com/elastic/elasticsearch/issues/118327))
* Set default reranker for text similarity reranker to Elastic reranker [#120551](https://github.com/elastic/elasticsearch/pull/120551)

Recovery:
* Allow archive and searchable snapshots indices in N-2 version [#118941](https://github.com/elastic/elasticsearch/pull/118941)
* Trigger merges after recovery [#113102](https://github.com/elastic/elasticsearch/pull/113102)

Reindex:
* Change Reindexing metrics unit from millis to seconds [#115721](https://github.com/elastic/elasticsearch/pull/115721)

Relevance:
* Add Multi-Field Support for Semantic Text Fields [#120128](https://github.com/elastic/elasticsearch/pull/120128)

Search:
* Add match support for `semantic_text` fields [#117839](https://github.com/elastic/elasticsearch/pull/117839)
* Add support for `sparse_vector` queries against `semantic_text` fields [#118617](https://github.com/elastic/elasticsearch/pull/118617)
* Add support for knn vector queries on `semantic_text` fields [#119011](https://github.com/elastic/elasticsearch/pull/119011)
* Added optional parameters to QSTR ES|QL function [#121787](https://github.com/elastic/elasticsearch/pull/121787) (issue: [#120933](https://github.com/elastic/elasticsearch/issues/120933))
* Adding linear retriever to support weighted sums of sub-retrievers [#120222](https://github.com/elastic/elasticsearch/pull/120222)
* Address and remove any references of RestApiVersion version 7 [#117572](https://github.com/elastic/elasticsearch/pull/117572)
* Feat: add a user-configurable timeout parameter to the `_resolve/cluster` API [#120542](https://github.com/elastic/elasticsearch/pull/120542)
* Make semantic text part of the text family [#119792](https://github.com/elastic/elasticsearch/pull/119792)
* Only aggregations require at least one shard request [#115314](https://github.com/elastic/elasticsearch/pull/115314)
* Prevent data nodes from sending stack traces to coordinator when `error_trace=false` [#118266](https://github.com/elastic/elasticsearch/pull/118266)
* Propagate status codes from shard failures appropriately [#118016](https://github.com/elastic/elasticsearch/pull/118016) (issue: [#118482](https://github.com/elastic/elasticsearch/issues/118482))
* Upgrade to Lucene 10 [#114741](https://github.com/elastic/elasticsearch/pull/114741)
* Upgrade to Lucene 10.1.0 [#119308](https://github.com/elastic/elasticsearch/pull/119308)

Security:
* Add refresh `.security` index call between security migrations [#114879](https://github.com/elastic/elasticsearch/pull/114879)

Snapshot/Restore:
* Add IMDSv2 support to `repository-s3` [#117748](https://github.com/elastic/elasticsearch/pull/117748) (issue: [#105135](https://github.com/elastic/elasticsearch/issues/105135))
* Expose operation and request counts separately in repository stats [#117530](https://github.com/elastic/elasticsearch/pull/117530) (issue: [#104443](https://github.com/elastic/elasticsearch/issues/104443))
* Retry `S3BlobContainer#getRegister` on all exceptions [#114813](https://github.com/elastic/elasticsearch/pull/114813)
* Retry internally when CAS upload is throttled [GCS] [#120250](https://github.com/elastic/elasticsearch/pull/120250) (issue: [#116546](https://github.com/elastic/elasticsearch/issues/116546))
* Track shard snapshot progress during node shutdown [#112567](https://github.com/elastic/elasticsearch/pull/112567)
* Upgrade AWS SDK to v1.12.746 [#122431](https://github.com/elastic/elasticsearch/pull/122431)

Suggesters:
* Extensible Completion Postings Formats [#111494](https://github.com/elastic/elasticsearch/pull/111494)

TSDB:
* Increase field limit for OTel metrics to 10 000 [#120591](https://github.com/elastic/elasticsearch/pull/120591)

Transform:
* Add support for `extended_stats` [#120340](https://github.com/elastic/elasticsearch/pull/120340)
* Auto-migrate `max_page_search_size` [#119348](https://github.com/elastic/elasticsearch/pull/119348)
* Create upgrade mode [#117858](https://github.com/elastic/elasticsearch/pull/117858)
* Wait while index is blocked [#119542](https://github.com/elastic/elasticsearch/pull/119542)
* [Deprecation] Add `transform_ids` to outdated index [#120821](https://github.com/elastic/elasticsearch/pull/120821)

Vector Search:
* Add new experimental `rank_vectors` mapping for late-interaction second order ranking [#118804](https://github.com/elastic/elasticsearch/pull/118804)
* Even better(er) binary quantization [#117994](https://github.com/elastic/elasticsearch/pull/117994)
* KNN vector rescoring for quantized vectors [#116663](https://github.com/elastic/elasticsearch/pull/116663)
* Mark bbq indices as GA and add rolling upgrade integration tests [#121105](https://github.com/elastic/elasticsearch/pull/121105)
* Speed up bit compared with floats or bytes script operations [#117199](https://github.com/elastic/elasticsearch/pull/117199)

### Fixes [elasticsearch-900-fixes]

Aggregations:
* Aggs: Let terms run in global ords mode no match [#124782](https://github.com/elastic/elasticsearch/pull/124782)
* Handle with `illegalArgumentExceptions` negative values in HDR percentile aggregations [#116174](https://github.com/elastic/elasticsearch/pull/116174) (issue: [#115777](https://github.com/elastic/elasticsearch/issues/115777))

Analysis:
* Adjust exception thrown when unable to load hunspell dict [#123743](https://github.com/elastic/elasticsearch/pull/123743)
* Analyze API to return 400 for wrong custom analyzer [#121568](https://github.com/elastic/elasticsearch/pull/121568) (issue: [#121443](https://github.com/elastic/elasticsearch/issues/121443))
* Non existing synonyms sets do not fail shard recovery for indices [#125659](https://github.com/elastic/elasticsearch/pull/125659) (issue: [#125603](https://github.com/elastic/elasticsearch/issues/125603))

Authentication:
* Fix NPE for missing Content Type header in OIDC Authenticator [#126191](https://github.com/elastic/elasticsearch/pull/126191)

CAT APIs:
* Fix cat_component_templates documentation [#120487](https://github.com/elastic/elasticsearch/pull/120487)

CRUD:
* Preserve thread context when waiting for segment generation in RTG [#114623](https://github.com/elastic/elasticsearch/pull/114623)
* Preserve thread context when waiting for segment generation in RTG [#117148](https://github.com/elastic/elasticsearch/pull/117148)

Data streams:
* Avoid updating settings version in `MetadataMigrateToDataStreamService` when settings have not changed [#118704](https://github.com/elastic/elasticsearch/pull/118704)
* Block-writes cannot be added after read-only [#119007](https://github.com/elastic/elasticsearch/pull/119007) (issue: [#119002](https://github.com/elastic/elasticsearch/issues/119002))
* Ensure removal of index blocks does not leave key with null value [#122246](https://github.com/elastic/elasticsearch/pull/122246)
* Fixes a invalid warning from being issued when restoring a system data stream from a snapshot. [#125881](https://github.com/elastic/elasticsearch/pull/125881)
* Match dot prefix of migrated DS backing index with the source index [#120042](https://github.com/elastic/elasticsearch/pull/120042)
* Refresh source index before reindexing data stream index [#120752](https://github.com/elastic/elasticsearch/pull/120752) (issue: [#120314](https://github.com/elastic/elasticsearch/issues/120314))
* Updating `TransportRolloverAction.checkBlock` so that non-write-index blocks do not prevent data stream rollover [#122905](https://github.com/elastic/elasticsearch/pull/122905)
* `ReindexDataStreamIndex` bug in assertion caused by reference equality [#121325](https://github.com/elastic/elasticsearch/pull/121325)

Downsampling:
* Copy metrics and `default_metric` properties when downsampling `aggregate_metric_double` [#121727](https://github.com/elastic/elasticsearch/pull/121727) (issues: [#119696](https://github.com/elastic/elasticsearch/issues/119696), [#96076](https://github.com/elastic/elasticsearch/issues/96076))
* Improve downsample performance by avoiding to read unnecessary dimension values when downsampling. [#124451](https://github.com/elastic/elasticsearch/pull/124451)

ES|QL:
* Add support to VALUES aggregation for spatial types [#122886](https://github.com/elastic/elasticsearch/pull/122886) (issue: [#122413](https://github.com/elastic/elasticsearch/issues/122413))
* Allow the data type of `null` in filters [#118324](https://github.com/elastic/elasticsearch/pull/118324) (issue: [#116351](https://github.com/elastic/elasticsearch/issues/116351))
* Avoid over collecting in Limit or Lucene Operator [#123296](https://github.com/elastic/elasticsearch/pull/123296)
* Change the order of the optimization rules [#124335](https://github.com/elastic/elasticsearch/pull/124335)
* Correct line and column numbers of missing named parameters [#120852](https://github.com/elastic/elasticsearch/pull/120852)
* Drop null columns in text formats [#117643](https://github.com/elastic/elasticsearch/pull/117643) (issue: [#116848](https://github.com/elastic/elasticsearch/issues/116848))
* ESQL - date nanos range bug? [#125345](https://github.com/elastic/elasticsearch/pull/125345) (issue: [#125439](https://github.com/elastic/elasticsearch/issues/125439))
* ESQL: Fail in `AggregateFunction` when `LogicPlan` is not an `Aggregate` [#124446](https://github.com/elastic/elasticsearch/pull/124446) (issue: [#124311](https://github.com/elastic/elasticsearch/issues/124311))
* ESQL: Remove estimated row size assertion [#122762](https://github.com/elastic/elasticsearch/pull/122762) (issue: [#121535](https://github.com/elastic/elasticsearch/issues/121535))
* ES|QL: Fix scoring for full text functions [#124540](https://github.com/elastic/elasticsearch/pull/124540)
* Esql - Fix lucene push down behavior when a range contains nanos and millis [#125595](https://github.com/elastic/elasticsearch/pull/125595)
* Fix ROUND() with unsigned longs throwing in some edge cases [#119536](https://github.com/elastic/elasticsearch/pull/119536)
* Fix TDigestState.read CB leaks [#114303](https://github.com/elastic/elasticsearch/pull/114303) (issue: [#114194](https://github.com/elastic/elasticsearch/issues/114194))
* Fix TopN row size estimate [#119476](https://github.com/elastic/elasticsearch/pull/119476) (issue: [#106956](https://github.com/elastic/elasticsearch/issues/106956))
* Fix `AbstractShapeGeometryFieldMapperTests` [#119265](https://github.com/elastic/elasticsearch/pull/119265) (issue: [#119201](https://github.com/elastic/elasticsearch/issues/119201))
* Fix `ReplaceMissingFieldsWithNull` [#125764](https://github.com/elastic/elasticsearch/pull/125764) (issues: [#126036](https://github.com/elastic/elasticsearch/issues/126036), [#121754](https://github.com/elastic/elasticsearch/issues/121754), [#126030](https://github.com/elastic/elasticsearch/issues/126030))
* Fix a bug in TOP [#121552](https://github.com/elastic/elasticsearch/pull/121552)
* Fix async stop sometimes not properly collecting result [#121843](https://github.com/elastic/elasticsearch/pull/121843) (issue: [#121249](https://github.com/elastic/elasticsearch/issues/121249))
* Fix attribute set equals [#118823](https://github.com/elastic/elasticsearch/pull/118823)
* Fix double lookup failure on ESQL [#115616](https://github.com/elastic/elasticsearch/pull/115616) (issue: [#111398](https://github.com/elastic/elasticsearch/issues/111398))
* Fix function registry concurrency issues on constructor [#123492](https://github.com/elastic/elasticsearch/pull/123492) (issue: [#123430](https://github.com/elastic/elasticsearch/issues/123430))
* Fix queries with document level security on lookup indexes [#120617](https://github.com/elastic/elasticsearch/pull/120617) (issue: [#120509](https://github.com/elastic/elasticsearch/issues/120509))
* Fix writing for LOOKUP status [#119296](https://github.com/elastic/elasticsearch/pull/119296) (issue: [#119086](https://github.com/elastic/elasticsearch/issues/119086))
* Implicit numeric casting for CASE/GREATEST/LEAST [#122601](https://github.com/elastic/elasticsearch/pull/122601) (issue: [#121890](https://github.com/elastic/elasticsearch/issues/121890))
* Lazy collection copying during node transform [#124424](https://github.com/elastic/elasticsearch/pull/124424)
* Limit memory usage of `fold` [#118602](https://github.com/elastic/elasticsearch/pull/118602)
* Limit size of query [#117898](https://github.com/elastic/elasticsearch/pull/117898)
* Make `numberOfChannels` consistent with layout map by removing duplicated `ChannelSet` [#125636](https://github.com/elastic/elasticsearch/pull/125636)
* Reduce iteration complexity for plan traversal [#123427](https://github.com/elastic/elasticsearch/pull/123427)
* Remove redundant sorts from execution plan [#121156](https://github.com/elastic/elasticsearch/pull/121156)
* Revert unwanted ES|QL lexer changes from PR #120354 [#120538](https://github.com/elastic/elasticsearch/pull/120538)
* Revive inlinestats [#122257](https://github.com/elastic/elasticsearch/pull/122257)
* Revive some more of inlinestats functionality [#123589](https://github.com/elastic/elasticsearch/pull/123589)
* Use a must boolean statement when pushing down to Lucene when scoring is also needed [#124001](https://github.com/elastic/elasticsearch/pull/124001) (issue: [#123967](https://github.com/elastic/elasticsearch/issues/123967))

Engine:
* Hold store reference in `InternalEngine#performActionWithDirectoryReader(...)` [#123010](https://github.com/elastic/elasticsearch/pull/123010) (issue: [#122974](https://github.com/elastic/elasticsearch/issues/122974))

Health:
* Do not recommend increasing `max_shards_per_node` [#120458](https://github.com/elastic/elasticsearch/pull/120458)

Highlighting:
* Restore V8 REST compatibility around highlight `force_source` parameter [#124873](https://github.com/elastic/elasticsearch/pull/124873)

Indices APIs:
* Add `?master_timeout` to `POST /_ilm/migrate_to_data_tiers` [#120883](https://github.com/elastic/elasticsearch/pull/120883)
* Fix NPE in rolling over unknown target and return 404 [#125352](https://github.com/elastic/elasticsearch/pull/125352)
* Fix broken yaml test `30_create_from` [#120662](https://github.com/elastic/elasticsearch/pull/120662)
* Include hidden indices in `DeprecationInfoAction` [#118035](https://github.com/elastic/elasticsearch/pull/118035) (issue: [#118020](https://github.com/elastic/elasticsearch/issues/118020))
* Preventing `ConcurrentModificationException` when updating settings for more than one index [#126077](https://github.com/elastic/elasticsearch/pull/126077)
* Updates the deprecation info API to not warn about system indices and data streams [#122951](https://github.com/elastic/elasticsearch/pull/122951)

Inference:
* [Inference API] Put back legacy EIS URL setting [#121207](https://github.com/elastic/elasticsearch/pull/121207)

Infra/Core:
* Epoch Millis Rounding Down and Not Up 2 [#118353](https://github.com/elastic/elasticsearch/pull/118353)
* Fix system data streams to be restorable from a snapshot [#124651](https://github.com/elastic/elasticsearch/pull/124651) (issue: [#89261](https://github.com/elastic/elasticsearch/issues/89261))
* Have create index return a bad request on poor formatting [#123761](https://github.com/elastic/elasticsearch/pull/123761)
* Include data streams when converting an existing resource to a system resource [#121392](https://github.com/elastic/elasticsearch/pull/121392)
* System Index Migration Failure Results in a Non-Recoverable State [#122326](https://github.com/elastic/elasticsearch/pull/122326)
* System data streams are not being upgraded in the feature migration API [#124884](https://github.com/elastic/elasticsearch/pull/124884) (issue: [#122949](https://github.com/elastic/elasticsearch/issues/122949))
* Wrap jackson exception on malformed json string [#114445](https://github.com/elastic/elasticsearch/pull/114445) (issue: [#114142](https://github.com/elastic/elasticsearch/issues/114142))

Infra/Logging:
* Move `SlowLogFieldProvider` instantiation to node construction [#117949](https://github.com/elastic/elasticsearch/pull/117949)

Infra/Metrics:
* Make `randomInstantBetween` always return value in range [minInstant, `maxInstant]` [#114177](https://github.com/elastic/elasticsearch/pull/114177)

Infra/Plugins:
* Remove unnecessary entitlement [#120959](https://github.com/elastic/elasticsearch/pull/120959)
* Restrict agent entitlements to the system classloader unnamed module [#120546](https://github.com/elastic/elasticsearch/pull/120546)

Infra/REST API:
* Fixed a `NullPointerException` in `_capabilities` API when the `path` parameter is null. [#113413](https://github.com/elastic/elasticsearch/pull/113413) (issue: [#113413](https://github.com/elastic/elasticsearch/issues/113413))

Infra/Scripting:
* Register mustache size limit setting [#119291](https://github.com/elastic/elasticsearch/pull/119291)

Infra/Settings:
* Don't allow secure settings in YML config (109115) [#115779](https://github.com/elastic/elasticsearch/pull/115779) (issue: [#109115](https://github.com/elastic/elasticsearch/issues/109115))

Ingest Node:
* Add warning headers for ingest pipelines containing special characters [#114837](https://github.com/elastic/elasticsearch/pull/114837) (issue: [#104411](https://github.com/elastic/elasticsearch/issues/104411))
* Fix geoip databases index access after system feature migration [#121196](https://github.com/elastic/elasticsearch/pull/121196)
* Fix geoip databases index access after system feature migration (again) [#122938](https://github.com/elastic/elasticsearch/pull/122938)
* Fix geoip databases index access after system feature migration (take 3) [#124604](https://github.com/elastic/elasticsearch/pull/124604)

Logs:
* Always check if index mode is logsdb [#116922](https://github.com/elastic/elasticsearch/pull/116922)

Machine Learning:
* Add `ElasticInferenceServiceCompletionServiceSettings` [#123155](https://github.com/elastic/elasticsearch/pull/123155)
* Add enterprise license check to inference action for semantic text fields [#122293](https://github.com/elastic/elasticsearch/pull/122293)
* Avoid potentially throwing calls to Task#getDescription in model download [#124527](https://github.com/elastic/elasticsearch/pull/124527)
* Change format for Unified Chat [#121396](https://github.com/elastic/elasticsearch/pull/121396)
* Fix `AlibabaCloudSearchCompletionAction` not accepting `ChatCompletionInputs` [#125023](https://github.com/elastic/elasticsearch/pull/125023)
* Fix get all inference endponts not returning multiple endpoints sharing model deployment [#121821](https://github.com/elastic/elasticsearch/pull/121821)
* Fix serialising the inference update request [#122278](https://github.com/elastic/elasticsearch/pull/122278)
* Fixing bedrock event executor terminated cache issue [#118177](https://github.com/elastic/elasticsearch/pull/118177) (issue: [#117916](https://github.com/elastic/elasticsearch/issues/117916))
* Fixing bug setting index when parsing Google Vertex AI results [#117287](https://github.com/elastic/elasticsearch/pull/117287)
* Retry on streaming errors [#123076](https://github.com/elastic/elasticsearch/pull/123076)
* Set Connect Timeout to 5s [#123272](https://github.com/elastic/elasticsearch/pull/123272)
* Set default similarity for Cohere model to cosine [#125370](https://github.com/elastic/elasticsearch/pull/125370) (issue: [#122878](https://github.com/elastic/elasticsearch/issues/122878))
* Updating Inference Update API documentation to have the correct PUT method [#121048](https://github.com/elastic/elasticsearch/pull/121048)
* Wait for up to 2 seconds for yellow status before starting search [#115938](https://github.com/elastic/elasticsearch/pull/115938) (issues: [#107777](https://github.com/elastic/elasticsearch/issues/107777), [#105955](https://github.com/elastic/elasticsearch/issues/105955), [#107815](https://github.com/elastic/elasticsearch/issues/107815), [#112191](https://github.com/elastic/elasticsearch/issues/112191))
* [Inference API] Fix output stream ordering in `InferenceActionProxy` [#124225](https://github.com/elastic/elasticsearch/pull/124225)
* [Inference API] Fix unique ID message for inference ID matches trained model ID [#119543](https://github.com/elastic/elasticsearch/pull/119543) (issue: [#111312](https://github.com/elastic/elasticsearch/issues/111312))

Mapping:
* Avoid serializing empty `_source` fields in mappings [#122606](https://github.com/elastic/elasticsearch/pull/122606)
* Enable New Semantic Text Format Only On Newly Created Indices [#121556](https://github.com/elastic/elasticsearch/pull/121556)
* Fix Semantic Text 8.x Upgrade Bug [#125446](https://github.com/elastic/elasticsearch/pull/125446)
* Fix propagation of dynamic mapping parameter when applying `copy_to` [#121109](https://github.com/elastic/elasticsearch/pull/121109) (issue: [#113049](https://github.com/elastic/elasticsearch/issues/113049))
* Fix realtime get of nested fields with synthetic source [#119575](https://github.com/elastic/elasticsearch/pull/119575) (issue: [#119553](https://github.com/elastic/elasticsearch/issues/119553))
* Merge field mappers when updating mappings with [subobjects:false] [#120370](https://github.com/elastic/elasticsearch/pull/120370) (issue: [#120216](https://github.com/elastic/elasticsearch/issues/120216))
* Merge template mappings properly during validation [#124784](https://github.com/elastic/elasticsearch/pull/124784) (issue: [#123372](https://github.com/elastic/elasticsearch/issues/123372))
* Tweak `copy_to` handling in synthetic `_source` to account for nested objects [#120974](https://github.com/elastic/elasticsearch/pull/120974) (issue: [#120831](https://github.com/elastic/elasticsearch/issues/120831))

Network:
* Remove ChunkedToXContentBuilder [#119310](https://github.com/elastic/elasticsearch/pull/119310) (issue: [#118647](https://github.com/elastic/elasticsearch/issues/118647))

Ranking:
* Fix LTR query feature with phrases (and two-phase) queries [#125103](https://github.com/elastic/elasticsearch/pull/125103)

Search:
* Catch and handle disconnect exceptions in search [#115836](https://github.com/elastic/elasticsearch/pull/115836)
* Fix leak in `DfsQueryPhase` and introduce search disconnect stress test [#116060](https://github.com/elastic/elasticsearch/pull/116060) (issue: [#115056](https://github.com/elastic/elasticsearch/issues/115056))
* Fix/QueryBuilderBWCIT_muted_test [#117831](https://github.com/elastic/elasticsearch/pull/117831)
* Handle long overflow in dates [#124048](https://github.com/elastic/elasticsearch/pull/124048) (issue: [#112483](https://github.com/elastic/elasticsearch/issues/112483))
* Handle search timeout in `SuggestPhase` [#122357](https://github.com/elastic/elasticsearch/pull/122357) (issue: [#122186](https://github.com/elastic/elasticsearch/issues/122186))
* In this pr, a 400 error is returned when _source / _seq_no / _feature / _nested_path / _field_names is requested, rather a 5xx [#117229](https://github.com/elastic/elasticsearch/pull/117229)
* Inconsistency in the _analyzer api when the index is not included [#115930](https://github.com/elastic/elasticsearch/pull/115930)
* Let MLTQuery throw IAE when no analyzer is set [#124662](https://github.com/elastic/elasticsearch/pull/124662) (issue: [#124562](https://github.com/elastic/elasticsearch/issues/124562))
* Load `FieldInfos` from store if not yet initialised through a refresh on `IndexShard` [#125650](https://github.com/elastic/elasticsearch/pull/125650) (issue: [#125483](https://github.com/elastic/elasticsearch/issues/125483))
* Log stack traces on data nodes before they are cleared for transport [#125732](https://github.com/elastic/elasticsearch/pull/125732)
* Minor-Fixes Support 7x segments as archive in 8x / 9x [#125666](https://github.com/elastic/elasticsearch/pull/125666)
* Re-enable parallel collection for field sorted top hits [#125916](https://github.com/elastic/elasticsearch/pull/125916)
* Remove duplicate code in ESIntegTestCase [#120799](https://github.com/elastic/elasticsearch/pull/120799)
* SearchStatesIt failures reported by CI [#117618](https://github.com/elastic/elasticsearch/pull/117618) (issues: [#116617](https://github.com/elastic/elasticsearch/issues/116617), [#116618](https://github.com/elastic/elasticsearch/issues/116618))
* Skip fetching _inference_fields field in legacy semantic_text format [#121720](https://github.com/elastic/elasticsearch/pull/121720)
* Support indices created in ESv6 and updated in ESV7 using different LuceneCodecs as archive in current version. [#119503](https://github.com/elastic/elasticsearch/pull/119503) (issue: [#117042](https://github.com/elastic/elasticsearch/issues/117042))
* Test/107515 restore template with match only text mapper it fail [#120392](https://github.com/elastic/elasticsearch/pull/120392) (issue: [#107515](https://github.com/elastic/elasticsearch/issues/107515))
* Updated Date Range to Follow Documentation When Assuming Missing Values [#112258](https://github.com/elastic/elasticsearch/pull/112258) (issue: [#111484](https://github.com/elastic/elasticsearch/issues/111484))
* `CrossClusterIT` `testCancel` failure [#117750](https://github.com/elastic/elasticsearch/pull/117750) (issue: [#108061](https://github.com/elastic/elasticsearch/issues/108061))
* `SearchServiceTests.testParseSourceValidation` failure [#117963](https://github.com/elastic/elasticsearch/pull/117963)

Snapshot/Restore:
* Add undeclared Azure settings, modify test to exercise them [#118634](https://github.com/elastic/elasticsearch/pull/118634)
* Fork post-snapshot-delete cleanup off master thread [#122731](https://github.com/elastic/elasticsearch/pull/122731)
* Retry throttled snapshot deletions [#113237](https://github.com/elastic/elasticsearch/pull/113237)
* This PR fixes a bug whereby partial snapshots of system datastreams could be used to restore system features. [#124931](https://github.com/elastic/elasticsearch/pull/124931)
* Use the system index descriptor in the snapshot blob cache cleanup task [#120937](https://github.com/elastic/elasticsearch/pull/120937) (issue: [#120518](https://github.com/elastic/elasticsearch/issues/120518))

Store:
* Do not capture `ClusterChangedEvent` in `IndicesStore` call to #onClusterStateShardsClosed [#120193](https://github.com/elastic/elasticsearch/pull/120193)

Suggesters:
* Return an empty suggestion when suggest phase times out [#122575](https://github.com/elastic/elasticsearch/pull/122575) (issue: [#122548](https://github.com/elastic/elasticsearch/issues/122548))

Transform:
* If the Transform is configured to write to an alias as its destination index, when the delete_dest_index parameter is set to true, then the Delete API will now delete the write index backing the alias [#122074](https://github.com/elastic/elasticsearch/pull/122074) (issue: [#121913](https://github.com/elastic/elasticsearch/issues/121913))

Vector Search:
* Apply default k for knn query eagerly [#118774](https://github.com/elastic/elasticsearch/pull/118774)
* Fix `bbq_hnsw` merge file cleanup on random IO exceptions [#119691](https://github.com/elastic/elasticsearch/pull/119691) (issue: [#119392](https://github.com/elastic/elasticsearch/issues/119392))
* Knn vector rescoring to sort score docs [#122653](https://github.com/elastic/elasticsearch/pull/122653) (issue: [#119711](https://github.com/elastic/elasticsearch/issues/119711))
* Return appropriate error on null dims update instead of npe [#125716](https://github.com/elastic/elasticsearch/pull/125716)

Watcher:
* Watcher history index has too many indexed fields - [#117701](https://github.com/elastic/elasticsearch/pull/117701) (issue: [#71479](https://github.com/elastic/elasticsearch/issues/71479))

