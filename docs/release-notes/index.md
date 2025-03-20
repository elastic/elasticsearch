---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-release-notes.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/master/release-notes-9.1.0.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/master/migrating-9.1.html
---

# Elasticsearch release notes [elasticsearch-release-notes]

Review the changes, fixes, and more in each version of Elasticsearch.

To check for security updates, go to [Security announcements for the Elastic stack](https://discuss.elastic.co/c/announcements/security-announcements/31).

% Release notes include only features, enhancements, and fixes. Add breaking changes, deprecations, and known issues to the applicable release notes sections.

% ## version.next [elasticsearch-next-release-notes]
% **Release date:** Month day, year

% ### Features and enhancements [elasticsearch-next-features-enhancements]
% *

% ### Fixes [elasticsearch-next-fixes]
% *

## 9.1.0 [elasticsearch-910-release-notes]
**Release date:** April 01, 2025

### Highlights

::::{dropdown} Release semantic_text as a GA feature
semantic_text is now an official GA (generally available) feature! This field type allows you to easily set up and perform semantic search with minimal ramp up time.

For more information, check [PR #124669](https://github.com/elastic/elasticsearch/pull/124669).
::::

### Features and enhancements [elasticsearch-910-features-enhancements]

Allocation:
* Introduce `AllocationBalancingRoundSummaryService` [#120957](https://github.com/elastic/elasticsearch/pull/120957)

Authorization:
* Do not fetch reserved roles from native store when Get Role API is called [#121971](https://github.com/elastic/elasticsearch/pull/121971)

CRUD:
* Enhance memory accounting for document expansion and introduce max document size limit [#123543](https://github.com/elastic/elasticsearch/pull/123543)

Data streams:
* Add index mode to get data stream API [#122486](https://github.com/elastic/elasticsearch/pull/122486)
* Reindex data stream indices on different nodes [#125171](https://github.com/elastic/elasticsearch/pull/125171)
* Retry ILM async action after reindexing data stream [#124149](https://github.com/elastic/elasticsearch/pull/124149)
* Run `TransportGetDataStreamsAction` on local node [#122852](https://github.com/elastic/elasticsearch/pull/122852)
* Set cause on create index request in create from action [#124363](https://github.com/elastic/elasticsearch/pull/124363)

Downsampling:
* Improve downsample performance by buffering docids and do bulk processing [#124477](https://github.com/elastic/elasticsearch/pull/124477)
* Improve rolling up metrics [#124739](https://github.com/elastic/elasticsearch/pull/124739)

ES|QL:
* Add initial grammar and changes for FORK [#121948](https://github.com/elastic/elasticsearch/pull/121948)
* Add initial grammar and planning for RRF (snapshot) [#123396](https://github.com/elastic/elasticsearch/pull/123396)
* Allow partial results in ES|QL [#121942](https://github.com/elastic/elasticsearch/pull/121942)
* Allow skip shards with `_tier` and `_index` in ES|QL [#123728](https://github.com/elastic/elasticsearch/pull/123728)
* Avoid `NamedWritable` in block serialization [#124394](https://github.com/elastic/elasticsearch/pull/124394)
* Double parameter markers for identifiers [#122459](https://github.com/elastic/elasticsearch/pull/122459)
* ESQL: Align `RENAME` behavior with `EVAL` for sequential processing [#122250](https://github.com/elastic/elasticsearch/pull/122250) (issue: [#122250](https://github.com/elastic/elasticsearch/pull/122250))
* ES|QL - Add scoring for full text functions disjunctions [#121793](https://github.com/elastic/elasticsearch/pull/121793)
* ES|QL slow log [#124094](https://github.com/elastic/elasticsearch/pull/124094)
* ES|QL: Support `::date` in inline cast [#123460](https://github.com/elastic/elasticsearch/pull/123460) (issue: [#123460](https://github.com/elastic/elasticsearch/pull/123460))
* Fix Driver status iterations and `cpuTime` [#123290](https://github.com/elastic/elasticsearch/pull/123290) (issue: [#123290](https://github.com/elastic/elasticsearch/pull/123290))
* Implement runtime skip_unavailable=true [#121240](https://github.com/elastic/elasticsearch/pull/121240)
* Include failures in partial response [#124929](https://github.com/elastic/elasticsearch/pull/124929)
* Initial support for unmapped fields [#119886](https://github.com/elastic/elasticsearch/pull/119886)
* Introduce `allow_partial_results` setting in ES|QL [#122890](https://github.com/elastic/elasticsearch/pull/122890)
* Introduce a pre-mapping logical plan processing step [#121260](https://github.com/elastic/elasticsearch/pull/121260)
* Pragma to load from stored fields [#122891](https://github.com/elastic/elasticsearch/pull/122891)
* Push down `StartsWith` and `EndsWith` functions to Lucene [#123381](https://github.com/elastic/elasticsearch/pull/123381) (issue: [#123381](https://github.com/elastic/elasticsearch/pull/123381))
* Remove page alignment in exchange sink [#124610](https://github.com/elastic/elasticsearch/pull/124610)
* Render `aggregate_metric_double` [#122660](https://github.com/elastic/elasticsearch/pull/122660)
* Report failures on partial results [#124823](https://github.com/elastic/elasticsearch/pull/124823)
* Retry ES|QL node requests on shard level failures [#120774](https://github.com/elastic/elasticsearch/pull/120774)
* Reuse child `outputSet` inside the plan where possible [#124611](https://github.com/elastic/elasticsearch/pull/124611)
* Support partial results in CCS in ES|QL [#122708](https://github.com/elastic/elasticsearch/pull/122708)
* Support subset of metrics in aggregate metric double [#121805](https://github.com/elastic/elasticsearch/pull/121805)
* `ToAggregateMetricDouble` function [#124595](https://github.com/elastic/elasticsearch/pull/124595)

Engine:
* Threadpool merge scheduler [#120869](https://github.com/elastic/elasticsearch/pull/120869)

Health:
* Add health indicator impact to `HealthPeriodicLogger` [#122390](https://github.com/elastic/elasticsearch/pull/122390)

ILM+SLM:
* Improve SLM Health Indicator to cover missing snapshot [#121370](https://github.com/elastic/elasticsearch/pull/121370)
* Run `TransportExplainLifecycleAction` on local node [#122885](https://github.com/elastic/elasticsearch/pull/122885)
* Truncate `step_info` and error reason in ILM execution state and history [#125054](https://github.com/elastic/elasticsearch/pull/125054) (issue: [#125054](https://github.com/elastic/elasticsearch/pull/125054))

Indices APIs:
* Run `TransportGetMappingsAction` on local node [#122921](https://github.com/elastic/elasticsearch/pull/122921)

Inference:
* [Inference API] Rename `model_id` prop to model in EIS sparse inference request body [#122272](https://github.com/elastic/elasticsearch/pull/122272)

Infra/CLI:
* Ignore _JAVA_OPTIONS [#124843](https://github.com/elastic/elasticsearch/pull/124843)

Infra/Core:
* Give Kibana user 'all' permissions for .entity_analytics.* indices [#123588](https://github.com/elastic/elasticsearch/pull/123588)
* Improve size limiting string message [#122427](https://github.com/elastic/elasticsearch/pull/122427)
* Permanently switch from Java SecurityManager to Entitlements. The Java SecurityManager has been deprecated since Java 17, and it is now completely disabled in Java 24. In order to retain an similar level of protection, Elasticsearch implemented its own protection mechanism, Entitlements. Starting with this version, Entitlements will permanently replace the Java SecurityManager. [#125117](https://github.com/elastic/elasticsearch/pull/125117)

Infra/REST API:
* Indicate when errors represent timeouts [#124936](https://github.com/elastic/elasticsearch/pull/124936)

Infra/Settings:
* Allow passing several reserved state chunks in single process call [#124574](https://github.com/elastic/elasticsearch/pull/124574)

Ingest Node:
* Allow setting the `type` in the reroute processor [#122409](https://github.com/elastic/elasticsearch/pull/122409) (issue: [#122409](https://github.com/elastic/elasticsearch/pull/122409))
* Run `TransportEnrichStatsAction` on local node [#121256](https://github.com/elastic/elasticsearch/pull/121256)

Machine Learning:
* Add `ModelRegistryMetadata` to Cluster State [#121106](https://github.com/elastic/elasticsearch/pull/121106)
* Adding elser default endpoint for EIS [#122066](https://github.com/elastic/elasticsearch/pull/122066)
* Adding endpoint creation validation to `ElasticInferenceService` [#117642](https://github.com/elastic/elasticsearch/pull/117642)
* Adding integration for VoyageAI embeddings and rerank models [#122134](https://github.com/elastic/elasticsearch/pull/122134)
* Adding support for binary embedding type to Cohere service embedding type [#120751](https://github.com/elastic/elasticsearch/pull/120751)
* Adding support for specifying embedding type to Jina AI service settings [#121548](https://github.com/elastic/elasticsearch/pull/121548)
* ES|QL `change_point` processing command [#120998](https://github.com/elastic/elasticsearch/pull/120998)
* Expose `input_type` option at root level for `text_embedding` task type in Perform Inference API [#122638](https://github.com/elastic/elasticsearch/pull/122638) (issue: [#122638](https://github.com/elastic/elasticsearch/pull/122638))
* Integrate with `DeepSeek` API [#122218](https://github.com/elastic/elasticsearch/pull/122218)
* Limit the number of chunks for semantic text to prevent high memory usage [#123150](https://github.com/elastic/elasticsearch/pull/123150)
* Upgrade AWS v2 SDK to 2.30.38 [#124738](https://github.com/elastic/elasticsearch/pull/124738)
* [Inference API] Propagate product use case http header to EIS [#124025](https://github.com/elastic/elasticsearch/pull/124025)

Mapping:
* Enable synthetic recovery source by default when synthetic source is enabled. Using synthetic recovery source significantly improves indexing performance compared to regular recovery source. [#122615](https://github.com/elastic/elasticsearch/pull/122615) (issue: [#122615](https://github.com/elastic/elasticsearch/pull/122615))
* Enable the use of nested field type with index.mode=time_series [#122224](https://github.com/elastic/elasticsearch/pull/122224) (issue: [#122224](https://github.com/elastic/elasticsearch/pull/122224))
* Improved error message when index field type is invalid [#122860](https://github.com/elastic/elasticsearch/pull/122860)
* Introduce `FallbackSyntheticSourceBlockLoader` and apply it to keyword fields [#119546](https://github.com/elastic/elasticsearch/pull/119546)
* Release semantic_text as a GA feature [#124669](https://github.com/elastic/elasticsearch/pull/124669)
* Store arrays offsets for ip fields natively with synthetic source [#122999](https://github.com/elastic/elasticsearch/pull/122999)
* Store arrays offsets for keyword fields natively with synthetic source instead of falling back to ignored source. [#113757](https://github.com/elastic/elasticsearch/pull/113757)
* Store arrays offsets for numeric fields natively with synthetic source [#124594](https://github.com/elastic/elasticsearch/pull/124594)
* Use `FallbackSyntheticSourceBlockLoader` for `shape` and `geo_shape` [#124927](https://github.com/elastic/elasticsearch/pull/124927)
* Use `FallbackSyntheticSourceBlockLoader` for `unsigned_long` and `scaled_float` fields [#122637](https://github.com/elastic/elasticsearch/pull/122637)
* Use `FallbackSyntheticSourceBlockLoader` for boolean and date fields [#124050](https://github.com/elastic/elasticsearch/pull/124050)
* Use `FallbackSyntheticSourceBlockLoader` for number fields [#122280](https://github.com/elastic/elasticsearch/pull/122280)

Packaging:
* Update bundled JDK to Java 24 [#125159](https://github.com/elastic/elasticsearch/pull/125159)

Ranking:
* Leverage scorer supplier in `QueryFeatureExtractor` [#125259](https://github.com/elastic/elasticsearch/pull/125259)

Relevance:
* Skip semantic_text embedding generation when no content is provided. [#123763](https://github.com/elastic/elasticsearch/pull/123763)

Search:
* Account for the `SearchHit` source in circuit breaker [#121920](https://github.com/elastic/elasticsearch/pull/121920) (issue: [#121920](https://github.com/elastic/elasticsearch/pull/121920))
* Added optional parameters to QSTR ES|QL function [#121787](https://github.com/elastic/elasticsearch/pull/121787) (issue: [#121787](https://github.com/elastic/elasticsearch/pull/121787))
* Optimize memory usage in `ShardBulkInferenceActionFilter` [#124313](https://github.com/elastic/elasticsearch/pull/124313)
* Optionally allow text similarity reranking to fail [#121784](https://github.com/elastic/elasticsearch/pull/121784)

Security:
* Bump nimbus-jose-jwt to 10.0.2 [#124544](https://github.com/elastic/elasticsearch/pull/124544)

Snapshot/Restore:
* GCS blob store: add `OperationPurpose/Operation` stats counters [#122991](https://github.com/elastic/elasticsearch/pull/122991)
* Retry when the server can't be resolved (Google Cloud Storage) [#123852](https://github.com/elastic/elasticsearch/pull/123852)
* Upgrade AWS SDK to v1.12.746 [#122431](https://github.com/elastic/elasticsearch/pull/122431)

Stats:
* Run XPack usage actions on local node [#122933](https://github.com/elastic/elasticsearch/pull/122933)

Store:
* Abort pending deletion on `IndicesService` close [#123569](https://github.com/elastic/elasticsearch/pull/123569)

Vector Search:
* Add bit vector support to semantic text [#123187](https://github.com/elastic/elasticsearch/pull/123187)
* Adds implementations of dotProduct and cosineSimilarity painless methods to operate on float vectors for byte fields [#122381](https://github.com/elastic/elasticsearch/pull/122381) (issue: [#122381](https://github.com/elastic/elasticsearch/pull/122381))
* New `vector_rescore` parameter as a quantized index type option [#124581](https://github.com/elastic/elasticsearch/pull/124581)

Watcher:
* Run `TransportGetWatcherSettingsAction` on local node [#122857](https://github.com/elastic/elasticsearch/pull/122857)

### Fixes [elasticsearch-910-fixes]

Allocation:
* `DesiredBalanceReconciler` always returns `AllocationStats` [#122458](https://github.com/elastic/elasticsearch/pull/122458)

Analysis:
* Adjust exception thrown when unable to load hunspell dict [#123743](https://github.com/elastic/elasticsearch/pull/123743)

Cluster Coordination:
* Disable logging in `ClusterFormationFailureHelper` on shutdown [#125244](https://github.com/elastic/elasticsearch/pull/125244) (issue: [#125244](https://github.com/elastic/elasticsearch/pull/125244))

Data streams:
* Updating `TransportRolloverAction.checkBlock` so that non-write-index blocks do not prevent data stream rollover [#122905](https://github.com/elastic/elasticsearch/pull/122905)

Distributed:
* Pass `IndexReshardingMetadata` over the wire [#124841](https://github.com/elastic/elasticsearch/pull/124841)

Downsampling:
* Improve downsample performance by avoiding to read unnecessary dimension values when downsampling. [#124451](https://github.com/elastic/elasticsearch/pull/124451)

EQL:
* Fix EQL double invoking listener [#124918](https://github.com/elastic/elasticsearch/pull/124918)

ES|QL:
* Add support to VALUES aggregation for spatial types [#122886](https://github.com/elastic/elasticsearch/pull/122886) (issue: [#122886](https://github.com/elastic/elasticsearch/pull/122886))
* Avoid over collecting in Limit or Lucene Operator [#123296](https://github.com/elastic/elasticsearch/pull/123296)
* Catch parsing exception [#124958](https://github.com/elastic/elasticsearch/pull/124958) (issue: [#124958](https://github.com/elastic/elasticsearch/pull/124958))
* Change the order of the optimization rules [#124335](https://github.com/elastic/elasticsearch/pull/124335)
* ESQL: Fix inconsistent results in using scaled_float field [#122586](https://github.com/elastic/elasticsearch/pull/122586) (issue: [#122586](https://github.com/elastic/elasticsearch/pull/122586))
* ESQL: Remove estimated row size assertion [#122762](https://github.com/elastic/elasticsearch/pull/122762) (issue: [#122762](https://github.com/elastic/elasticsearch/pull/122762))
* ES|QL: Fix scoring for full text functions [#124540](https://github.com/elastic/elasticsearch/pull/124540)
* Fix early termination in `LuceneSourceOperator` [#123197](https://github.com/elastic/elasticsearch/pull/123197)
* Fix function registry concurrency issues on constructor [#123492](https://github.com/elastic/elasticsearch/pull/123492) (issue: [#123492](https://github.com/elastic/elasticsearch/pull/123492))
* Fix functions emitting warnings with no source [#122821](https://github.com/elastic/elasticsearch/pull/122821) (issue: [#122821](https://github.com/elastic/elasticsearch/pull/122821))
* Implicit numeric casting for CASE/GREATEST/LEAST [#122601](https://github.com/elastic/elasticsearch/pull/122601) (issue: [#122601](https://github.com/elastic/elasticsearch/pull/122601))
* Improve error message for ( and [ [#124177](https://github.com/elastic/elasticsearch/pull/124177) (issue: [#124177](https://github.com/elastic/elasticsearch/pull/124177))
* Lazy collection copying during node transform [#124424](https://github.com/elastic/elasticsearch/pull/124424)
* Reduce iteration complexity for plan traversal [#123427](https://github.com/elastic/elasticsearch/pull/123427)
* Remove duplicated nested commands [#123085](https://github.com/elastic/elasticsearch/pull/123085)
* Revive inlinestats [#122257](https://github.com/elastic/elasticsearch/pull/122257)
* Revive some more of inlinestats functionality [#123589](https://github.com/elastic/elasticsearch/pull/123589)
* TO_LOWER processes all values [#124676](https://github.com/elastic/elasticsearch/pull/124676) (issue: [#124676](https://github.com/elastic/elasticsearch/pull/124676))
* Use a must boolean statement when pushing down to Lucene when scoring is also needed [#124001](https://github.com/elastic/elasticsearch/pull/124001) (issue: [#124001](https://github.com/elastic/elasticsearch/pull/124001))

Engine:
* Hold store reference in `InternalEngine#performActionWithDirectoryReader(...)` [#123010](https://github.com/elastic/elasticsearch/pull/123010) (issue: [#123010](https://github.com/elastic/elasticsearch/pull/123010))

Highlighting:
* Restore V8 REST compatibility around highlight `force_source` parameter [#124873](https://github.com/elastic/elasticsearch/pull/124873)

Indices APIs:
* Avoid hoarding cluster state references during rollover [#124107](https://github.com/elastic/elasticsearch/pull/124107) (issue: [#124107](https://github.com/elastic/elasticsearch/pull/124107))
* Updates the deprecation info API to not warn about system indices and data streams [#122951](https://github.com/elastic/elasticsearch/pull/122951)

Infra/Core:
* Fix system data streams to be restorable from a snapshot [#124651](https://github.com/elastic/elasticsearch/pull/124651) (issue: [#124651](https://github.com/elastic/elasticsearch/pull/124651))
* Have create index return a bad request on poor formatting [#123761](https://github.com/elastic/elasticsearch/pull/123761)
* Include data streams when converting an existing resource to a system resource [#121392](https://github.com/elastic/elasticsearch/pull/121392)
* Prevent rare starvation bug when using scaling `EsThreadPoolExecutor` with empty core pool size. [#124732](https://github.com/elastic/elasticsearch/pull/124732) (issue: [#124732](https://github.com/elastic/elasticsearch/pull/124732))
* Reduce Data Loss in System Indices Migration [#121327](https://github.com/elastic/elasticsearch/pull/121327)
* System Index Migration Failure Results in a Non-Recoverable State [#122326](https://github.com/elastic/elasticsearch/pull/122326)

Ingest Node:
* Fix geoip databases index access after system feature migration (again) [#122938](https://github.com/elastic/elasticsearch/pull/122938)
* Fix geoip databases index access after system feature migration (take 3) [#124604](https://github.com/elastic/elasticsearch/pull/124604)
* apm-data: Use representative count as event.success_count if available [#119995](https://github.com/elastic/elasticsearch/pull/119995)

Machine Learning:
* Add `ElasticInferenceServiceCompletionServiceSettings` [#123155](https://github.com/elastic/elasticsearch/pull/123155)
* Add enterprise license check to inference action for semantic text fields [#122293](https://github.com/elastic/elasticsearch/pull/122293)
* Avoid potentially throwing calls to Task#getDescription in model download [#124527](https://github.com/elastic/elasticsearch/pull/124527)
* Fix `AlibabaCloudSearchCompletionAction` not accepting `ChatCompletionInputs` [#125023](https://github.com/elastic/elasticsearch/pull/125023)
* Fix serialising the inference update request [#122278](https://github.com/elastic/elasticsearch/pull/122278)
* Migrate `model_version` to `model_id` when parsing persistent elser inference endpoints [#124769](https://github.com/elastic/elasticsearch/pull/124769) (issue: [#124769](https://github.com/elastic/elasticsearch/pull/124769))
* Provide model size statistics as soon as an anomaly detection job is opened [#124638](https://github.com/elastic/elasticsearch/pull/124638) (issue: [#124638](https://github.com/elastic/elasticsearch/pull/124638))
* Retry on streaming errors [#123076](https://github.com/elastic/elasticsearch/pull/123076)
* Set Connect Timeout to 5s [#123272](https://github.com/elastic/elasticsearch/pull/123272)
* Updates to allow using Cohere binary embedding response in semantic search queries [#121827](https://github.com/elastic/elasticsearch/pull/121827)
* [Inference API] Fix output stream ordering in `InferenceActionProxy` [#124225](https://github.com/elastic/elasticsearch/pull/124225)

Mapping:
* Avoid serializing empty `_source` fields in mappings [#122606](https://github.com/elastic/elasticsearch/pull/122606)
* Merge template mappings properly during validation [#124784](https://github.com/elastic/elasticsearch/pull/124784) (issue: [#124784](https://github.com/elastic/elasticsearch/pull/124784))

Ranking:
* Fix LTR query feature with phrases (and two-phase) queries [#125103](https://github.com/elastic/elasticsearch/pull/125103)
* Restore `TextSimilarityRankBuilder` XContent output [#124564](https://github.com/elastic/elasticsearch/pull/124564)

Relevance:
* Prevent Query Rule Creation with Invalid Numeric Match Criteria [#122823](https://github.com/elastic/elasticsearch/pull/122823)

Search:
* Do not let `ShardBulkInferenceActionFilter` unwrap / rewrap ESExceptions [#123890](https://github.com/elastic/elasticsearch/pull/123890)
* Don't generate stacktrace in `TaskCancelledException` [#125002](https://github.com/elastic/elasticsearch/pull/125002)
* Fix concurrency issue in `ScriptSortBuilder` [#123757](https://github.com/elastic/elasticsearch/pull/123757)
* Fix handling of auto expand replicas for stateless indices [#122365](https://github.com/elastic/elasticsearch/pull/122365)
* Handle search timeout in `SuggestPhase` [#122357](https://github.com/elastic/elasticsearch/pull/122357) (issue: [#122357](https://github.com/elastic/elasticsearch/pull/122357))
* Let MLTQuery throw IAE when no analyzer is set [#124662](https://github.com/elastic/elasticsearch/pull/124662) (issue: [#124662](https://github.com/elastic/elasticsearch/pull/124662))

Snapshot/Restore:
* Fork post-snapshot-delete cleanup off master thread [#122731](https://github.com/elastic/elasticsearch/pull/122731)
* Limit number of suppressed S3 deletion errors [#123630](https://github.com/elastic/elasticsearch/pull/123630) (issue: [#123630](https://github.com/elastic/elasticsearch/pull/123630))
* This PR fixes a bug whereby partial snapshots of system datastreams could be used to restore system features. [#124931](https://github.com/elastic/elasticsearch/pull/124931)

Suggesters:
* Return an empty suggestion when suggest phase times out [#122575](https://github.com/elastic/elasticsearch/pull/122575) (issue: [#122575](https://github.com/elastic/elasticsearch/pull/122575))
* Support duplicate suggestions in completion field [#121324](https://github.com/elastic/elasticsearch/pull/121324) (issue: [#121324](https://github.com/elastic/elasticsearch/pull/121324))

Transform:
* If the Transform is configured to write to an alias as its destination index, when the delete_dest_index parameter is set to true, then the Delete API will now delete the write index backing the alias [#122074](https://github.com/elastic/elasticsearch/pull/122074) (issue: [#122074](https://github.com/elastic/elasticsearch/pull/122074))

Vector Search:
* Knn vector rescoring to sort score docs [#122653](https://github.com/elastic/elasticsearch/pull/122653) (issue: [#122653](https://github.com/elastic/elasticsearch/pull/122653))


