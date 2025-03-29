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

% ### Features and enhancements [elasticsearch-next-features-enhancements]
% *

% ### Fixes [elasticsearch-next-fixes]
% *

## 9.1.0 [elasticsearch-910-release-notes]
**Release date:** April 01, 2025

### Highlights [elasticsearch-910-highlights]

::::{dropdown} Release semantic_text as a GA feature
semantic_text is now an official GA (generally available) feature! This field type allows you to easily set up and perform semantic search with minimal ramp up time.

For more information, check [PR #124669](https://github.com/elastic/elasticsearch/pull/124669).
::::

### Features and enhancements [elasticsearch-910-features-enhancements]

Allocation:
* Add cache support in `TransportGetAllocationStatsAction` [#124898](https://github.com/elastic/elasticsearch/pull/124898) (issue: [#124898](https://github.com/elastic/elasticsearch/pull/124898))
* Introduce `AllocationBalancingRoundSummaryService` [#120957](https://github.com/elastic/elasticsearch/pull/120957)

CRUD:
* Enhance memory accounting for document expansion and introduce max document size limit [#123543](https://github.com/elastic/elasticsearch/pull/123543)

Data streams:
* Add index mode to get data stream API [#122486](https://github.com/elastic/elasticsearch/pull/122486)
* Reindex data stream indices on different nodes [#125171](https://github.com/elastic/elasticsearch/pull/125171)
* Run `TransportGetDataStreamLifecycleAction` on local node [#125214](https://github.com/elastic/elasticsearch/pull/125214)
* Run `TransportGetDataStreamOptionsAction` on local node [#125213](https://github.com/elastic/elasticsearch/pull/125213)
* Run `TransportGetDataStreamsAction` on local node [#122852](https://github.com/elastic/elasticsearch/pull/122852)

ES|QL:
* Add initial grammar and changes for FORK [#121948](https://github.com/elastic/elasticsearch/pull/121948)
* Add initial grammar and planning for RRF (snapshot) [#123396](https://github.com/elastic/elasticsearch/pull/123396)
* Allow partial results in ES|QL [#121942](https://github.com/elastic/elasticsearch/pull/121942)
* Avoid `NamedWritable` in block serialization [#124394](https://github.com/elastic/elasticsearch/pull/124394)
* Calculate concurrent node limit [#124901](https://github.com/elastic/elasticsearch/pull/124901)
* Double parameter markers for identifiers [#122459](https://github.com/elastic/elasticsearch/pull/122459)
* ESQL: Align `RENAME` behavior with `EVAL` for sequential processing [#122250](https://github.com/elastic/elasticsearch/pull/122250) (issue: [#122250](https://github.com/elastic/elasticsearch/pull/122250))
* ES|QL slow log [#124094](https://github.com/elastic/elasticsearch/pull/124094)
* ES|QL: Support `::date` in inline cast [#123460](https://github.com/elastic/elasticsearch/pull/123460) (issue: [#123460](https://github.com/elastic/elasticsearch/pull/123460))
* Implement runtime skip_unavailable=true [#121240](https://github.com/elastic/elasticsearch/pull/121240)
* Include failures in partial response [#124929](https://github.com/elastic/elasticsearch/pull/124929)
* Introduce `allow_partial_results` setting in ES|QL [#122890](https://github.com/elastic/elasticsearch/pull/122890)
* Introduce a pre-mapping logical plan processing step [#121260](https://github.com/elastic/elasticsearch/pull/121260)
* Keep ordinals in conversion functions [#125357](https://github.com/elastic/elasticsearch/pull/125357)
* Pragma to load from stored fields [#122891](https://github.com/elastic/elasticsearch/pull/122891)
* Remove page alignment in exchange sink [#124610](https://github.com/elastic/elasticsearch/pull/124610)
* Render `aggregate_metric_double` [#122660](https://github.com/elastic/elasticsearch/pull/122660)
* Report `original_types` [#124913](https://github.com/elastic/elasticsearch/pull/124913)
* Report failures on partial results [#124823](https://github.com/elastic/elasticsearch/pull/124823)
* Retry ES|QL node requests on shard level failures [#120774](https://github.com/elastic/elasticsearch/pull/120774)
* Support partial results in CCS in ES|QL [#122708](https://github.com/elastic/elasticsearch/pull/122708)
* Support subset of metrics in aggregate metric double [#121805](https://github.com/elastic/elasticsearch/pull/121805)
* `ToAggregateMetricDouble` function [#124595](https://github.com/elastic/elasticsearch/pull/124595)

Engine:
* Threadpool merge scheduler [#120869](https://github.com/elastic/elasticsearch/pull/120869)

Health:
* Add health indicator impact to `HealthPeriodicLogger` [#122390](https://github.com/elastic/elasticsearch/pull/122390)

ILM+SLM:
* Improve SLM Health Indicator to cover missing snapshot [#121370](https://github.com/elastic/elasticsearch/pull/121370)
* Process ILM cluster state updates on another thread [#123712](https://github.com/elastic/elasticsearch/pull/123712)
* Run `TransportExplainLifecycleAction` on local node [#122885](https://github.com/elastic/elasticsearch/pull/122885)
* Truncate `step_info` and error reason in ILM execution state and history [#125054](https://github.com/elastic/elasticsearch/pull/125054) (issue: [#125054](https://github.com/elastic/elasticsearch/pull/125054))

Indices APIs:
* Avoid creating known_fields for every check in Alias [#124690](https://github.com/elastic/elasticsearch/pull/124690)
* Run `TransportGetMappingsAction` on local node [#122921](https://github.com/elastic/elasticsearch/pull/122921)

Inference:
* [Inference API] Rename `model_id` prop to model in EIS sparse inference request body [#122272](https://github.com/elastic/elasticsearch/pull/122272)

Infra/Core:
* Give Kibana user 'all' permissions for .entity_analytics.* indices [#123588](https://github.com/elastic/elasticsearch/pull/123588)
* Permanently switch from Java SecurityManager to Entitlements. The Java SecurityManager has been deprecated since Java 17, and it is now completely disabled in Java 24. In order to retain an similar level of protection, Elasticsearch implemented its own protection mechanism, Entitlements. Starting with this version, Entitlements will permanently replace the Java SecurityManager. [#125117](https://github.com/elastic/elasticsearch/pull/125117)

Infra/Settings:
* Allow passing several reserved state chunks in single process call [#124574](https://github.com/elastic/elasticsearch/pull/124574)

Ingest Node:
* Run `TransportEnrichStatsAction` on local node [#121256](https://github.com/elastic/elasticsearch/pull/121256)

Machine Learning:
* Add `ModelRegistryMetadata` to Cluster State [#121106](https://github.com/elastic/elasticsearch/pull/121106)
* Adding common rerank options to Perform Inference API [#125239](https://github.com/elastic/elasticsearch/pull/125239) (issue: [#125239](https://github.com/elastic/elasticsearch/pull/125239))
* Adding elser default endpoint for EIS [#122066](https://github.com/elastic/elasticsearch/pull/122066)
* Adding endpoint creation validation to `ElasticInferenceService` [#117642](https://github.com/elastic/elasticsearch/pull/117642)
* Adding integration for VoyageAI embeddings and rerank models [#122134](https://github.com/elastic/elasticsearch/pull/122134)
* Adding support for binary embedding type to Cohere service embedding type [#120751](https://github.com/elastic/elasticsearch/pull/120751)
* Adding support for specifying embedding type to Jina AI service settings [#121548](https://github.com/elastic/elasticsearch/pull/121548)
* Check if the anomaly results index has been rolled over [#125404](https://github.com/elastic/elasticsearch/pull/125404)
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
* Store arrays offsets for boolean fields natively with synthetic source [#125529](https://github.com/elastic/elasticsearch/pull/125529)
* Store arrays offsets for ip fields natively with synthetic source [#122999](https://github.com/elastic/elasticsearch/pull/122999)
* Store arrays offsets for keyword fields natively with synthetic source instead of falling back to ignored source. [#113757](https://github.com/elastic/elasticsearch/pull/113757)
* Store arrays offsets for numeric fields natively with synthetic source [#124594](https://github.com/elastic/elasticsearch/pull/124594)
* Store arrays offsets for unsigned long fields natively with synthetic source [#125709](https://github.com/elastic/elasticsearch/pull/125709)
* Use `FallbackSyntheticSourceBlockLoader` for `shape` and `geo_shape` [#124927](https://github.com/elastic/elasticsearch/pull/124927)
* Use `FallbackSyntheticSourceBlockLoader` for `unsigned_long` and `scaled_float` fields [#122637](https://github.com/elastic/elasticsearch/pull/122637)
* Use `FallbackSyntheticSourceBlockLoader` for boolean and date fields [#124050](https://github.com/elastic/elasticsearch/pull/124050)
* Use `FallbackSyntheticSourceBlockLoader` for number fields [#122280](https://github.com/elastic/elasticsearch/pull/122280)

Ranking:
* Leverage scorer supplier in `QueryFeatureExtractor` [#125259](https://github.com/elastic/elasticsearch/pull/125259)

Relevance:
* Skip semantic_text embedding generation when no content is provided. [#123763](https://github.com/elastic/elasticsearch/pull/123763)

Search:
* Account for the `SearchHit` source in circuit breaker [#121920](https://github.com/elastic/elasticsearch/pull/121920) (issue: [#121920](https://github.com/elastic/elasticsearch/pull/121920))
* Optimize memory usage in `ShardBulkInferenceActionFilter` [#124313](https://github.com/elastic/elasticsearch/pull/124313)
* Optionally allow text similarity reranking to fail [#121784](https://github.com/elastic/elasticsearch/pull/121784)

Snapshot/Restore:
* GCS blob store: add `OperationPurpose/Operation` stats counters [#122991](https://github.com/elastic/elasticsearch/pull/122991)
* Retry when the server can't be resolved (Google Cloud Storage) [#123852](https://github.com/elastic/elasticsearch/pull/123852)
* Upgrade to repository-gcs to use com.google.cloud:google-cloud-storage-bom:2.50.0 [#124062](https://github.com/elastic/elasticsearch/pull/124062)

Stats:
* Run XPack usage actions on local node [#122933](https://github.com/elastic/elasticsearch/pull/122933)

Store:
* Abort pending deletion on `IndicesService` close [#123569](https://github.com/elastic/elasticsearch/pull/123569)

Vector Search:
* Add bit vector support to semantic text [#123187](https://github.com/elastic/elasticsearch/pull/123187)
* Add panama implementations of byte-bit and float-bit script operations [#124722](https://github.com/elastic/elasticsearch/pull/124722) (issue: [#124722](https://github.com/elastic/elasticsearch/pull/124722))
* Adds implementations of dotProduct and cosineSimilarity painless methods to operate on float vectors for byte fields [#122381](https://github.com/elastic/elasticsearch/pull/122381) (issue: [#122381](https://github.com/elastic/elasticsearch/pull/122381))
* Allow zero for `rescore_vector.oversample` to indicate by-passing oversample and rescoring [#125599](https://github.com/elastic/elasticsearch/pull/125599)
* New `vector_rescore` parameter as a quantized index type option [#124581](https://github.com/elastic/elasticsearch/pull/124581)

Watcher:
* Run `TransportGetWatcherSettingsAction` on local node [#122857](https://github.com/elastic/elasticsearch/pull/122857)

### Fixes [elasticsearch-910-fixes]

Aggregations:
* Aggs: Let terms run in global ords mode no match [#124782](https://github.com/elastic/elasticsearch/pull/124782)

Allocation:
* `DesiredBalanceReconciler` always returns `AllocationStats` [#122458](https://github.com/elastic/elasticsearch/pull/122458)

Analysis:
* Non existing synonyms sets do not fail shard recovery for indices [#125659](https://github.com/elastic/elasticsearch/pull/125659) (issue: [#125659](https://github.com/elastic/elasticsearch/pull/125659))

Cluster Coordination:
* Disable logging in `ClusterFormationFailureHelper` on shutdown [#125244](https://github.com/elastic/elasticsearch/pull/125244) (issue: [#125244](https://github.com/elastic/elasticsearch/pull/125244))

Distributed:
* Pass `IndexReshardingMetadata` over the wire [#124841](https://github.com/elastic/elasticsearch/pull/124841)

EQL:
* Fix EQL double invoking listener [#124918](https://github.com/elastic/elasticsearch/pull/124918)

ES|QL:
* ESQL - date nanos range bug? [#125345](https://github.com/elastic/elasticsearch/pull/125345) (issue: [#125345](https://github.com/elastic/elasticsearch/pull/125345))
* ESQL: Fail in `AggregateFunction` when `LogicPlan` is not an `Aggregate` [#124446](https://github.com/elastic/elasticsearch/pull/124446) (issue: [#124446](https://github.com/elastic/elasticsearch/pull/124446))
* ESQL: Fix inconsistent results in using scaled_float field [#122586](https://github.com/elastic/elasticsearch/pull/122586) (issue: [#122586](https://github.com/elastic/elasticsearch/pull/122586))
* Esql - Fix lucene push down behavior when a range contains nanos and millis [#125595](https://github.com/elastic/elasticsearch/pull/125595)
* Fix functions emitting warnings with no source [#122821](https://github.com/elastic/elasticsearch/pull/122821) (issue: [#122821](https://github.com/elastic/elasticsearch/pull/122821))
* Improve error message for ( and [ [#124177](https://github.com/elastic/elasticsearch/pull/124177) (issue: [#124177](https://github.com/elastic/elasticsearch/pull/124177))
* Make `numberOfChannels` consistent with layout map by removing duplicated `ChannelSet` [#125636](https://github.com/elastic/elasticsearch/pull/125636)
* Remove duplicated nested commands [#123085](https://github.com/elastic/elasticsearch/pull/123085)
* TO_LOWER processes all values [#124676](https://github.com/elastic/elasticsearch/pull/124676) (issue: [#124676](https://github.com/elastic/elasticsearch/pull/124676))

Indices APIs:
* Fix NPE in rolling over unknown target and return 404 [#125352](https://github.com/elastic/elasticsearch/pull/125352)

Infra/Core:
* Reduce Data Loss in System Indices Migration [#121327](https://github.com/elastic/elasticsearch/pull/121327)

Ingest Node:
* apm-data: Use representative count as event.success_count if available [#119995](https://github.com/elastic/elasticsearch/pull/119995)

Machine Learning:
* Fix `AlibabaCloudSearchCompletionAction` not accepting `ChatCompletionInputs` [#125023](https://github.com/elastic/elasticsearch/pull/125023)
* Provide model size statistics as soon as an anomaly detection job is opened [#124638](https://github.com/elastic/elasticsearch/pull/124638) (issue: [#124638](https://github.com/elastic/elasticsearch/pull/124638))
* Return a Conflict status code if the model deployment is stopped by a user [#125204](https://github.com/elastic/elasticsearch/pull/125204) (issue: [#125204](https://github.com/elastic/elasticsearch/pull/125204))
* Set default similarity for Cohere model to cosine [#125370](https://github.com/elastic/elasticsearch/pull/125370) (issue: [#125370](https://github.com/elastic/elasticsearch/pull/125370))
* Updates to allow using Cohere binary embedding response in semantic search queries [#121827](https://github.com/elastic/elasticsearch/pull/121827)

Mapping:
* Fix Semantic Text 8.x Upgrade Bug [#125446](https://github.com/elastic/elasticsearch/pull/125446)

Ranking:
* Restore `TextSimilarityRankBuilder` XContent output [#124564](https://github.com/elastic/elasticsearch/pull/124564)

Relevance:
* Prevent Query Rule Creation with Invalid Numeric Match Criteria [#122823](https://github.com/elastic/elasticsearch/pull/122823)

Search:
* Fix handling of auto expand replicas for stateless indices [#122365](https://github.com/elastic/elasticsearch/pull/122365)
* Handle long overflow in dates [#124048](https://github.com/elastic/elasticsearch/pull/124048) (issue: [#124048](https://github.com/elastic/elasticsearch/pull/124048))
* Load `FieldInfos` from store if not yet initialised through a refresh on `IndexShard` [#125650](https://github.com/elastic/elasticsearch/pull/125650) (issue: [#125650](https://github.com/elastic/elasticsearch/pull/125650))
* Minor-Fixes Support 7x segments as archive in 8x / 9x [#125666](https://github.com/elastic/elasticsearch/pull/125666)
* Support indices created in ESv6 and updated in ESV7 using different LuceneCodecs as archive in current version. [#119503](https://github.com/elastic/elasticsearch/pull/119503) (issue: [#119503](https://github.com/elastic/elasticsearch/pull/119503))

Snapshot/Restore:
* Limit number of suppressed S3 deletion errors [#123630](https://github.com/elastic/elasticsearch/pull/123630) (issue: [#123630](https://github.com/elastic/elasticsearch/pull/123630))

Suggesters:
* Support duplicate suggestions in completion field [#121324](https://github.com/elastic/elasticsearch/pull/121324) (issue: [#121324](https://github.com/elastic/elasticsearch/pull/121324))

Vector Search:
* Return appropriate error on null dims update instead of npe [#125716](https://github.com/elastic/elasticsearch/pull/125716)


