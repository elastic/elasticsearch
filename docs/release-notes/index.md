---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-release-notes.html
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

## 9.0.0 [elasticsearch-900-release-notes]

### Highlights [elasticsearch-900-highlights]

::::{dropdown} Add new experimental `rank_vectors` mapping for late-interaction second order ranking
Late-interaction models are powerful rerankers. While their size and overall cost doesn't lend itself for HNSW indexing, utilizing them as second order reranking can provide excellent boosts in relevance. The new `rank_vectors` mapping allows for rescoring over new and novel multi-vector late-interaction models like ColBERT or ColPali.

For more information, check [PR #118804](https://github.com/elastic/elasticsearch/pull/118804).
::::

::::{dropdown} Enable LOOKUP JOIN in non-snapshot builds
This effectively releases LOOKUP JOIN into tech preview. Docs will
follow in a separate PR.

- Enable the lexing/grammar for LOOKUP JOIN in non-snapshot builds.
- Remove the grammar for the unsupported `| JOIN ...` command (without `LOOKUP` as first keyword). The way the lexer modes work, otherwise we'd also have to enable `| JOIN ...` syntax on non-snapshot builds and would have to add additional validation to provide appropriate error messages.
- Remove grammar for `LOOKUP JOIN index AS ...` because qualifiers are not yet supported. Otherwise we'd have to put in additional validation as well to prevent such queries.

Also fix https://github.com/elastic/elasticsearch/issues/121185

For more information, check [PR #121193](https://github.com/elastic/elasticsearch/pull/121193).
::::

::::{dropdown} Release semantic_text as a GA feature
semantic_text is now an official GA (generally available) feature! This field type allows you to easily set up and perform semantic search with minimal ramp up time.

For more information, check [PR #124669](https://github.com/elastic/elasticsearch/pull/124669).
::::

### Features and enhancements [elasticsearch-900-features-enhancements]

Allocation:
* Add `FailedShardEntry` info to shard-failed task source string [#125520](https://github.com/elastic/elasticsearch/pull/125520) (issue: [#125520](https://github.com/elastic/elasticsearch/pull/125520))
* Add a not-master state for desired balance [#116904](https://github.com/elastic/elasticsearch/pull/116904)
* Add cache support in `TransportGetAllocationStatsAction` [#124898](https://github.com/elastic/elasticsearch/pull/124898) (issue: [#124898](https://github.com/elastic/elasticsearch/pull/124898))
* Introduce `AllocationBalancingRoundSummaryService` [#120957](https://github.com/elastic/elasticsearch/pull/120957)
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
* Enhance memory accounting for document expansion and introduce max document size limit [#123543](https://github.com/elastic/elasticsearch/pull/123543)
* Metrics for indexing failures due to version conflicts [#119067](https://github.com/elastic/elasticsearch/pull/119067)
* Remove INDEX_REFRESH_BLOCK after index becomes searchable [#120807](https://github.com/elastic/elasticsearch/pull/120807)
* Suppress merge-on-recovery for older indices [#113462](https://github.com/elastic/elasticsearch/pull/113462)

Cluster Coordination:
* Include `clusterApplyListener` in long cluster apply warnings [#120087](https://github.com/elastic/elasticsearch/pull/120087)

Data streams:
* Add action to create index from a source index [#118890](https://github.com/elastic/elasticsearch/pull/118890)
* Add index and reindex request settings to speed up reindex [#119780](https://github.com/elastic/elasticsearch/pull/119780)
* Add index mode to get data stream API [#122486](https://github.com/elastic/elasticsearch/pull/122486)
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
* Run `TransportGetDataStreamLifecycleAction` on local node [#125214](https://github.com/elastic/elasticsearch/pull/125214)
* Run `TransportGetDataStreamOptionsAction` on local node [#125213](https://github.com/elastic/elasticsearch/pull/125213)
* Run `TransportGetDataStreamsAction` on local node [#122852](https://github.com/elastic/elasticsearch/pull/122852)
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
* Add a `LicenseAware` interface for licensed Nodes [#118931](https://github.com/elastic/elasticsearch/pull/118931) (issue: [#118931](https://github.com/elastic/elasticsearch/pull/118931))
* Add a `PostAnalysisAware,` distribute verification [#119798](https://github.com/elastic/elasticsearch/pull/119798)
* Add a standard deviation aggregating function: STD_DEV [#116531](https://github.com/elastic/elasticsearch/pull/116531)
* Add cluster level reduction [#117731](https://github.com/elastic/elasticsearch/pull/117731)
* Add initial grammar and changes for FORK [#121948](https://github.com/elastic/elasticsearch/pull/121948)
* Add initial grammar and planning for RRF (snapshot) [#123396](https://github.com/elastic/elasticsearch/pull/123396)
* Add nulls support to Categorize [#117655](https://github.com/elastic/elasticsearch/pull/117655)
* Allow partial results in ES|QL [#121942](https://github.com/elastic/elasticsearch/pull/121942)
* Allow skip shards with `_tier` and `_index` in ES|QL [#123728](https://github.com/elastic/elasticsearch/pull/123728)
* Async search responses have CCS metadata while searches are running [#117265](https://github.com/elastic/elasticsearch/pull/117265)
* Avoid `NamedWritable` in block serialization [#124394](https://github.com/elastic/elasticsearch/pull/124394)
* Calculate concurrent node limit [#124901](https://github.com/elastic/elasticsearch/pull/124901)
* Check for early termination in Driver [#118188](https://github.com/elastic/elasticsearch/pull/118188)
* Do not serialize `EsIndex` in plan [#119580](https://github.com/elastic/elasticsearch/pull/119580)
* Double parameter markers for identifiers [#122459](https://github.com/elastic/elasticsearch/pull/122459)
* ESQL - Add Match function options [#120360](https://github.com/elastic/elasticsearch/pull/120360)
* ESQL - Allow full text functions disjunctions for non-full text functions [#120291](https://github.com/elastic/elasticsearch/pull/120291)
* ESQL - Remove restrictions for disjunctions in full text functions [#118544](https://github.com/elastic/elasticsearch/pull/118544)
* ESQL - enabling scoring with METADATA `_score` [#113120](https://github.com/elastic/elasticsearch/pull/113120)
* ESQL Add esql hash function [#117989](https://github.com/elastic/elasticsearch/pull/117989)
* ESQL Support IN operator for Date nanos [#119772](https://github.com/elastic/elasticsearch/pull/119772) (issue: [#119772](https://github.com/elastic/elasticsearch/pull/119772))
* ESQL: Align `RENAME` behavior with `EVAL` for sequential processing [#122250](https://github.com/elastic/elasticsearch/pull/122250) (issue: [#122250](https://github.com/elastic/elasticsearch/pull/122250))
* ESQL: CATEGORIZE as a `BlockHash` [#114317](https://github.com/elastic/elasticsearch/pull/114317)
* ESQL: Enable async get to support formatting [#111104](https://github.com/elastic/elasticsearch/pull/111104) (issue: [#111104](https://github.com/elastic/elasticsearch/pull/111104))
* ESQL: Enterprise license enforcement for CCS [#118102](https://github.com/elastic/elasticsearch/pull/118102)
* ES|QL - Add scoring for full text functions disjunctions [#121793](https://github.com/elastic/elasticsearch/pull/121793)
* ES|QL slow log [#124094](https://github.com/elastic/elasticsearch/pull/124094)
* ES|QL: Partial result on demand for async queries [#118122](https://github.com/elastic/elasticsearch/pull/118122)
* ES|QL: Support `::date` in inline cast [#123460](https://github.com/elastic/elasticsearch/pull/123460) (issue: [#123460](https://github.com/elastic/elasticsearch/pull/123460))
* Enable KQL function as a tech preview [#119730](https://github.com/elastic/elasticsearch/pull/119730)
* Enable LOOKUP JOIN in non-snapshot builds [#121193](https://github.com/elastic/elasticsearch/pull/121193) (issue: [#121193](https://github.com/elastic/elasticsearch/pull/121193))
* Enable node-level reduction by default [#119621](https://github.com/elastic/elasticsearch/pull/119621)
* Enable physical plan verification [#118114](https://github.com/elastic/elasticsearch/pull/118114)
* Ensure cluster string could be quoted [#120355](https://github.com/elastic/elasticsearch/pull/120355)
* Esql - Support date nanos in date extract function [#120727](https://github.com/elastic/elasticsearch/pull/120727) (issue: [#120727](https://github.com/elastic/elasticsearch/pull/120727))
* Esql - support date nanos in date format function [#120143](https://github.com/elastic/elasticsearch/pull/120143) (issue: [#120143](https://github.com/elastic/elasticsearch/pull/120143))
* Esql Support date nanos on date diff function [#120645](https://github.com/elastic/elasticsearch/pull/120645) (issue: [#120645](https://github.com/elastic/elasticsearch/pull/120645))
* Esql bucket function for date nanos [#118474](https://github.com/elastic/elasticsearch/pull/118474) (issue: [#118474](https://github.com/elastic/elasticsearch/pull/118474))
* Esql compare nanos and millis [#118027](https://github.com/elastic/elasticsearch/pull/118027) (issue: [#118027](https://github.com/elastic/elasticsearch/pull/118027))
* Esql implicit casting for date nanos [#118697](https://github.com/elastic/elasticsearch/pull/118697) (issue: [#118697](https://github.com/elastic/elasticsearch/pull/118697))
* Expand type compatibility for match function and operator [#117555](https://github.com/elastic/elasticsearch/pull/117555)
* Extend `TranslationAware` to all pushable expressions [#120192](https://github.com/elastic/elasticsearch/pull/120192)
* Fix Driver status iterations and `cpuTime` [#123290](https://github.com/elastic/elasticsearch/pull/123290) (issue: [#123290](https://github.com/elastic/elasticsearch/pull/123290))
* Fix sorting when `aggregate_metric_double` present [#125191](https://github.com/elastic/elasticsearch/pull/125191)
* Hash functions [#118938](https://github.com/elastic/elasticsearch/pull/118938)
* Implement a `MetricsAware` interface [#121074](https://github.com/elastic/elasticsearch/pull/121074)
* Implement runtime skip_unavailable=true [#121240](https://github.com/elastic/elasticsearch/pull/121240)
* Include failures in partial response [#124929](https://github.com/elastic/elasticsearch/pull/124929)
* Infer the score mode to use from the Lucene collector [#125930](https://github.com/elastic/elasticsearch/pull/125930)
* Initial support for unmapped fields [#119886](https://github.com/elastic/elasticsearch/pull/119886)
* Introduce `allow_partial_results` setting in ES|QL [#122890](https://github.com/elastic/elasticsearch/pull/122890)
* Introduce a pre-mapping logical plan processing step [#121260](https://github.com/elastic/elasticsearch/pull/121260)
* Keep ordinals in conversion functions [#125357](https://github.com/elastic/elasticsearch/pull/125357)
* LOOKUP JOIN using field-caps for field mapping [#117246](https://github.com/elastic/elasticsearch/pull/117246)
* Lookup join on multiple join fields not yet supported [#118858](https://github.com/elastic/elasticsearch/pull/118858)
* Move scoring in ES|QL out of snapshot [#120354](https://github.com/elastic/elasticsearch/pull/120354)
* Optimize ST_EXTENT_AGG for `geo_shape` and `cartesian_shape` [#119889](https://github.com/elastic/elasticsearch/pull/119889)
* Pragma to load from stored fields [#122891](https://github.com/elastic/elasticsearch/pull/122891)
* Push down `StartsWith` and `EndsWith` functions to Lucene [#123381](https://github.com/elastic/elasticsearch/pull/123381) (issue: [#123381](https://github.com/elastic/elasticsearch/pull/123381))
* Push down filter passed lookup join [#118410](https://github.com/elastic/elasticsearch/pull/118410)
* Remove page alignment in exchange sink [#124610](https://github.com/elastic/elasticsearch/pull/124610)
* Render `aggregate_metric_double` [#122660](https://github.com/elastic/elasticsearch/pull/122660)
* Report `original_types` [#124913](https://github.com/elastic/elasticsearch/pull/124913)
* Report failures on partial results [#124823](https://github.com/elastic/elasticsearch/pull/124823)
* Resume Driver on cancelled or early finished [#120020](https://github.com/elastic/elasticsearch/pull/120020)
* Retry ES|QL node requests on shard level failures [#120774](https://github.com/elastic/elasticsearch/pull/120774)
* Reuse child `outputSet` inside the plan where possible [#124611](https://github.com/elastic/elasticsearch/pull/124611)
* Rewrite TO_UPPER/TO_LOWER comparisons [#118870](https://github.com/elastic/elasticsearch/pull/118870) (issue: [#118870](https://github.com/elastic/elasticsearch/pull/118870))
* ST_EXTENT aggregation [#117451](https://github.com/elastic/elasticsearch/pull/117451) (issue: [#117451](https://github.com/elastic/elasticsearch/pull/117451))
* ST_EXTENT_AGG optimize envelope extraction from doc-values for cartesian_shape [#118802](https://github.com/elastic/elasticsearch/pull/118802)
* Smarter field caps with subscribable listener [#116755](https://github.com/elastic/elasticsearch/pull/116755)
* Support ST_ENVELOPE and related (ST_XMIN, ST_XMAX, ST_YMIN, ST_YMAX) functions [#116964](https://github.com/elastic/elasticsearch/pull/116964) (issue: [#116964](https://github.com/elastic/elasticsearch/pull/116964))
* Support partial results in CCS in ES|QL [#122708](https://github.com/elastic/elasticsearch/pull/122708)
* Support partial sort fields in TopN pushdown [#116043](https://github.com/elastic/elasticsearch/pull/116043) (issue: [#116043](https://github.com/elastic/elasticsearch/pull/116043))
* Support some stats on aggregate_metric_double [#120343](https://github.com/elastic/elasticsearch/pull/120343) (issue: [#120343](https://github.com/elastic/elasticsearch/pull/120343))
* Support subset of metrics in aggregate metric double [#121805](https://github.com/elastic/elasticsearch/pull/121805)
* Take double parameter markers for identifiers out of snapshot [#125690](https://github.com/elastic/elasticsearch/pull/125690)
* Take named parameters for identifier and pattern out of snapshot [#121850](https://github.com/elastic/elasticsearch/pull/121850)
* Term query for ES|QL [#117359](https://github.com/elastic/elasticsearch/pull/117359)
* Update grammar to rely on `indexPattern` instead of identifier in join target [#120494](https://github.com/elastic/elasticsearch/pull/120494)
* `ToAggregateMetricDouble` function [#124595](https://github.com/elastic/elasticsearch/pull/124595)
* `_score` should not be a reserved attribute in ES|QL [#118435](https://github.com/elastic/elasticsearch/pull/118435) (issue: [#118435](https://github.com/elastic/elasticsearch/pull/118435))

Engine:
* Defer unpromotable shard refreshes until index refresh blocks are cleared [#120642](https://github.com/elastic/elasticsearch/pull/120642)
* POC mark read-only [#119743](https://github.com/elastic/elasticsearch/pull/119743)
* Threadpool merge scheduler [#120869](https://github.com/elastic/elasticsearch/pull/120869)

Experiences:
* Integrate IBM watsonx to Inference API for re-ranking task [#117176](https://github.com/elastic/elasticsearch/pull/117176)

Extract&Transform:
* [Connector API] Support hard deletes with new URL param in delete endpoint [#120200](https://github.com/elastic/elasticsearch/pull/120200)
* [Connector API] Support soft-deletes of connectors [#118669](https://github.com/elastic/elasticsearch/pull/118669)
* [Connector APIs] Enforce index prefix for managed connectors [#117778](https://github.com/elastic/elasticsearch/pull/117778)

Geo:
* Optimize indexing points with index and doc values set to true [#120271](https://github.com/elastic/elasticsearch/pull/120271)

Health:
* Add health indicator impact to `HealthPeriodicLogger` [#122390](https://github.com/elastic/elasticsearch/pull/122390)
* Increase `replica_unassigned_buffer_time` default from 3s to 5s [#112834](https://github.com/elastic/elasticsearch/pull/112834)

Highlighting:
* Add Highlighter for Semantic Text Fields [#118064](https://github.com/elastic/elasticsearch/pull/118064)

ILM+SLM:
* Add a `replicate_for` option to the ILM `searchable_snapshot` action [#119003](https://github.com/elastic/elasticsearch/pull/119003)
* Improve SLM Health Indicator to cover missing snapshot [#121370](https://github.com/elastic/elasticsearch/pull/121370)
* Optimize usage calculation in ILM policies retrieval API [#106953](https://github.com/elastic/elasticsearch/pull/106953) (issue: [#106953](https://github.com/elastic/elasticsearch/pull/106953))
* Process ILM cluster state updates on another thread [#123712](https://github.com/elastic/elasticsearch/pull/123712)
* Run `TransportExplainLifecycleAction` on local node [#122885](https://github.com/elastic/elasticsearch/pull/122885)
* Truncate `step_info` and error reason in ILM execution state and history [#125054](https://github.com/elastic/elasticsearch/pull/125054) (issue: [#125054](https://github.com/elastic/elasticsearch/pull/125054))

Indices APIs:
* Add `remove_index_block` arg to `_create_from` api [#120548](https://github.com/elastic/elasticsearch/pull/120548)
* Avoid creating known_fields for every check in Alias [#124690](https://github.com/elastic/elasticsearch/pull/124690)
* Remove index blocks by default in `create_from` [#120643](https://github.com/elastic/elasticsearch/pull/120643)
* Run `TransportGetComponentTemplateAction` on local node [#116868](https://github.com/elastic/elasticsearch/pull/116868)
* Run `TransportGetComposableIndexTemplate` on local node [#119830](https://github.com/elastic/elasticsearch/pull/119830)
* Run `TransportGetIndexTemplateAction` on local node [#119837](https://github.com/elastic/elasticsearch/pull/119837)
* Run `TransportGetMappingsAction` on local node [#122921](https://github.com/elastic/elasticsearch/pull/122921)
* introduce new categories for deprecated resources in deprecation API [#120505](https://github.com/elastic/elasticsearch/pull/120505)

Inference:
* Add version prefix to Inference Service API path [#117095](https://github.com/elastic/elasticsearch/pull/117095)
* Remove Elastic Inference Service feature flag and deprecated setting [#120842](https://github.com/elastic/elasticsearch/pull/120842)
* Update sparse text embeddings API route for Inference Service [#118025](https://github.com/elastic/elasticsearch/pull/118025)
* [Elastic Inference Service] Add ElasticInferenceService Unified ChatCompletions Integration [#118871](https://github.com/elastic/elasticsearch/pull/118871)
* [Inference API] Rename `model_id` prop to model in EIS sparse inference request body [#122272](https://github.com/elastic/elasticsearch/pull/122272)

Infra/CLI:
* Ignore _JAVA_OPTIONS [#124843](https://github.com/elastic/elasticsearch/pull/124843)
* Strengthen encryption for elasticsearch-keystore tool to AES 256 [#119749](https://github.com/elastic/elasticsearch/pull/119749)

Infra/Circuit Breakers:
* Add link to Circuit Breaker "Data too large" exception message [#113561](https://github.com/elastic/elasticsearch/pull/113561)

Infra/Core:
* Add support for specifying reindexing script for system index migration [#119001](https://github.com/elastic/elasticsearch/pull/119001)
* Bump major version for feature migration system indices [#117243](https://github.com/elastic/elasticsearch/pull/117243)
* Change default Docker image to be based on UBI minimal instead of Ubuntu [#116739](https://github.com/elastic/elasticsearch/pull/116739)
* Give Kibana user 'all' permissions for .entity_analytics.* indices [#123588](https://github.com/elastic/elasticsearch/pull/123588)
* Improve size limiting string message [#122427](https://github.com/elastic/elasticsearch/pull/122427)
* Infrastructure for assuming cluster features in the next major version [#118143](https://github.com/elastic/elasticsearch/pull/118143)
* Permanently switch from Java SecurityManager to Entitlements. The Java SecurityManager has been deprecated since Java 17, and it is now completely disabled in Java 24. In order to retain an similar level of protection, Elasticsearch implemented its own protection mechanism, Entitlements. Starting with this version, Entitlements will permanently replace the Java SecurityManager. [#124865](https://github.com/elastic/elasticsearch/pull/124865)
* Permanently switch from Java SecurityManager to Entitlements. The Java SecurityManager has been deprecated since Java 17, and it is now completely disabled in Java 24. In order to retain an similar level of protection, Elasticsearch implemented its own protection mechanism, Entitlements. Starting with this version, Entitlements will permanently replace the Java SecurityManager. [#125117](https://github.com/elastic/elasticsearch/pull/125117)
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
* Allow passing several reserved state chunks in single process call [#124574](https://github.com/elastic/elasticsearch/pull/124574)
* Introduce `IndexSettingDeprecatedInV8AndRemovedInV9` Setting property [#120334](https://github.com/elastic/elasticsearch/pull/120334)
* Run `TransportClusterGetSettingsAction` on local node [#119831](https://github.com/elastic/elasticsearch/pull/119831)

Ingest Node:
* Allow setting the `type` in the reroute processor [#122409](https://github.com/elastic/elasticsearch/pull/122409) (issue: [#122409](https://github.com/elastic/elasticsearch/pull/122409))
* Optimize `IngestCtxMap` construction [#120833](https://github.com/elastic/elasticsearch/pull/120833)
* Optimize `IngestDocMetadata` `isAvailable` [#120753](https://github.com/elastic/elasticsearch/pull/120753)
* Optimize `IngestDocument` `FieldPath` allocation [#120573](https://github.com/elastic/elasticsearch/pull/120573)
* Optimize some per-document hot paths in the geoip processor [#120824](https://github.com/elastic/elasticsearch/pull/120824)
* Returning ignored fields in the simulate ingest API [#117214](https://github.com/elastic/elasticsearch/pull/117214)
* Run `GetPipelineTransportAction` on local node [#120445](https://github.com/elastic/elasticsearch/pull/120445)
* Run `TransportEnrichStatsAction` on local node [#121256](https://github.com/elastic/elasticsearch/pull/121256)
* Run `TransportGetEnrichPolicyAction` on local node [#121124](https://github.com/elastic/elasticsearch/pull/121124)
* Run template simulation actions on local node [#120038](https://github.com/elastic/elasticsearch/pull/120038)

License:
* Bump `TrialLicenseVersion` to allow starting new trial on 9.0 [#120198](https://github.com/elastic/elasticsearch/pull/120198)

Logs:
* Add LogsDB option to route on sort fields [#116687](https://github.com/elastic/elasticsearch/pull/116687)
* Add a new index setting to skip recovery source when synthetic source is enabled [#114618](https://github.com/elastic/elasticsearch/pull/114618)
* Configure index sorting through index settings for logsdb [#118968](https://github.com/elastic/elasticsearch/pull/118968) (issue: [#118968](https://github.com/elastic/elasticsearch/pull/118968))
* Optimize loading mappings when determining synthetic source usage and whether host.name can be sorted on. [#120055](https://github.com/elastic/elasticsearch/pull/120055)

Machine Learning:
* Add DeBERTa-V2/V3 tokenizer [#111852](https://github.com/elastic/elasticsearch/pull/111852)
* Add Inference Unified API for chat completions for OpenAI [#117589](https://github.com/elastic/elasticsearch/pull/117589)
* Add Jina AI API to do inference for Embedding and Rerank models [#118652](https://github.com/elastic/elasticsearch/pull/118652)
* Add `ModelRegistryMetadata` to Cluster State [#121106](https://github.com/elastic/elasticsearch/pull/121106)
* Add enterprise license check for Inference API actions [#119893](https://github.com/elastic/elasticsearch/pull/119893)
* Adding chunking settings to `IbmWatsonxService` [#114914](https://github.com/elastic/elasticsearch/pull/114914)
* Adding common rerank options to Perform Inference API [#125239](https://github.com/elastic/elasticsearch/pull/125239) (issue: [#125239](https://github.com/elastic/elasticsearch/pull/125239))
* Adding default endpoint for Elastic Rerank [#117939](https://github.com/elastic/elasticsearch/pull/117939)
* Adding elser default endpoint for EIS [#122066](https://github.com/elastic/elasticsearch/pull/122066)
* Adding endpoint creation validation for all task types to remaining services [#115020](https://github.com/elastic/elasticsearch/pull/115020)
* Adding endpoint creation validation to `ElasticInferenceService` [#117642](https://github.com/elastic/elasticsearch/pull/117642)
* Adding integration for VoyageAI embeddings and rerank models [#122134](https://github.com/elastic/elasticsearch/pull/122134)
* Adding support for binary embedding type to Cohere service embedding type [#120751](https://github.com/elastic/elasticsearch/pull/120751)
* Adding support for specifying embedding type to Jina AI service settings [#121548](https://github.com/elastic/elasticsearch/pull/121548)
* Automatically rollover legacy .ml-anomalies indices [#120913](https://github.com/elastic/elasticsearch/pull/120913)
* Automatically rollover legacy ml indices [#120405](https://github.com/elastic/elasticsearch/pull/120405)
* Change the auditor to write via an alias [#120064](https://github.com/elastic/elasticsearch/pull/120064)
* Check for presence of error object when validating streaming responses from integrations in the inference API [#118375](https://github.com/elastic/elasticsearch/pull/118375)
* Check if the anomaly results index has been rolled over [#125404](https://github.com/elastic/elasticsearch/pull/125404)
* ES|QL `change_point` processing command [#120998](https://github.com/elastic/elasticsearch/pull/120998)
* ES|QL categorize with multiple groupings [#118173](https://github.com/elastic/elasticsearch/pull/118173)
* Expose `input_type` option at root level for `text_embedding` task type in Perform Inference API [#122638](https://github.com/elastic/elasticsearch/pull/122638) (issue: [#122638](https://github.com/elastic/elasticsearch/pull/122638))
* Ignore failures from renormalizing buckets in read-only index [#118674](https://github.com/elastic/elasticsearch/pull/118674)
* Inference duration and error metrics [#115876](https://github.com/elastic/elasticsearch/pull/115876)
* Integrate with `DeepSeek` API [#122218](https://github.com/elastic/elasticsearch/pull/122218)
* Limit the number of chunks for semantic text to prevent high memory usage [#123150](https://github.com/elastic/elasticsearch/pull/123150)
* Migrate stream to core error parsing [#120722](https://github.com/elastic/elasticsearch/pull/120722)
* Remove all mentions of eis and gateway and deprecate flags that do [#116692](https://github.com/elastic/elasticsearch/pull/116692)
* Remove deprecated sort from reindex operation within dataframe analytics procedure [#117606](https://github.com/elastic/elasticsearch/pull/117606)
* Retry on `ClusterBlockException` on transform destination index [#118194](https://github.com/elastic/elasticsearch/pull/118194)
* Support mTLS for the Elastic Inference Service integration inside the inference API [#119679](https://github.com/elastic/elasticsearch/pull/119679)
* Upgrade AWS v2 SDK to 2.30.38 [#124738](https://github.com/elastic/elasticsearch/pull/124738)
* [Inference API] Add node-local rate limiting for the inference API [#120400](https://github.com/elastic/elasticsearch/pull/120400)
* [Inference API] Propagate product use case http header to EIS [#124025](https://github.com/elastic/elasticsearch/pull/124025)
* [Inference API] fix spell words: covertToString to convertToString [#119922](https://github.com/elastic/elasticsearch/pull/119922)

Mapping:
* Add Optional Source Filtering to Source Loaders [#113827](https://github.com/elastic/elasticsearch/pull/113827)
* Add option to store `sparse_vector` outside `_source` [#117917](https://github.com/elastic/elasticsearch/pull/117917)
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

Network:
* Allow http unsafe buffers by default [#116115](https://github.com/elastic/elasticsearch/pull/116115)
* Http stream activity tracker and exceptions handling [#119564](https://github.com/elastic/elasticsearch/pull/119564)
* Remove HTTP content copies [#117303](https://github.com/elastic/elasticsearch/pull/117303)
* `ConnectTransportException` returns retryable BAD_GATEWAY [#118681](https://github.com/elastic/elasticsearch/pull/118681) (issue: [#118681](https://github.com/elastic/elasticsearch/pull/118681))

Packaging:
* Update bundled JDK to Java 24 [#125159](https://github.com/elastic/elasticsearch/pull/125159)

Ranking:
* Add a generic `rescorer` retriever based on the search request's rescore functionality [#118585](https://github.com/elastic/elasticsearch/pull/118585) (issue: [#118585](https://github.com/elastic/elasticsearch/pull/118585))
* Leverage scorer supplier in `QueryFeatureExtractor` [#125259](https://github.com/elastic/elasticsearch/pull/125259)
* Set default reranker for text similarity reranker to Elastic reranker [#120551](https://github.com/elastic/elasticsearch/pull/120551)

Recovery:
* Allow archive and searchable snapshots indices in N-2 version [#118941](https://github.com/elastic/elasticsearch/pull/118941)
* Trigger merges after recovery [#113102](https://github.com/elastic/elasticsearch/pull/113102)

Reindex:
* Change Reindexing metrics unit from millis to seconds [#115721](https://github.com/elastic/elasticsearch/pull/115721)

Relevance:
* Add Multi-Field Support for Semantic Text Fields [#120128](https://github.com/elastic/elasticsearch/pull/120128)
* Skip semantic_text embedding generation when no content is provided. [#123763](https://github.com/elastic/elasticsearch/pull/123763)

Search:
* Account for the `SearchHit` source in circuit breaker [#121920](https://github.com/elastic/elasticsearch/pull/121920) (issue: [#121920](https://github.com/elastic/elasticsearch/pull/121920))
* Add match support for `semantic_text` fields [#117839](https://github.com/elastic/elasticsearch/pull/117839)
* Add support for `sparse_vector` queries against `semantic_text` fields [#118617](https://github.com/elastic/elasticsearch/pull/118617)
* Add support for knn vector queries on `semantic_text` fields [#119011](https://github.com/elastic/elasticsearch/pull/119011)
* Added optional parameters to QSTR ES|QL function [#121787](https://github.com/elastic/elasticsearch/pull/121787) (issue: [#121787](https://github.com/elastic/elasticsearch/pull/121787))
* Adding linear retriever to support weighted sums of sub-retrievers [#120222](https://github.com/elastic/elasticsearch/pull/120222)
* Address and remove any references of RestApiVersion version 7 [#117572](https://github.com/elastic/elasticsearch/pull/117572)
* Feat: add a user-configurable timeout parameter to the `_resolve/cluster` API [#120542](https://github.com/elastic/elasticsearch/pull/120542)
* Introduce batched query execution and data-node side reduce [#121885](https://github.com/elastic/elasticsearch/pull/121885)
* Make semantic text part of the text family [#119792](https://github.com/elastic/elasticsearch/pull/119792)
* Only aggregations require at least one shard request [#115314](https://github.com/elastic/elasticsearch/pull/115314)
* Optimize memory usage in `ShardBulkInferenceActionFilter` [#124313](https://github.com/elastic/elasticsearch/pull/124313)
* Optionally allow text similarity reranking to fail [#121784](https://github.com/elastic/elasticsearch/pull/121784)
* Prevent data nodes from sending stack traces to coordinator when `error_trace=false` [#118266](https://github.com/elastic/elasticsearch/pull/118266)
* Propagate status codes from shard failures appropriately [#118016](https://github.com/elastic/elasticsearch/pull/118016) (issue: [#118016](https://github.com/elastic/elasticsearch/pull/118016))
* Upgrade to Lucene 10 [#114741](https://github.com/elastic/elasticsearch/pull/114741)
* Upgrade to Lucene 10.1.0 [#119308](https://github.com/elastic/elasticsearch/pull/119308)

Security:
* Add refresh `.security` index call between security migrations [#114879](https://github.com/elastic/elasticsearch/pull/114879)

Snapshot/Restore:
* Add IMDSv2 support to `repository-s3` [#117748](https://github.com/elastic/elasticsearch/pull/117748) (issue: [#117748](https://github.com/elastic/elasticsearch/pull/117748))
* Expose operation and request counts separately in repository stats [#117530](https://github.com/elastic/elasticsearch/pull/117530) (issue: [#117530](https://github.com/elastic/elasticsearch/pull/117530))
* GCS blob store: add `OperationPurpose/Operation` stats counters [#122991](https://github.com/elastic/elasticsearch/pull/122991)
* Retry `S3BlobContainer#getRegister` on all exceptions [#114813](https://github.com/elastic/elasticsearch/pull/114813)
* Retry internally when CAS upload is throttled [GCS] [#120250](https://github.com/elastic/elasticsearch/pull/120250) (issue: [#120250](https://github.com/elastic/elasticsearch/pull/120250))
* Retry when the server can't be resolved (Google Cloud Storage) [#123852](https://github.com/elastic/elasticsearch/pull/123852)
* Track shard snapshot progress during node shutdown [#112567](https://github.com/elastic/elasticsearch/pull/112567)
* Upgrade AWS SDK to v1.12.746 [#122431](https://github.com/elastic/elasticsearch/pull/122431)

Stats:
* Run XPack usage actions on local node [#122933](https://github.com/elastic/elasticsearch/pull/122933)

Store:
* Abort pending deletion on `IndicesService` close [#123569](https://github.com/elastic/elasticsearch/pull/123569)

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
* Add bit vector support to semantic text [#123187](https://github.com/elastic/elasticsearch/pull/123187)
* Add new experimental `rank_vectors` mapping for late-interaction second order ranking [#118804](https://github.com/elastic/elasticsearch/pull/118804)
* Add panama implementations of byte-bit and float-bit script operations [#124722](https://github.com/elastic/elasticsearch/pull/124722) (issue: [#124722](https://github.com/elastic/elasticsearch/pull/124722))
* Adds implementations of dotProduct and cosineSimilarity painless methods to operate on float vectors for byte fields [#122381](https://github.com/elastic/elasticsearch/pull/122381) (issue: [#122381](https://github.com/elastic/elasticsearch/pull/122381))
* Allow zero for `rescore_vector.oversample` to indicate by-passing oversample and rescoring [#125599](https://github.com/elastic/elasticsearch/pull/125599)
* Even better(er) binary quantization [#117994](https://github.com/elastic/elasticsearch/pull/117994)
* KNN vector rescoring for quantized vectors [#116663](https://github.com/elastic/elasticsearch/pull/116663)
* Mark bbq indices as GA and add rolling upgrade integration tests [#121105](https://github.com/elastic/elasticsearch/pull/121105)
* New `vector_rescore` parameter as a quantized index type option [#124581](https://github.com/elastic/elasticsearch/pull/124581)
* Speed up bit compared with floats or bytes script operations [#117199](https://github.com/elastic/elasticsearch/pull/117199)

Watcher:
* Run `TransportGetWatcherSettingsAction` on local node [#122857](https://github.com/elastic/elasticsearch/pull/122857)

### Fixes [elasticsearch-900-fixes]

Aggregations:
* Aggs: Let terms run in global ords mode no match [#124782](https://github.com/elastic/elasticsearch/pull/124782)
* Handle with `illegalArgumentExceptions` negative values in HDR percentile aggregations [#116174](https://github.com/elastic/elasticsearch/pull/116174) (issue: [#116174](https://github.com/elastic/elasticsearch/pull/116174))

Allocation:
* `DesiredBalanceReconciler` always returns `AllocationStats` [#122458](https://github.com/elastic/elasticsearch/pull/122458)

Analysis:
* Adjust exception thrown when unable to load hunspell dict [#123743](https://github.com/elastic/elasticsearch/pull/123743)
* Analyze API to return 400 for wrong custom analyzer [#121568](https://github.com/elastic/elasticsearch/pull/121568) (issue: [#121568](https://github.com/elastic/elasticsearch/pull/121568))
* Non existing synonyms sets do not fail shard recovery for indices [#125659](https://github.com/elastic/elasticsearch/pull/125659) (issue: [#125659](https://github.com/elastic/elasticsearch/pull/125659))

Authentication:
* Fix NPE for missing Content Type header in OIDC Authenticator [#126191](https://github.com/elastic/elasticsearch/pull/126191)

CAT APIs:
* Fix cat_component_templates documentation [#120487](https://github.com/elastic/elasticsearch/pull/120487)

CRUD:
* Preserve thread context when waiting for segment generation in RTG [#114623](https://github.com/elastic/elasticsearch/pull/114623)
* Preserve thread context when waiting for segment generation in RTG [#117148](https://github.com/elastic/elasticsearch/pull/117148)

Cluster Coordination:
* Disable logging in `ClusterFormationFailureHelper` on shutdown [#125244](https://github.com/elastic/elasticsearch/pull/125244) (issue: [#125244](https://github.com/elastic/elasticsearch/pull/125244))

Data streams:
* Avoid updating settings version in `MetadataMigrateToDataStreamService` when settings have not changed [#118704](https://github.com/elastic/elasticsearch/pull/118704)
* Block-writes cannot be added after read-only [#119007](https://github.com/elastic/elasticsearch/pull/119007) (issue: [#119007](https://github.com/elastic/elasticsearch/pull/119007))
* Ensure removal of index blocks does not leave key with null value [#122246](https://github.com/elastic/elasticsearch/pull/122246)
* Fixes a invalid warning from being issued when restoring a system data stream from a snapshot. [#125881](https://github.com/elastic/elasticsearch/pull/125881)
* Match dot prefix of migrated DS backing index with the source index [#120042](https://github.com/elastic/elasticsearch/pull/120042)
* Refresh source index before reindexing data stream index [#120752](https://github.com/elastic/elasticsearch/pull/120752) (issue: [#120752](https://github.com/elastic/elasticsearch/pull/120752))
* Updating `TransportRolloverAction.checkBlock` so that non-write-index blocks do not prevent data stream rollover [#122905](https://github.com/elastic/elasticsearch/pull/122905)
* `ReindexDataStreamIndex` bug in assertion caused by reference equality [#121325](https://github.com/elastic/elasticsearch/pull/121325)

Distributed:
* Pass `IndexReshardingMetadata` over the wire [#124841](https://github.com/elastic/elasticsearch/pull/124841)

Downsampling:
* Copy metrics and `default_metric` properties when downsampling `aggregate_metric_double` [#121727](https://github.com/elastic/elasticsearch/pull/121727) (issues: [#121727](https://github.com/elastic/elasticsearch/pull/121727), [#121727](https://github.com/elastic/elasticsearch/pull/121727))
* Improve downsample performance by avoiding to read unnecessary dimension values when downsampling. [#124451](https://github.com/elastic/elasticsearch/pull/124451)

EQL:
* Fix EQL double invoking listener [#124918](https://github.com/elastic/elasticsearch/pull/124918)

ES|QL:
* Add support to VALUES aggregation for spatial types [#122886](https://github.com/elastic/elasticsearch/pull/122886) (issue: [#122886](https://github.com/elastic/elasticsearch/pull/122886))
* Allow the data type of `null` in filters [#118324](https://github.com/elastic/elasticsearch/pull/118324) (issue: [#118324](https://github.com/elastic/elasticsearch/pull/118324))
* Avoid over collecting in Limit or Lucene Operator [#123296](https://github.com/elastic/elasticsearch/pull/123296)
* Change the order of the optimization rules [#124335](https://github.com/elastic/elasticsearch/pull/124335)
* Correct line and column numbers of missing named parameters [#120852](https://github.com/elastic/elasticsearch/pull/120852)
* Drop null columns in text formats [#117643](https://github.com/elastic/elasticsearch/pull/117643) (issue: [#117643](https://github.com/elastic/elasticsearch/pull/117643))
* ESQL - date nanos range bug? [#125345](https://github.com/elastic/elasticsearch/pull/125345) (issue: [#125345](https://github.com/elastic/elasticsearch/pull/125345))
* ESQL: Fail in `AggregateFunction` when `LogicPlan` is not an `Aggregate` [#124446](https://github.com/elastic/elasticsearch/pull/124446) (issue: [#124446](https://github.com/elastic/elasticsearch/pull/124446))
* ESQL: Fix inconsistent results in using scaled_float field [#122586](https://github.com/elastic/elasticsearch/pull/122586) (issue: [#122586](https://github.com/elastic/elasticsearch/pull/122586))
* ESQL: Remove estimated row size assertion [#122762](https://github.com/elastic/elasticsearch/pull/122762) (issue: [#122762](https://github.com/elastic/elasticsearch/pull/122762))
* ES|QL: Fix scoring for full text functions [#124540](https://github.com/elastic/elasticsearch/pull/124540)
* Esql - Fix lucene push down behavior when a range contains nanos and millis [#125595](https://github.com/elastic/elasticsearch/pull/125595)
* Fix ROUND() with unsigned longs throwing in some edge cases [#119536](https://github.com/elastic/elasticsearch/pull/119536)
* Fix TDigestState.read CB leaks [#114303](https://github.com/elastic/elasticsearch/pull/114303) (issue: [#114303](https://github.com/elastic/elasticsearch/pull/114303))
* Fix TopN row size estimate [#119476](https://github.com/elastic/elasticsearch/pull/119476) (issue: [#119476](https://github.com/elastic/elasticsearch/pull/119476))
* Fix `AbstractShapeGeometryFieldMapperTests` [#119265](https://github.com/elastic/elasticsearch/pull/119265) (issue: [#119265](https://github.com/elastic/elasticsearch/pull/119265))
* Fix `ReplaceMissingFieldsWithNull` [#125764](https://github.com/elastic/elasticsearch/pull/125764) (issues: [#125764](https://github.com/elastic/elasticsearch/pull/125764), [#125764](https://github.com/elastic/elasticsearch/pull/125764), [#125764](https://github.com/elastic/elasticsearch/pull/125764))
* Fix a bug in TOP [#121552](https://github.com/elastic/elasticsearch/pull/121552)
* Fix async stop sometimes not properly collecting result [#121843](https://github.com/elastic/elasticsearch/pull/121843) (issue: [#121843](https://github.com/elastic/elasticsearch/pull/121843))
* Fix attribute set equals [#118823](https://github.com/elastic/elasticsearch/pull/118823)
* Fix double lookup failure on ESQL [#115616](https://github.com/elastic/elasticsearch/pull/115616) (issue: [#115616](https://github.com/elastic/elasticsearch/pull/115616))
* Fix function registry concurrency issues on constructor [#123492](https://github.com/elastic/elasticsearch/pull/123492) (issue: [#123492](https://github.com/elastic/elasticsearch/pull/123492))
* Fix functions emitting warnings with no source [#122821](https://github.com/elastic/elasticsearch/pull/122821) (issue: [#122821](https://github.com/elastic/elasticsearch/pull/122821))
* Fix queries with document level security on lookup indexes [#120617](https://github.com/elastic/elasticsearch/pull/120617) (issue: [#120617](https://github.com/elastic/elasticsearch/pull/120617))
* Fix writing for LOOKUP status [#119296](https://github.com/elastic/elasticsearch/pull/119296) (issue: [#119296](https://github.com/elastic/elasticsearch/pull/119296))
* Implicit numeric casting for CASE/GREATEST/LEAST [#122601](https://github.com/elastic/elasticsearch/pull/122601) (issue: [#122601](https://github.com/elastic/elasticsearch/pull/122601))
* Improve error message for ( and [ [#124177](https://github.com/elastic/elasticsearch/pull/124177) (issue: [#124177](https://github.com/elastic/elasticsearch/pull/124177))
* Lazy collection copying during node transform [#124424](https://github.com/elastic/elasticsearch/pull/124424)
* Limit memory usage of `fold` [#118602](https://github.com/elastic/elasticsearch/pull/118602)
* Limit size of query [#117898](https://github.com/elastic/elasticsearch/pull/117898)
* Make `numberOfChannels` consistent with layout map by removing duplicated `ChannelSet` [#125636](https://github.com/elastic/elasticsearch/pull/125636)
* Reduce iteration complexity for plan traversal [#123427](https://github.com/elastic/elasticsearch/pull/123427)
* Remove duplicated nested commands [#123085](https://github.com/elastic/elasticsearch/pull/123085)
* Remove redundant sorts from execution plan [#121156](https://github.com/elastic/elasticsearch/pull/121156)
* Revert unwanted ES|QL lexer changes from PR #120354 [#120538](https://github.com/elastic/elasticsearch/pull/120538)
* Revive inlinestats [#122257](https://github.com/elastic/elasticsearch/pull/122257)
* Revive some more of inlinestats functionality [#123589](https://github.com/elastic/elasticsearch/pull/123589)
* TO_LOWER processes all values [#124676](https://github.com/elastic/elasticsearch/pull/124676) (issue: [#124676](https://github.com/elastic/elasticsearch/pull/124676))
* Use a must boolean statement when pushing down to Lucene when scoring is also needed [#124001](https://github.com/elastic/elasticsearch/pull/124001) (issue: [#124001](https://github.com/elastic/elasticsearch/pull/124001))

Engine:
* Hold store reference in `InternalEngine#performActionWithDirectoryReader(...)` [#123010](https://github.com/elastic/elasticsearch/pull/123010) (issue: [#123010](https://github.com/elastic/elasticsearch/pull/123010))

Health:
* Do not recommend increasing `max_shards_per_node` [#120458](https://github.com/elastic/elasticsearch/pull/120458)

Highlighting:
* Restore V8 REST compatibility around highlight `force_source` parameter [#124873](https://github.com/elastic/elasticsearch/pull/124873)

Indices APIs:
* Add `?master_timeout` to `POST /_ilm/migrate_to_data_tiers` [#120883](https://github.com/elastic/elasticsearch/pull/120883)
* Fix NPE in rolling over unknown target and return 404 [#125352](https://github.com/elastic/elasticsearch/pull/125352)
* Fix broken yaml test `30_create_from` [#120662](https://github.com/elastic/elasticsearch/pull/120662)
* Include hidden indices in `DeprecationInfoAction` [#118035](https://github.com/elastic/elasticsearch/pull/118035) (issue: [#118035](https://github.com/elastic/elasticsearch/pull/118035))
* Preventing `ConcurrentModificationException` when updating settings for more than one index [#126077](https://github.com/elastic/elasticsearch/pull/126077)
* Updates the deprecation info API to not warn about system indices and data streams [#122951](https://github.com/elastic/elasticsearch/pull/122951)

Inference:
* [Inference API] Put back legacy EIS URL setting [#121207](https://github.com/elastic/elasticsearch/pull/121207)

Infra/Core:
* Epoch Millis Rounding Down and Not Up 2 [#118353](https://github.com/elastic/elasticsearch/pull/118353)
* Fix system data streams to be restorable from a snapshot [#124651](https://github.com/elastic/elasticsearch/pull/124651) (issue: [#124651](https://github.com/elastic/elasticsearch/pull/124651))
* Have create index return a bad request on poor formatting [#123761](https://github.com/elastic/elasticsearch/pull/123761)
* Include data streams when converting an existing resource to a system resource [#121392](https://github.com/elastic/elasticsearch/pull/121392)
* Reduce Data Loss in System Indices Migration [#121327](https://github.com/elastic/elasticsearch/pull/121327)
* System Index Migration Failure Results in a Non-Recoverable State [#122326](https://github.com/elastic/elasticsearch/pull/122326)
* System data streams are not being upgraded in the feature migration API [#124884](https://github.com/elastic/elasticsearch/pull/124884) (issue: [#124884](https://github.com/elastic/elasticsearch/pull/124884))
* Wrap jackson exception on malformed json string [#114445](https://github.com/elastic/elasticsearch/pull/114445) (issue: [#114445](https://github.com/elastic/elasticsearch/pull/114445))

Infra/Logging:
* Move `SlowLogFieldProvider` instantiation to node construction [#117949](https://github.com/elastic/elasticsearch/pull/117949)

Infra/Metrics:
* Make `randomInstantBetween` always return value in range [minInstant, `maxInstant]` [#114177](https://github.com/elastic/elasticsearch/pull/114177)

Infra/Plugins:
* Remove unnecessary entitlement [#120959](https://github.com/elastic/elasticsearch/pull/120959)
* Restrict agent entitlements to the system classloader unnamed module [#120546](https://github.com/elastic/elasticsearch/pull/120546)

Infra/REST API:
* Fixed a `NullPointerException` in `_capabilities` API when the `path` parameter is null. [#113413](https://github.com/elastic/elasticsearch/pull/113413) (issue: [#113413](https://github.com/elastic/elasticsearch/pull/113413))

Infra/Scripting:
* Register mustache size limit setting [#119291](https://github.com/elastic/elasticsearch/pull/119291)

Infra/Settings:
* Don't allow secure settings in YML config (109115) [#115779](https://github.com/elastic/elasticsearch/pull/115779) (issue: [#115779](https://github.com/elastic/elasticsearch/pull/115779))

Ingest Node:
* Add warning headers for ingest pipelines containing special characters [#114837](https://github.com/elastic/elasticsearch/pull/114837) (issue: [#114837](https://github.com/elastic/elasticsearch/pull/114837))
* Fix geoip databases index access after system feature migration [#121196](https://github.com/elastic/elasticsearch/pull/121196)
* Fix geoip databases index access after system feature migration (again) [#122938](https://github.com/elastic/elasticsearch/pull/122938)
* Fix geoip databases index access after system feature migration (take 3) [#124604](https://github.com/elastic/elasticsearch/pull/124604)
* apm-data: Use representative count as event.success_count if available [#119995](https://github.com/elastic/elasticsearch/pull/119995)

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
* Fixing bedrock event executor terminated cache issue [#118177](https://github.com/elastic/elasticsearch/pull/118177) (issue: [#118177](https://github.com/elastic/elasticsearch/pull/118177))
* Fixing bug setting index when parsing Google Vertex AI results [#117287](https://github.com/elastic/elasticsearch/pull/117287)
* Prevent get datafeeds stats API returning an error when local tasks are slow to stop [#125477](https://github.com/elastic/elasticsearch/pull/125477) (issue: [#125477](https://github.com/elastic/elasticsearch/pull/125477))
* Provide model size statistics as soon as an anomaly detection job is opened [#124638](https://github.com/elastic/elasticsearch/pull/124638) (issue: [#124638](https://github.com/elastic/elasticsearch/pull/124638))
* Retry on streaming errors [#123076](https://github.com/elastic/elasticsearch/pull/123076)
* Return a Conflict status code if the model deployment is stopped by a user [#125204](https://github.com/elastic/elasticsearch/pull/125204) (issue: [#125204](https://github.com/elastic/elasticsearch/pull/125204))
* Set Connect Timeout to 5s [#123272](https://github.com/elastic/elasticsearch/pull/123272)
* Set default similarity for Cohere model to cosine [#125370](https://github.com/elastic/elasticsearch/pull/125370) (issue: [#125370](https://github.com/elastic/elasticsearch/pull/125370))
* Updates to allow using Cohere binary embedding response in semantic search queries [#121827](https://github.com/elastic/elasticsearch/pull/121827)
* Updating Inference Update API documentation to have the correct PUT method [#121048](https://github.com/elastic/elasticsearch/pull/121048)
* Wait for up to 2 seconds for yellow status before starting search [#115938](https://github.com/elastic/elasticsearch/pull/115938) (issues: [#115938](https://github.com/elastic/elasticsearch/pull/115938), [#115938](https://github.com/elastic/elasticsearch/pull/115938), [#115938](https://github.com/elastic/elasticsearch/pull/115938), [#115938](https://github.com/elastic/elasticsearch/pull/115938))
* [Inference API] Fix output stream ordering in `InferenceActionProxy` [#124225](https://github.com/elastic/elasticsearch/pull/124225)
* [Inference API] Fix unique ID message for inference ID matches trained model ID [#119543](https://github.com/elastic/elasticsearch/pull/119543) (issue: [#119543](https://github.com/elastic/elasticsearch/pull/119543))

Mapping:
* Avoid serializing empty `_source` fields in mappings [#122606](https://github.com/elastic/elasticsearch/pull/122606)
* Enable New Semantic Text Format Only On Newly Created Indices [#121556](https://github.com/elastic/elasticsearch/pull/121556)
* Fix Semantic Text 8.x Upgrade Bug [#125446](https://github.com/elastic/elasticsearch/pull/125446)
* Fix propagation of dynamic mapping parameter when applying `copy_to` [#121109](https://github.com/elastic/elasticsearch/pull/121109) (issue: [#121109](https://github.com/elastic/elasticsearch/pull/121109))
* Fix realtime get of nested fields with synthetic source [#119575](https://github.com/elastic/elasticsearch/pull/119575) (issue: [#119575](https://github.com/elastic/elasticsearch/pull/119575))
* Merge field mappers when updating mappings with [subobjects:false] [#120370](https://github.com/elastic/elasticsearch/pull/120370) (issue: [#120370](https://github.com/elastic/elasticsearch/pull/120370))
* Merge template mappings properly during validation [#124784](https://github.com/elastic/elasticsearch/pull/124784) (issue: [#124784](https://github.com/elastic/elasticsearch/pull/124784))
* Tweak `copy_to` handling in synthetic `_source` to account for nested objects [#120974](https://github.com/elastic/elasticsearch/pull/120974) (issue: [#120974](https://github.com/elastic/elasticsearch/pull/120974))

Network:
* Remove ChunkedToXContentBuilder [#119310](https://github.com/elastic/elasticsearch/pull/119310) (issue: [#119310](https://github.com/elastic/elasticsearch/pull/119310))

Ranking:
* Fix LTR query feature with phrases (and two-phase) queries [#125103](https://github.com/elastic/elasticsearch/pull/125103)
* Restore `TextSimilarityRankBuilder` XContent output [#124564](https://github.com/elastic/elasticsearch/pull/124564)

Relevance:
* Prevent Query Rule Creation with Invalid Numeric Match Criteria [#122823](https://github.com/elastic/elasticsearch/pull/122823)

Search:
* Catch and handle disconnect exceptions in search [#115836](https://github.com/elastic/elasticsearch/pull/115836)
* Fix handling of auto expand replicas for stateless indices [#122365](https://github.com/elastic/elasticsearch/pull/122365)
* Fix leak in `DfsQueryPhase` and introduce search disconnect stress test [#116060](https://github.com/elastic/elasticsearch/pull/116060) (issue: [#116060](https://github.com/elastic/elasticsearch/pull/116060))
* Fix/QueryBuilderBWCIT_muted_test [#117831](https://github.com/elastic/elasticsearch/pull/117831)
* Handle long overflow in dates [#124048](https://github.com/elastic/elasticsearch/pull/124048) (issue: [#124048](https://github.com/elastic/elasticsearch/pull/124048))
* Handle search timeout in `SuggestPhase` [#122357](https://github.com/elastic/elasticsearch/pull/122357) (issue: [#122357](https://github.com/elastic/elasticsearch/pull/122357))
* In this pr, a 400 error is returned when _source / _seq_no / _feature / _nested_path / _field_names is requested, rather a 5xx [#117229](https://github.com/elastic/elasticsearch/pull/117229)
* Inconsistency in the _analyzer api when the index is not included [#115930](https://github.com/elastic/elasticsearch/pull/115930)
* Let MLTQuery throw IAE when no analyzer is set [#124662](https://github.com/elastic/elasticsearch/pull/124662) (issue: [#124662](https://github.com/elastic/elasticsearch/pull/124662))
* Load `FieldInfos` from store if not yet initialised through a refresh on `IndexShard` [#125650](https://github.com/elastic/elasticsearch/pull/125650) (issue: [#125650](https://github.com/elastic/elasticsearch/pull/125650))
* Log stack traces on data nodes before they are cleared for transport [#125732](https://github.com/elastic/elasticsearch/pull/125732)
* Minor-Fixes Support 7x segments as archive in 8x / 9x [#125666](https://github.com/elastic/elasticsearch/pull/125666)
* Re-enable parallel collection for field sorted top hits [#125916](https://github.com/elastic/elasticsearch/pull/125916)
* Remove duplicate code in ESIntegTestCase [#120799](https://github.com/elastic/elasticsearch/pull/120799)
* SearchStatesIt failures reported by CI [#117618](https://github.com/elastic/elasticsearch/pull/117618) (issues: [#117618](https://github.com/elastic/elasticsearch/pull/117618), [#117618](https://github.com/elastic/elasticsearch/pull/117618))
* Skip fetching _inference_fields field in legacy semantic_text format [#121720](https://github.com/elastic/elasticsearch/pull/121720)
* Support indices created in ESv6 and updated in ESV7 using different LuceneCodecs as archive in current version. [#119503](https://github.com/elastic/elasticsearch/pull/119503) (issue: [#119503](https://github.com/elastic/elasticsearch/pull/119503))
* Test/107515 restore template with match only text mapper it fail [#120392](https://github.com/elastic/elasticsearch/pull/120392) (issue: [#120392](https://github.com/elastic/elasticsearch/pull/120392))
* Updated Date Range to Follow Documentation When Assuming Missing Values [#112258](https://github.com/elastic/elasticsearch/pull/112258) (issue: [#112258](https://github.com/elastic/elasticsearch/pull/112258))
* `CrossClusterIT` `testCancel` failure [#117750](https://github.com/elastic/elasticsearch/pull/117750) (issue: [#117750](https://github.com/elastic/elasticsearch/pull/117750))
* `SearchServiceTests.testParseSourceValidation` failure [#117963](https://github.com/elastic/elasticsearch/pull/117963)

Snapshot/Restore:
* Add undeclared Azure settings, modify test to exercise them [#118634](https://github.com/elastic/elasticsearch/pull/118634)
* Fork post-snapshot-delete cleanup off master thread [#122731](https://github.com/elastic/elasticsearch/pull/122731)
* Limit number of suppressed S3 deletion errors [#123630](https://github.com/elastic/elasticsearch/pull/123630) (issue: [#123630](https://github.com/elastic/elasticsearch/pull/123630))
* Retry throttled snapshot deletions [#113237](https://github.com/elastic/elasticsearch/pull/113237)
* This PR fixes a bug whereby partial snapshots of system datastreams could be used to restore system features. [#124931](https://github.com/elastic/elasticsearch/pull/124931)
* Use the system index descriptor in the snapshot blob cache cleanup task [#120937](https://github.com/elastic/elasticsearch/pull/120937) (issue: [#120937](https://github.com/elastic/elasticsearch/pull/120937))

Store:
* Do not capture `ClusterChangedEvent` in `IndicesStore` call to #onClusterStateShardsClosed [#120193](https://github.com/elastic/elasticsearch/pull/120193)

Suggesters:
* Return an empty suggestion when suggest phase times out [#122575](https://github.com/elastic/elasticsearch/pull/122575) (issue: [#122575](https://github.com/elastic/elasticsearch/pull/122575))
* Support duplicate suggestions in completion field [#121324](https://github.com/elastic/elasticsearch/pull/121324) (issue: [#121324](https://github.com/elastic/elasticsearch/pull/121324))

Transform:
* If the Transform is configured to write to an alias as its destination index, when the delete_dest_index parameter is set to true, then the Delete API will now delete the write index backing the alias [#122074](https://github.com/elastic/elasticsearch/pull/122074) (issue: [#122074](https://github.com/elastic/elasticsearch/pull/122074))

Vector Search:
* Apply default k for knn query eagerly [#118774](https://github.com/elastic/elasticsearch/pull/118774)
* Fix `bbq_hnsw` merge file cleanup on random IO exceptions [#119691](https://github.com/elastic/elasticsearch/pull/119691) (issue: [#119691](https://github.com/elastic/elasticsearch/pull/119691))
* Knn vector rescoring to sort score docs [#122653](https://github.com/elastic/elasticsearch/pull/122653) (issue: [#122653](https://github.com/elastic/elasticsearch/pull/122653))
* Return appropriate error on null dims update instead of npe [#125716](https://github.com/elastic/elasticsearch/pull/125716)

Watcher:
* Watcher history index has too many indexed fields - [#117701](https://github.com/elastic/elasticsearch/pull/117701) (issue: [#117701](https://github.com/elastic/elasticsearch/pull/117701))

