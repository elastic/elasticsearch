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

