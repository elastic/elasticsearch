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

% ## version.next [felasticsearch-next-release-notes]
% **Release date:** Month day, year

% ### Features and enhancements [elasticsearch-next-features-enhancements]
% *

% ### Fixes [elasticsearch-next-fixes]
% *

## 9.1.0 [elasticsearch-910-release-notes]
**Release date:** April 01, 2025

### Breaking changes [elasticsearch-910-breaking]

Aggregations:
* Remove date histogram boolean support [#118484](https://github.com/elastic/elasticsearch/pull/118484)

Allocation:
* Increase minimum threshold in shard balancer [#115831](https://github.com/elastic/elasticsearch/pull/115831)
* Remove `cluster.routing.allocation.disk.watermark.enable_for_single_data_node` setting [#114207](https://github.com/elastic/elasticsearch/pull/114207)
* Remove cluster state from `/_cluster/reroute` response [#114231](https://github.com/elastic/elasticsearch/pull/114231) (issue: {es-issue}88978[#88978])

Analysis:
* Snowball stemmers have been upgraded [#114146](https://github.com/elastic/elasticsearch/pull/114146)
* The 'german2' stemmer is now an alias for the 'german' snowball stemmer [#113614](https://github.com/elastic/elasticsearch/pull/113614)
* The 'persian' analyzer has stemmer by default [#113482](https://github.com/elastic/elasticsearch/pull/113482) (issue: {es-issue}113050[#113050])
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
* Remove `client.type` setting [#118192](https://github.com/elastic/elasticsearch/pull/118192) (issue: {es-issue}104574[#104574])
* Remove any references to org.elasticsearch.core.RestApiVersion#V_7 [#118103](https://github.com/elastic/elasticsearch/pull/118103)

Infra/Logging:
* Change `deprecation.elasticsearch` keyword to `elasticsearch.deprecation` [#117933](https://github.com/elastic/elasticsearch/pull/117933) (issue: {es-issue}83251[#83251])

Infra/Metrics:
* Deprecated tracing.apm.* settings got removed. [#119926](https://github.com/elastic/elasticsearch/pull/119926)

Infra/REST API:
* Output a consistent format when generating error json [#90529](https://github.com/elastic/elasticsearch/pull/90529) (issue: {es-issue}89387[#89387])

Ingest Node:
* Remove `ecs` option on `user_agent` processor [#116077](https://github.com/elastic/elasticsearch/pull/116077)
* Remove ignored fallback option on GeoIP processor [#116112](https://github.com/elastic/elasticsearch/pull/116112)

Logs:
* Conditionally enable logsdb by default for data streams matching with logs-*-* pattern. [#121049](https://github.com/elastic/elasticsearch/pull/121049) (issue: {es-issue}106489[#106489])

Machine Learning:
* Disable machine learning on macOS x86_64 [#104125](https://github.com/elastic/elasticsearch/pull/104125)

Mapping:
* Remove support for type, fields, `copy_to` and boost in metadata field definition [#118825](https://github.com/elastic/elasticsearch/pull/118825)
* Turn `_source` meta fieldmapper's mode attribute into a no-op [#119072](https://github.com/elastic/elasticsearch/pull/119072) (issue: {es-issue}118596[#118596])

Search:
* Adjust `random_score` default field to `_seq_no` field [#118671](https://github.com/elastic/elasticsearch/pull/118671)
* Change Semantic Text To Act Like A Normal Text Field [#120813](https://github.com/elastic/elasticsearch/pull/120813)
* Remove legacy params from range query [#116970](https://github.com/elastic/elasticsearch/pull/116970)

Snapshot/Restore:
* Remove deprecated `xpack.searchable.snapshot.allocate_on_rolling_restart` setting [#114202](https://github.com/elastic/elasticsearch/pull/114202)

TLS:
* Remove TLSv1.1 from default protocols [#121731](https://github.com/elastic/elasticsearch/pull/121731)

Transform:
* Remove `data_frame_transforms` roles [#117519](https://github.com/elastic/elasticsearch/pull/117519)

Vector Search:
* Remove old `_knn_search` tech preview API in v9 [#118104](https://github.com/elastic/elasticsearch/pull/118104)

Watcher:
* Removing support for types field in watcher search [#120748](https://github.com/elastic/elasticsearch/pull/120748)

### Deprecations [elasticsearch-910-deprecation]

ES|QL:
* Drop support for brackets from METADATA syntax [#119846](https://github.com/elastic/elasticsearch/pull/119846) (issue: {es-issue}115401[#115401])

Ingest Node:
* Fix `_type` deprecation on simulate pipeline API [#116259](https://github.com/elastic/elasticsearch/pull/116259)

Machine Learning:
* Add deprecation warning for flush API [#121667](https://github.com/elastic/elasticsearch/pull/121667) (issue: {es-issue}121506[#121506])
* Removing index alias creation for deprecated transforms notification index [#117583](https://github.com/elastic/elasticsearch/pull/117583)
* [Inference API] Deprecate elser service [#113216](https://github.com/elastic/elasticsearch/pull/113216)

Rollup:
* Emit deprecation warning when executing one of the rollup APIs [#113131](https://github.com/elastic/elasticsearch/pull/113131)

Search:
* Deprecate Behavioral Analytics CRUD apis [#122960](https://github.com/elastic/elasticsearch/pull/122960)

Security:
* Deprecate certificate based remote cluster security model [#120806](https://github.com/elastic/elasticsearch/pull/120806)

### Features and enhancements [elasticsearch-910-features-enhancements]

Allocation:
* Add a not-master state for desired balance [#116904](https://github.com/elastic/elasticsearch/pull/116904)
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
* Metrics for indexing failures due to version conflicts [#119067](https://github.com/elastic/elasticsearch/pull/119067)
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
* Report Deprecated Indices That Are Flagged To Ignore Migration Reindex As A Warning [#120629](https://github.com/elastic/elasticsearch/pull/120629)
* Update data stream deprecations warnings to new format and filter searchable snapshots from response [#118562](https://github.com/elastic/elasticsearch/pull/118562)

Distributed:
* Make various alias retrieval APIs wait for cluster to unblock [#117230](https://github.com/elastic/elasticsearch/pull/117230)
* Metrics for incremental bulk splits [#116765](https://github.com/elastic/elasticsearch/pull/116765)
* Use Azure blob batch API to delete blobs in batches [#114566](https://github.com/elastic/elasticsearch/pull/114566)

EQL:
* Add support for partial shard results [#116388](https://github.com/elastic/elasticsearch/pull/116388)
* Optional named arguments for function in map [#118619](https://github.com/elastic/elasticsearch/pull/118619)

ES|QL:
* Add ES|QL cross-cluster query telemetry collection [#119474](https://github.com/elastic/elasticsearch/pull/119474)
* Add a `LicenseAware` interface for licensed Nodes [#118931](https://github.com/elastic/elasticsearch/pull/118931) (issue: {es-issue}117405[#117405])
* Add a `PostAnalysisAware,` distribute verification [#119798](https://github.com/elastic/elasticsearch/pull/119798)
* Add a standard deviation aggregating function: STD_DEV [#116531](https://github.com/elastic/elasticsearch/pull/116531)
* Add cluster level reduction [#117731](https://github.com/elastic/elasticsearch/pull/117731)
* Add initial grammar and changes for FORK [#121948](https://github.com/elastic/elasticsearch/pull/121948)
* Add nulls support to Categorize [#117655](https://github.com/elastic/elasticsearch/pull/117655)
* Allow partial results in ES|QL [#121942](https://github.com/elastic/elasticsearch/pull/121942)
* Allow skip shards with `_tier` and `_index` in ES|QL [#123728](https://github.com/elastic/elasticsearch/pull/123728)
* Async search responses have CCS metadata while searches are running [#117265](https://github.com/elastic/elasticsearch/pull/117265)
* Check for early termination in Driver [#118188](https://github.com/elastic/elasticsearch/pull/118188)
* Do not serialize `EsIndex` in plan [#119580](https://github.com/elastic/elasticsearch/pull/119580)
* ESQL - Add Match function options [#120360](https://github.com/elastic/elasticsearch/pull/120360)
* ESQL - Allow full text functions disjunctions for non-full text functions [#120291](https://github.com/elastic/elasticsearch/pull/120291)
* ESQL - Remove restrictions for disjunctions in full text functions [#118544](https://github.com/elastic/elasticsearch/pull/118544)
* ESQL - enabling scoring with METADATA `_score` [#113120](https://github.com/elastic/elasticsearch/pull/113120)
* ESQL Add esql hash function [#117989](https://github.com/elastic/elasticsearch/pull/117989)
* ESQL Support IN operator for Date nanos [#119772](https://github.com/elastic/elasticsearch/pull/119772) (issue: {es-issue}118578[#118578])
* ESQL: CATEGORIZE as a `BlockHash` [#114317](https://github.com/elastic/elasticsearch/pull/114317)
* ESQL: Enable async get to support formatting [#111104](https://github.com/elastic/elasticsearch/pull/111104) (issue: {es-issue}110926[#110926])
* ESQL: Enterprise license enforcement for CCS [#118102](https://github.com/elastic/elasticsearch/pull/118102)
* ES|QL: Partial result on demand for async queries [#118122](https://github.com/elastic/elasticsearch/pull/118122)
* Enable KQL function as a tech preview [#119730](https://github.com/elastic/elasticsearch/pull/119730)
* Enable LOOKUP JOIN in non-snapshot builds [#121193](https://github.com/elastic/elasticsearch/pull/121193) (issue: {es-issue}121185[#121185])
* Enable node-level reduction by default [#119621](https://github.com/elastic/elasticsearch/pull/119621)
* Enable physical plan verification [#118114](https://github.com/elastic/elasticsearch/pull/118114)
* Ensure cluster string could be quoted [#120355](https://github.com/elastic/elasticsearch/pull/120355)
* Esql - Support date nanos in date extract function [#120727](https://github.com/elastic/elasticsearch/pull/120727) (issue: {es-issue}110000[#110000])
* Esql - support date nanos in date format function [#120143](https://github.com/elastic/elasticsearch/pull/120143) (issue: {es-issue}109994[#109994])
* Esql Support date nanos on date diff function [#120645](https://github.com/elastic/elasticsearch/pull/120645) (issue: {es-issue}109999[#109999])
* Esql bucket function for date nanos [#118474](https://github.com/elastic/elasticsearch/pull/118474) (issue: {es-issue}118031[#118031])
* Esql compare nanos and millis [#118027](https://github.com/elastic/elasticsearch/pull/118027) (issue: {es-issue}116281[#116281])
* Esql implicit casting for date nanos [#118697](https://github.com/elastic/elasticsearch/pull/118697) (issue: {es-issue}118476[#118476])
* Expand type compatibility for match function and operator [#117555](https://github.com/elastic/elasticsearch/pull/117555)
* Extend `TranslationAware` to all pushable expressions [#120192](https://github.com/elastic/elasticsearch/pull/120192)
* Fix Driver status iterations and `cpuTime` [#123290](https://github.com/elastic/elasticsearch/pull/123290) (issue: {es-issue}122967[#122967])
* Hash functions [#118938](https://github.com/elastic/elasticsearch/pull/118938)
* Implement a `MetricsAware` interface [#121074](https://github.com/elastic/elasticsearch/pull/121074)
* Implement runtime skip_unavailable=true [#121240](https://github.com/elastic/elasticsearch/pull/121240)
* Initial support for unmapped fields [#119886](https://github.com/elastic/elasticsearch/pull/119886)
* Introduce a pre-mapping logical plan processing step [#121260](https://github.com/elastic/elasticsearch/pull/121260)
* LOOKUP JOIN using field-caps for field mapping [#117246](https://github.com/elastic/elasticsearch/pull/117246)
* Lookup join on multiple join fields not yet supported [#118858](https://github.com/elastic/elasticsearch/pull/118858)
* Move scoring in ES|QL out of snapshot [#120354](https://github.com/elastic/elasticsearch/pull/120354)
* Optimize ST_EXTENT_AGG for `geo_shape` and `cartesian_shape` [#119889](https://github.com/elastic/elasticsearch/pull/119889)
* Push down filter passed lookup join [#118410](https://github.com/elastic/elasticsearch/pull/118410)
* Render `aggregate_metric_double` [#122660](https://github.com/elastic/elasticsearch/pull/122660)
* Resume Driver on cancelled or early finished [#120020](https://github.com/elastic/elasticsearch/pull/120020)
* Retry ES|QL node requests on shard level failures [#120774](https://github.com/elastic/elasticsearch/pull/120774)
* Rewrite TO_UPPER/TO_LOWER comparisons [#118870](https://github.com/elastic/elasticsearch/pull/118870) (issue: {es-issue}118304[#118304])
* ST_EXTENT aggregation [#117451](https://github.com/elastic/elasticsearch/pull/117451) (issue: {es-issue}104659[#104659])
* ST_EXTENT_AGG optimize envelope extraction from doc-values for cartesian_shape [#118802](https://github.com/elastic/elasticsearch/pull/118802)
* Smarter field caps with subscribable listener [#116755](https://github.com/elastic/elasticsearch/pull/116755)
* Support ST_ENVELOPE and related (ST_XMIN, ST_XMAX, ST_YMIN, ST_YMAX) functions [#116964](https://github.com/elastic/elasticsearch/pull/116964) (issue: {es-issue}104875[#104875])
* Support partial results in CCS in ES|QL [#122708](https://github.com/elastic/elasticsearch/pull/122708)
* Support partial sort fields in TopN pushdown [#116043](https://github.com/elastic/elasticsearch/pull/116043) (issue: {es-issue}114515[#114515])
* Support some stats on aggregate_metric_double [#120343](https://github.com/elastic/elasticsearch/pull/120343) (issue: {es-issue}110649[#110649])
* Support subset of metrics in aggregate metric double [#121805](https://github.com/elastic/elasticsearch/pull/121805)
* Take named parameters for identifier and pattern out of snapshot [#121850](https://github.com/elastic/elasticsearch/pull/121850)
* Term query for ES|QL [#117359](https://github.com/elastic/elasticsearch/pull/117359)
* Update grammar to rely on `indexPattern` instead of identifier in join target [#120494](https://github.com/elastic/elasticsearch/pull/120494)
* `_score` should not be a reserved attribute in ES|QL [#118435](https://github.com/elastic/elasticsearch/pull/118435) (issue: {es-issue}118460[#118460])

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
* Add health indicator impact to `HealthPeriodicLogger` [#122390](https://github.com/elastic/elasticsearch/pull/122390)
* Increase `replica_unassigned_buffer_time` default from 3s to 5s [#112834](https://github.com/elastic/elasticsearch/pull/112834)

Highlighting:
* Add Highlighter for Semantic Text Fields [#118064](https://github.com/elastic/elasticsearch/pull/118064)

ILM+SLM:
* Add a `replicate_for` option to the ILM `searchable_snapshot` action [#119003](https://github.com/elastic/elasticsearch/pull/119003)
* Improve SLM Health Indicator to cover missing snapshot [#121370](https://github.com/elastic/elasticsearch/pull/121370)

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
* [Inference API] Rename `model_id` prop to model in EIS sparse inference request body [#122272](https://github.com/elastic/elasticsearch/pull/122272)

Infra/CLI:
* Strengthen encryption for elasticsearch-keystore tool to AES 256 [#119749](https://github.com/elastic/elasticsearch/pull/119749)

Infra/Circuit Breakers:
* Add link to Circuit Breaker "Data too large" exception message [#113561](https://github.com/elastic/elasticsearch/pull/113561)

Infra/Core:
* Add support for specifying reindexing script for system index migration [#119001](https://github.com/elastic/elasticsearch/pull/119001)
* Change default Docker image to be based on UBI minimal instead of Ubuntu [#116739](https://github.com/elastic/elasticsearch/pull/116739)
* Improve size limiting string message [#122427](https://github.com/elastic/elasticsearch/pull/122427)
* Infrastructure for assuming cluster features in the next major version [#118143](https://github.com/elastic/elasticsearch/pull/118143)

Infra/Metrics:
* Add `ensureGreen` test method for use with `adminClient` [#113425](https://github.com/elastic/elasticsearch/pull/113425)

Infra/REST API:
* A new query parameter `?include_source_on_error` was added for create / index, update and bulk REST APIs to control
if to include the document source in the error response in case of parsing errors. The default value is `true`. [#120725](https://github.com/elastic/elasticsearch/pull/120725)

Infra/Scripting:
* Add a `mustache.max_output_size_bytes` setting to limit the length of results from mustache scripts [#114002](https://github.com/elastic/elasticsearch/pull/114002)

Infra/Settings:
* Introduce `IndexSettingDeprecatedInV8AndRemovedInV9` Setting property [#120334](https://github.com/elastic/elasticsearch/pull/120334)
* Run `TransportClusterGetSettingsAction` on local node [#119831](https://github.com/elastic/elasticsearch/pull/119831)

Ingest Node:
* Allow setting the `type` in the reroute processor [#122409](https://github.com/elastic/elasticsearch/pull/122409) (issue: {es-issue}121553[#121553])
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
* Configure index sorting through index settings for logsdb [#118968](https://github.com/elastic/elasticsearch/pull/118968) (issue: {es-issue}118686[#118686])
* Optimize loading mappings when determining synthetic source usage and whether host.name can be sorted on. [#120055](https://github.com/elastic/elasticsearch/pull/120055)

Machine Learning:
* Add DeBERTa-V2/V3 tokenizer [#111852](https://github.com/elastic/elasticsearch/pull/111852)
* Add Inference Unified API for chat completions for OpenAI [#117589](https://github.com/elastic/elasticsearch/pull/117589)
* Add Jina AI API to do inference for Embedding and Rerank models [#118652](https://github.com/elastic/elasticsearch/pull/118652)
* Add enterprise license check for Inference API actions [#119893](https://github.com/elastic/elasticsearch/pull/119893)
* Adding chunking settings to `IbmWatsonxService` [#114914](https://github.com/elastic/elasticsearch/pull/114914)
* Adding default endpoint for Elastic Rerank [#117939](https://github.com/elastic/elasticsearch/pull/117939)
* Adding elser default endpoint for EIS [#122066](https://github.com/elastic/elasticsearch/pull/122066)
* Adding endpoint creation validation for all task types to remaining services [#115020](https://github.com/elastic/elasticsearch/pull/115020)
* Adding endpoint creation validation to `ElasticInferenceService` [#117642](https://github.com/elastic/elasticsearch/pull/117642)
* Adding integration for VoyageAI embeddings and rerank models [#122134](https://github.com/elastic/elasticsearch/pull/122134)
* Adding support for binary embedding type to Cohere service embedding type [#120751](https://github.com/elastic/elasticsearch/pull/120751)
* Adding support for specifying embedding type to Jina AI service settings [#121548](https://github.com/elastic/elasticsearch/pull/121548)
* Check for presence of error object when validating streaming responses from integrations in the inference API [#118375](https://github.com/elastic/elasticsearch/pull/118375)
* ES|QL `change_point` processing command [#120998](https://github.com/elastic/elasticsearch/pull/120998)
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
* Enable the use of nested field type with index.mode=time_series [#122224](https://github.com/elastic/elasticsearch/pull/122224) (issue: {es-issue}120874[#120874])
* Improved error message when index field type is invalid [#122860](https://github.com/elastic/elasticsearch/pull/122860)
* Introduce `FallbackSyntheticSourceBlockLoader` and apply it to keyword fields [#119546](https://github.com/elastic/elasticsearch/pull/119546)
* Store arrays offsets for ip fields natively with synthetic source [#122999](https://github.com/elastic/elasticsearch/pull/122999)
* Store arrays offsets for keyword fields natively with synthetic source instead of falling back to ignored source. [#113757](https://github.com/elastic/elasticsearch/pull/113757)
* Use `FallbackSyntheticSourceBlockLoader` for `unsigned_long` and `scaled_float` fields [#122637](https://github.com/elastic/elasticsearch/pull/122637)
* Use `FallbackSyntheticSourceBlockLoader` for number fields [#122280](https://github.com/elastic/elasticsearch/pull/122280)

Network:
* Allow http unsafe buffers by default [#116115](https://github.com/elastic/elasticsearch/pull/116115)
* Http stream activity tracker and exceptions handling [#119564](https://github.com/elastic/elasticsearch/pull/119564)
* Remove HTTP content copies [#117303](https://github.com/elastic/elasticsearch/pull/117303)
* `ConnectTransportException` returns retryable BAD_GATEWAY [#118681](https://github.com/elastic/elasticsearch/pull/118681) (issue: {es-issue}118320[#118320])

Ranking:
* Add a generic `rescorer` retriever based on the search request's rescore functionality [#118585](https://github.com/elastic/elasticsearch/pull/118585) (issue: {es-issue}118327[#118327])
* Set default reranker for text similarity reranker to Elastic reranker [#120551](https://github.com/elastic/elasticsearch/pull/120551)

Recovery:
* Allow archive and searchable snapshots indices in N-2 version [#118941](https://github.com/elastic/elasticsearch/pull/118941)
* Trigger merges after recovery [#113102](https://github.com/elastic/elasticsearch/pull/113102)

Reindex:
* Change Reindexing metrics unit from millis to seconds [#115721](https://github.com/elastic/elasticsearch/pull/115721)

Relevance:
* Add Multi-Field Support for Semantic Text Fields [#120128](https://github.com/elastic/elasticsearch/pull/120128)

Search:
* Account for the `SearchHit` source in circuit breaker [#121920](https://github.com/elastic/elasticsearch/pull/121920) (issue: {es-issue}89656[#89656])
* Add match support for `semantic_text` fields [#117839](https://github.com/elastic/elasticsearch/pull/117839)
* Add support for `sparse_vector` queries against `semantic_text` fields [#118617](https://github.com/elastic/elasticsearch/pull/118617)
* Add support for knn vector queries on `semantic_text` fields [#119011](https://github.com/elastic/elasticsearch/pull/119011)
* Adding linear retriever to support weighted sums of sub-retrievers [#120222](https://github.com/elastic/elasticsearch/pull/120222)
* Address and remove any references of RestApiVersion version 7 [#117572](https://github.com/elastic/elasticsearch/pull/117572)
* Feat: add a user-configurable timeout parameter to the `_resolve/cluster` API [#120542](https://github.com/elastic/elasticsearch/pull/120542)
* Make semantic text part of the text family [#119792](https://github.com/elastic/elasticsearch/pull/119792)
* Only aggregations require at least one shard request [#115314](https://github.com/elastic/elasticsearch/pull/115314)
* Optionally allow text similarity reranking to fail [#121784](https://github.com/elastic/elasticsearch/pull/121784)
* Prevent data nodes from sending stack traces to coordinator when `error_trace=false` [#118266](https://github.com/elastic/elasticsearch/pull/118266)
* Propagate status codes from shard failures appropriately [#118016](https://github.com/elastic/elasticsearch/pull/118016) (issue: {es-issue}118482[#118482])

Security:
* Add refresh `.security` index call between security migrations [#114879](https://github.com/elastic/elasticsearch/pull/114879)

Snapshot/Restore:
* Add IMDSv2 support to `repository-s3` [#117748](https://github.com/elastic/elasticsearch/pull/117748) (issue: {es-issue}105135[#105135])
* Expose operation and request counts separately in repository stats [#117530](https://github.com/elastic/elasticsearch/pull/117530) (issue: {es-issue}104443[#104443])
* Retry `S3BlobContainer#getRegister` on all exceptions [#114813](https://github.com/elastic/elasticsearch/pull/114813)
* Retry internally when CAS upload is throttled [GCS] [#120250](https://github.com/elastic/elasticsearch/pull/120250) (issue: {es-issue}116546[#116546])
* Track shard snapshot progress during node shutdown [#112567](https://github.com/elastic/elasticsearch/pull/112567)

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
* Add new experimental `rank_vectors` mapping for late-interaction second order ranking [#118804](https://github.com/elastic/elasticsearch/pull/118804)
* Adds implementations of dotProduct and cosineSimilarity painless methods to operate on float vectors for byte fields [#122381](https://github.com/elastic/elasticsearch/pull/122381) (issue: {es-issue}117274[#117274])
* Even better(er) binary quantization [#117994](https://github.com/elastic/elasticsearch/pull/117994)
* KNN vector rescoring for quantized vectors [#116663](https://github.com/elastic/elasticsearch/pull/116663)
* Mark bbq indices as GA and add rolling upgrade integration tests [#121105](https://github.com/elastic/elasticsearch/pull/121105)
* Speed up bit compared with floats or bytes script operations [#117199](https://github.com/elastic/elasticsearch/pull/117199)

Watcher:
* Run `TransportGetWatcherSettingsAction` on local node [#122857](https://github.com/elastic/elasticsearch/pull/122857)

### Fixes [elasticsearch-910-fixes]

Aggregations:
* Disable concurrency when `top_hits` sorts on anything but `_score` [#123610](https://github.com/elastic/elasticsearch/pull/123610)
* Handle with `illegalArgumentExceptions` negative values in HDR percentile aggregations [#116174](https://github.com/elastic/elasticsearch/pull/116174) (issue: {es-issue}115777[#115777])

Allocation:
* Deduplicate allocation stats calls [#123246](https://github.com/elastic/elasticsearch/pull/123246)
* `DesiredBalanceReconciler` always returns `AllocationStats` [#122458](https://github.com/elastic/elasticsearch/pull/122458)

Analysis:
* Analyze API to return 400 for wrong custom analyzer [#121568](https://github.com/elastic/elasticsearch/pull/121568) (issue: {es-issue}121443[#121443])

Authentication:
* Improve jwt logging on failed auth [#122247](https://github.com/elastic/elasticsearch/pull/122247)

CAT APIs:
* Fix cat_component_templates documentation [#120487](https://github.com/elastic/elasticsearch/pull/120487)

CRUD:
* Preserve thread context when waiting for segment generation in RTG [#114623](https://github.com/elastic/elasticsearch/pull/114623)
* Preserve thread context when waiting for segment generation in RTG [#117148](https://github.com/elastic/elasticsearch/pull/117148)
* Reduce license checks in `LicensedWriteLoadForecaster` [#123346](https://github.com/elastic/elasticsearch/pull/123346) (issue: {es-issue}123247[#123247])

Data streams:
* Add `_metric_names_hash` field to OTel metric mappings [#120952](https://github.com/elastic/elasticsearch/pull/120952)
* Avoid updating settings version in `MetadataMigrateToDataStreamService` when settings have not changed [#118704](https://github.com/elastic/elasticsearch/pull/118704)
* Block-writes cannot be added after read-only [#119007](https://github.com/elastic/elasticsearch/pull/119007) (issue: {es-issue}119002[#119002])
* Ensure removal of index blocks does not leave key with null value [#122246](https://github.com/elastic/elasticsearch/pull/122246)
* Match dot prefix of migrated DS backing index with the source index [#120042](https://github.com/elastic/elasticsearch/pull/120042)
* Refresh source index before reindexing data stream index [#120752](https://github.com/elastic/elasticsearch/pull/120752) (issue: {es-issue}120314[#120314])
* Updating `TransportRolloverAction.checkBlock` so that non-write-index blocks do not prevent data stream rollover [#122905](https://github.com/elastic/elasticsearch/pull/122905)
* `ReindexDataStreamIndex` bug in assertion caused by reference equality [#121325](https://github.com/elastic/elasticsearch/pull/121325)

Downsampling:
* Copy metrics and `default_metric` properties when downsampling `aggregate_metric_double` [#121727](https://github.com/elastic/elasticsearch/pull/121727) (issues: {es-issue}119696[#119696], {es-issue}96076[#96076])

EQL:
* Fix JOIN command validation (not supported) [#122011](https://github.com/elastic/elasticsearch/pull/122011)

ES|QL:
* Add support to VALUES aggregation for spatial types [#122886](https://github.com/elastic/elasticsearch/pull/122886) (issue: {es-issue}122413[#122413])
* Allow the data type of `null` in filters [#118324](https://github.com/elastic/elasticsearch/pull/118324) (issue: {es-issue}116351[#116351])
* Avoid over collecting in Limit or Lucene Operator [#123296](https://github.com/elastic/elasticsearch/pull/123296)
* Correct line and column numbers of missing named parameters [#120852](https://github.com/elastic/elasticsearch/pull/120852)
* Drop null columns in text formats [#117643](https://github.com/elastic/elasticsearch/pull/117643) (issue: {es-issue}116848[#116848])
* ESQL: Fix inconsistent results in using scaled_float field [#122586](https://github.com/elastic/elasticsearch/pull/122586) (issue: {es-issue}122547[#122547])
* Fix ENRICH validation for use of wildcards [#121911](https://github.com/elastic/elasticsearch/pull/121911)
* Fix ROUND() with unsigned longs throwing in some edge cases [#119536](https://github.com/elastic/elasticsearch/pull/119536)
* Fix TDigestState.read CB leaks [#114303](https://github.com/elastic/elasticsearch/pull/114303) (issue: {es-issue}114194[#114194])
* Fix TopN row size estimate [#119476](https://github.com/elastic/elasticsearch/pull/119476) (issue: {es-issue}106956[#106956])
* Fix `AbstractShapeGeometryFieldMapperTests` [#119265](https://github.com/elastic/elasticsearch/pull/119265) (issue: {es-issue}119201[#119201])
* Fix a bug in TOP [#121552](https://github.com/elastic/elasticsearch/pull/121552)
* Fix async stop sometimes not properly collecting result [#121843](https://github.com/elastic/elasticsearch/pull/121843) (issue: {es-issue}121249[#121249])
* Fix attribute set equals [#118823](https://github.com/elastic/elasticsearch/pull/118823)
* Fix double lookup failure on ESQL [#115616](https://github.com/elastic/elasticsearch/pull/115616) (issue: {es-issue}111398[#111398])
* Fix early termination in `LuceneSourceOperator` [#123197](https://github.com/elastic/elasticsearch/pull/123197)
* Fix function registry concurrency issues on constructor [#123492](https://github.com/elastic/elasticsearch/pull/123492) (issue: {es-issue}123430[#123430])
* Fix functions emitting warnings with no source [#122821](https://github.com/elastic/elasticsearch/pull/122821) (issue: {es-issue}122588[#122588])
* Fix listener leak in exchange service [#122417](https://github.com/elastic/elasticsearch/pull/122417) (issue: {es-issue}122271[#122271])
* Fix queries with document level security on lookup indexes [#120617](https://github.com/elastic/elasticsearch/pull/120617) (issue: {es-issue}120509[#120509])
* Fix writing for LOOKUP status [#119296](https://github.com/elastic/elasticsearch/pull/119296) (issue: {es-issue}119086[#119086])
* Implicit numeric casting for CASE/GREATEST/LEAST [#122601](https://github.com/elastic/elasticsearch/pull/122601) (issue: {es-issue}121890[#121890])
* Limit memory usage of `fold` [#118602](https://github.com/elastic/elasticsearch/pull/118602)
* Limit size of query [#117898](https://github.com/elastic/elasticsearch/pull/117898)
* Reduce iteration complexity for plan traversal [#123427](https://github.com/elastic/elasticsearch/pull/123427)
* Remove duplicated nested commands [#123085](https://github.com/elastic/elasticsearch/pull/123085)
* Remove redundant sorts from execution plan [#121156](https://github.com/elastic/elasticsearch/pull/121156)
* Revert unwanted ES|QL lexer changes from PR #120354 [#120538](https://github.com/elastic/elasticsearch/pull/120538)
* Revive inlinestats [#122257](https://github.com/elastic/elasticsearch/pull/122257)
* Speed up VALUES for many buckets [#123073](https://github.com/elastic/elasticsearch/pull/123073)

Engine:
* Hold store reference in `InternalEngine#performActionWithDirectoryReader(...)` [#123010](https://github.com/elastic/elasticsearch/pull/123010) (issue: {es-issue}122974[#122974])

Health:
* Do not recommend increasing `max_shards_per_node` [#120458](https://github.com/elastic/elasticsearch/pull/120458)

Indices APIs:
* Add `?master_timeout` to `POST /_ilm/migrate_to_data_tiers` [#120883](https://github.com/elastic/elasticsearch/pull/120883)
* Fix broken yaml test `30_create_from` [#120662](https://github.com/elastic/elasticsearch/pull/120662)
* Include hidden indices in `DeprecationInfoAction` [#118035](https://github.com/elastic/elasticsearch/pull/118035) (issue: {es-issue}118020[#118020])
* Updates the deprecation info API to not warn about system indices and data streams [#122951](https://github.com/elastic/elasticsearch/pull/122951)

Inference:
* [Inference API] Put back legacy EIS URL setting [#121207](https://github.com/elastic/elasticsearch/pull/121207)

Infra/Core:
* Epoch Millis Rounding Down and Not Up 2 [#118353](https://github.com/elastic/elasticsearch/pull/118353)
* Include data streams when converting an existing resource to a system resource [#121392](https://github.com/elastic/elasticsearch/pull/121392)
* Reduce Data Loss in System Indices Migration [#121327](https://github.com/elastic/elasticsearch/pull/121327)
* System Index Migration Failure Results in a Non-Recoverable State [#122326](https://github.com/elastic/elasticsearch/pull/122326)
* Wrap jackson exception on malformed json string [#114445](https://github.com/elastic/elasticsearch/pull/114445) (issue: {es-issue}114142[#114142])

Infra/Logging:
* Move `SlowLogFieldProvider` instantiation to node construction [#117949](https://github.com/elastic/elasticsearch/pull/117949)

Infra/Metrics:
* Make `randomInstantBetween` always return value in range [minInstant, `maxInstant]` [#114177](https://github.com/elastic/elasticsearch/pull/114177)

Infra/Plugins:
* Remove unnecessary entitlement [#120959](https://github.com/elastic/elasticsearch/pull/120959)
* Restrict agent entitlements to the system classloader unnamed module [#120546](https://github.com/elastic/elasticsearch/pull/120546)

Infra/REST API:
* Fixed a `NullPointerException` in `_capabilities` API when the `path` parameter is null. [#113413](https://github.com/elastic/elasticsearch/pull/113413) (issue: {es-issue}113413[#113413])

Infra/Scripting:
* Register mustache size limit setting [#119291](https://github.com/elastic/elasticsearch/pull/119291)

Infra/Settings:
* Don't allow secure settings in YML config (109115) [#115779](https://github.com/elastic/elasticsearch/pull/115779) (issue: {es-issue}109115[#109115])

Ingest:
* Fix `ArrayIndexOutOfBoundsException` in `ShardBulkInferenceActionFilter` [#122538](https://github.com/elastic/elasticsearch/pull/122538)

Ingest Node:
* Add warning headers for ingest pipelines containing special characters [#114837](https://github.com/elastic/elasticsearch/pull/114837) (issue: {es-issue}104411[#104411])
* Canonicalize processor names and types in `IngestStats` [#122610](https://github.com/elastic/elasticsearch/pull/122610)
* Deduplicate `IngestStats` and `IngestStats.Stats` identity records when deserializing [#122496](https://github.com/elastic/elasticsearch/pull/122496)
* Fix geoip databases index access after system feature migration [#121196](https://github.com/elastic/elasticsearch/pull/121196)
* Fix geoip databases index access after system feature migration (again) [#122938](https://github.com/elastic/elasticsearch/pull/122938)
* Fix redact processor arraycopy bug [#122640](https://github.com/elastic/elasticsearch/pull/122640)
* Register `IngestGeoIpMetadata` as a NamedXContent [#123079](https://github.com/elastic/elasticsearch/pull/123079)
* Use ordered maps for `PipelineConfiguration` xcontent deserialization [#123403](https://github.com/elastic/elasticsearch/pull/123403)
* apm-data: Use representative count as event.success_count if available [#119995](https://github.com/elastic/elasticsearch/pull/119995)

Logs:
* Always check if index mode is logsdb [#116922](https://github.com/elastic/elasticsearch/pull/116922)
* Fix issues that prevents using search only snapshots for indices that use index sorting. This is includes Logsdb and time series indices. [#122199](https://github.com/elastic/elasticsearch/pull/122199)
* Use min node version to guard injecting settings in logs provider [#123005](https://github.com/elastic/elasticsearch/pull/123005) (issue: {es-issue}122950[#122950])

Machine Learning:
* Add `ElasticInferenceServiceCompletionServiceSettings` [#123155](https://github.com/elastic/elasticsearch/pull/123155)
* Add enterprise license check to inference action for semantic text fields [#122293](https://github.com/elastic/elasticsearch/pull/122293)
* Change format for Unified Chat [#121396](https://github.com/elastic/elasticsearch/pull/121396)
* Fix get all inference endponts not returning multiple endpoints sharing model deployment [#121821](https://github.com/elastic/elasticsearch/pull/121821)
* Fix serialising the inference update request [#122278](https://github.com/elastic/elasticsearch/pull/122278)
* Fixing bedrock event executor terminated cache issue [#118177](https://github.com/elastic/elasticsearch/pull/118177) (issue: {es-issue}117916[#117916])
* Fixing bug setting index when parsing Google Vertex AI results [#117287](https://github.com/elastic/elasticsearch/pull/117287)
* Set Connect Timeout to 5s [#123272](https://github.com/elastic/elasticsearch/pull/123272)
* Updates to allow using Cohere binary embedding response in semantic search queries [#121827](https://github.com/elastic/elasticsearch/pull/121827)
* Updating Inference Update API documentation to have the correct PUT method [#121048](https://github.com/elastic/elasticsearch/pull/121048)
* Wait for up to 2 seconds for yellow status before starting search [#115938](https://github.com/elastic/elasticsearch/pull/115938) (issues: {es-issue}107777[#107777], {es-issue}105955[#105955], {es-issue}107815[#107815], {es-issue}112191[#112191])
* [Inference API] Fix unique ID message for inference ID matches trained model ID [#119543](https://github.com/elastic/elasticsearch/pull/119543) (issue: {es-issue}111312[#111312])

Mapping:
* Enable New Semantic Text Format Only On Newly Created Indices [#121556](https://github.com/elastic/elasticsearch/pull/121556)
* Fix propagation of dynamic mapping parameter when applying `copy_to` [#121109](https://github.com/elastic/elasticsearch/pull/121109) (issue: {es-issue}113049[#113049])
* Fix realtime get of nested fields with synthetic source [#119575](https://github.com/elastic/elasticsearch/pull/119575) (issue: {es-issue}119553[#119553])
* Fix synthetic source bug that would mishandle nested `dense_vector` fields [#122425](https://github.com/elastic/elasticsearch/pull/122425)
* Merge field mappers when updating mappings with [subobjects:false] [#120370](https://github.com/elastic/elasticsearch/pull/120370) (issue: {es-issue}120216[#120216])
* Tweak `copy_to` handling in synthetic `_source` to account for nested objects [#120974](https://github.com/elastic/elasticsearch/pull/120974) (issue: {es-issue}120831[#120831])
* fix stale data in synthetic source for string stored field [#123105](https://github.com/elastic/elasticsearch/pull/123105) (issue: {es-issue}123110[#123110])

Network:
* Remove ChunkedToXContentBuilder [#119310](https://github.com/elastic/elasticsearch/pull/119310) (issue: {es-issue}118647[#118647])

Search:
* Catch and handle disconnect exceptions in search [#115836](https://github.com/elastic/elasticsearch/pull/115836)
* Fix handling of auto expand replicas for stateless indices [#122365](https://github.com/elastic/elasticsearch/pull/122365)
* Fix leak in `DfsQueryPhase` and introduce search disconnect stress test [#116060](https://github.com/elastic/elasticsearch/pull/116060) (issue: {es-issue}115056[#115056])
* Fix/QueryBuilderBWCIT_muted_test [#117831](https://github.com/elastic/elasticsearch/pull/117831)
* Handle search timeout in `SuggestPhase` [#122357](https://github.com/elastic/elasticsearch/pull/122357) (issue: {es-issue}122186[#122186])
* In this pr, a 400 error is returned when _source / _seq_no / _feature / _nested_path / _field_names is requested, rather a 5xx [#117229](https://github.com/elastic/elasticsearch/pull/117229)
* Inconsistency in the _analyzer api when the index is not included [#115930](https://github.com/elastic/elasticsearch/pull/115930)
* Remove duplicate code in ESIntegTestCase [#120799](https://github.com/elastic/elasticsearch/pull/120799)
* SearchStatesIt failures reported by CI [#117618](https://github.com/elastic/elasticsearch/pull/117618) (issues: {es-issue}116617[#116617], {es-issue}116618[#116618])
* Skip fetching _inference_fields field in legacy semantic_text format [#121720](https://github.com/elastic/elasticsearch/pull/121720)
* Test/107515 restore template with match only text mapper it fail [#120392](https://github.com/elastic/elasticsearch/pull/120392) (issue: {es-issue}107515[#107515])
* Updated Date Range to Follow Documentation When Assuming Missing Values [#112258](https://github.com/elastic/elasticsearch/pull/112258) (issue: {es-issue}111484[#111484])
* `CrossClusterIT` `testCancel` failure [#117750](https://github.com/elastic/elasticsearch/pull/117750) (issue: {es-issue}108061[#108061])
* `SearchServiceTests.testParseSourceValidation` failure [#117963](https://github.com/elastic/elasticsearch/pull/117963)

Snapshot/Restore:
* Add undeclared Azure settings, modify test to exercise them [#118634](https://github.com/elastic/elasticsearch/pull/118634)
* Fork post-snapshot-delete cleanup off master thread [#122731](https://github.com/elastic/elasticsearch/pull/122731)
* Limit number of suppressed S3 deletion errors [#123630](https://github.com/elastic/elasticsearch/pull/123630) (issue: {es-issue}123354[#123354])
* Retry throttled snapshot deletions [#113237](https://github.com/elastic/elasticsearch/pull/113237)
* Use the system index descriptor in the snapshot blob cache cleanup task [#120937](https://github.com/elastic/elasticsearch/pull/120937) (issue: {es-issue}120518[#120518])

Stats:
* Fixing serialization of `ScriptStats` `cache_evictions_history` [#123384](https://github.com/elastic/elasticsearch/pull/123384)

Store:
* Do not capture `ClusterChangedEvent` in `IndicesStore` call to #onClusterStateShardsClosed [#120193](https://github.com/elastic/elasticsearch/pull/120193)

Suggesters:
* Return an empty suggestion when suggest phase times out [#122575](https://github.com/elastic/elasticsearch/pull/122575) (issue: {es-issue}122548[#122548])
* Support duplicate suggestions in completion field [#121324](https://github.com/elastic/elasticsearch/pull/121324) (issue: {es-issue}82432[#82432])

Transform:
* If the Transform is configured to write to an alias as its destination index, when the delete_dest_index parameter is set to true, then the Delete API will now delete the write index backing the alias [#122074](https://github.com/elastic/elasticsearch/pull/122074) (issue: {es-issue}121913[#121913])

Vector Search:
* Apply default k for knn query eagerly [#118774](https://github.com/elastic/elasticsearch/pull/118774)
* Fix `bbq_hnsw` merge file cleanup on random IO exceptions [#119691](https://github.com/elastic/elasticsearch/pull/119691) (issue: {es-issue}119392[#119392])
* Knn vector rescoring to sort score docs [#122653](https://github.com/elastic/elasticsearch/pull/122653) (issue: {es-issue}119711[#119711])

Watcher:
* Watcher history index has too many indexed fields - [#117701](https://github.com/elastic/elasticsearch/pull/117701) (issue: {es-issue}71479[#71479])

### Upgrades [elasticsearch-910-upgrade]

Authentication:
* Bump json-smart and oauth2-oidc-sdk [#122737](https://github.com/elastic/elasticsearch/pull/122737)

Infra/Core:
* Bump major version for feature migration system indices [#117243](https://github.com/elastic/elasticsearch/pull/117243)
* Update ASM 9.7 -> 9.7.1 to support JDK 24 [#118094](https://github.com/elastic/elasticsearch/pull/118094)

Machine Learning:
* Automatically rollover legacy .ml-anomalies indices [#120913](https://github.com/elastic/elasticsearch/pull/120913)
* Automatically rollover legacy ml indices [#120405](https://github.com/elastic/elasticsearch/pull/120405)
* Change the auditor to write via an alias [#120064](https://github.com/elastic/elasticsearch/pull/120064)

Search:
* Upgrade to Lucene 10 [#114741](https://github.com/elastic/elasticsearch/pull/114741)
* Upgrade to Lucene 10.1.0 [#119308](https://github.com/elastic/elasticsearch/pull/119308)

Snapshot/Restore:
* Upgrade AWS SDK to v1.12.746 [#122431](https://github.com/elastic/elasticsearch/pull/122431)


