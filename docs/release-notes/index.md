---
navigation_title: "Elasticsearch"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-release-notes.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-release-notes.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/master/release-notes-9.0.0.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/master/migrating-9.0.html
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

## 9.0.0 [elasticsearch-900-release-notes]
**Release date:** March 25, 2025

### Features and enhancements [elasticsearch-900-features-enhancements]
Allocation
:   * Only publish desired balance gauges on master [#115383](https://github.com/elastic/elasticsearch/pull/115383)


Authorization
:   * Add a `monitor_stats` privilege and allow that privilege for remote cluster privileges [#114964](https://github.com/elastic/elasticsearch/pull/114964)
* [Security Solution] Add `create_index` to `kibana_system` role for index/DS `.logs-endpoint.action.responses-*` [#115241](https://github.com/elastic/elasticsearch/pull/115241)


CRUD
:   * Suppress merge-on-recovery for older indices [#113462](https://github.com/elastic/elasticsearch/pull/113462)


Data streams
:   * Adding a deprecation info API warning for data streams with old indices [#116447](https://github.com/elastic/elasticsearch/pull/116447)
* Apm-data: disable date_detection for all apm data streams [#116995](https://github.com/elastic/elasticsearch/pull/116995)
* Add default ILM policies and switch to ILM for apm-data plugin [#115687](https://github.com/elastic/elasticsearch/pull/115687)


Distributed
:   * Metrics for incremental bulk splits [#116765](https://github.com/elastic/elasticsearch/pull/116765)
* Use Azure blob batch API to delete blobs in batches [#114566](https://github.com/elastic/elasticsearch/pull/114566)


ES|QL
:   * Add ES|QL `bit_length` function [#115792](https://github.com/elastic/elasticsearch/pull/115792)
* ESQL: Honor skip_unavailable setting for nonmatching indices errors at planning time [#116348](https://github.com/elastic/elasticsearch/pull/116348) (issue: [#114531](https://github.com/elastic/elasticsearch/issues/114531))
* ESQL: Remove parent from `FieldAttribute` [#112881](https://github.com/elastic/elasticsearch/pull/112881)
* ESQL: extract common filter from aggs [#115678](https://github.com/elastic/elasticsearch/pull/115678)
* ESQL: optimise aggregations filtered by false/null into evals [#115858](https://github.com/elastic/elasticsearch/pull/115858)
* ES|QL CCS uses `skip_unavailable` setting for handling disconnected remote clusters [#115266](https://github.com/elastic/elasticsearch/pull/115266) (issue: [#114531](https://github.com/elastic/elasticsearch/issues/114531))
* ES|QL: add metrics for functions [#114620](https://github.com/elastic/elasticsearch/pull/114620)
* Esql Enable Date Nanos (tech preview) [#117080](https://github.com/elastic/elasticsearch/pull/117080)
* Support partial sort fields in TopN pushdown [#116043](https://github.com/elastic/elasticsearch/pull/116043) (issue: [#114515](https://github.com/elastic/elasticsearch/issues/114515))
* [ES|QL] Implicit casting string literal to intervals [#115814](https://github.com/elastic/elasticsearch/pull/115814) (issue: [#115352](https://github.com/elastic/elasticsearch/issues/115352))
* Add support for `BYTE_LENGTH` scalar function [#116591](https://github.com/elastic/elasticsearch/pull/116591)
* Esql/lookup join grammar [#116515](https://github.com/elastic/elasticsearch/pull/116515)
* Remove snapshot build restriction for match and qstr functions [#114482](https://github.com/elastic/elasticsearch/pull/114482)


Health
:   * Increase `replica_unassigned_buffer_time` default from 3s to 5s [#112834](https://github.com/elastic/elasticsearch/pull/112834)


Indices APIs
:   * Ensure class resource stream is closed in `ResourceUtils` [#116437](https://github.com/elastic/elasticsearch/pull/116437)


Inference
:   * Add version prefix to Inference Service API path [#117095](https://github.com/elastic/elasticsearch/pull/117095)


Infra/Circuit Breakers
:   * Add link to Circuit Breaker "Data too large" exception message [#113561](https://github.com/elastic/elasticsearch/pull/113561)


Infra/Core
:   * Support for unsigned 64 bit numbers in Cpu stats [#114681](https://github.com/elastic/elasticsearch/pull/114681) (issue: [#112274](https://github.com/elastic/elasticsearch/issues/112274))


Infra/Metrics
:   * Add `ensureGreen` test method for use with `adminClient` [#113425](https://github.com/elastic/elasticsearch/pull/113425)


Infra/Scripting
:   * Add a `mustache.max_output_size_bytes` setting to limit the length of results from mustache scripts [#114002](https://github.com/elastic/elasticsearch/pull/114002)


Ingest Node
:   * Add postal_code support to the City and Enterprise databases [#114193](https://github.com/elastic/elasticsearch/pull/114193)
* Add support for registered country fields for maxmind geoip databases [#114521](https://github.com/elastic/elasticsearch/pull/114521)
* Adding support for additional mapping to simulate ingest API [#114742](https://github.com/elastic/elasticsearch/pull/114742)
* Adding support for simulate ingest mapping adddition for indices with mappings that do not come from templates [#115359](https://github.com/elastic/elasticsearch/pull/115359)
* Support IPinfo database configurations [#114548](https://github.com/elastic/elasticsearch/pull/114548)
* Support more maxmind fields in the geoip processor [#114268](https://github.com/elastic/elasticsearch/pull/114268)


Logs
:   * Add logsdb telemetry [#115994](https://github.com/elastic/elasticsearch/pull/115994)
* Add num docs and size to logsdb telemetry [#116128](https://github.com/elastic/elasticsearch/pull/116128)
* Feature: re-structure document ID generation favoring _id inverted index compression [#104683](https://github.com/elastic/elasticsearch/pull/104683)


Machine Learning
:   * Add DeBERTa-V2/V3 tokenizer [#111852](https://github.com/elastic/elasticsearch/pull/111852)
* Add special case for elastic reranker in inference API [#116962](https://github.com/elastic/elasticsearch/pull/116962)
* Adding inference endpoint validation for `AzureAiStudioService` [#113713](https://github.com/elastic/elasticsearch/pull/113713)
* Adds support for `input_type` field to Vertex inference service [#116431](https://github.com/elastic/elasticsearch/pull/116431)
* Enable built-in Inference Endpoints and default for Semantic Text [#116931](https://github.com/elastic/elasticsearch/pull/116931)
* Increase default `queue_capacity` to 10_000 and decrease max `queue_capacity` to 100_000 [#115041](https://github.com/elastic/elasticsearch/pull/115041)
* Inference duration and error metrics [#115876](https://github.com/elastic/elasticsearch/pull/115876)
* Remove all mentions of eis and gateway and deprecate flags that do [#116692](https://github.com/elastic/elasticsearch/pull/116692)
* [Inference API] Add API to get configuration of inference services [#114862](https://github.com/elastic/elasticsearch/pull/114862)
* [Inference API] Improve chunked results error message [#115807](https://github.com/elastic/elasticsearch/pull/115807)


Network
:   * Allow http unsafe buffers by default [#116115](https://github.com/elastic/elasticsearch/pull/116115)


Recovery
:   * Attempt to clean up index before remote transfer [#115142](https://github.com/elastic/elasticsearch/pull/115142) (issue: [#104473](https://github.com/elastic/elasticsearch/issues/104473))
* Trigger merges after recovery [#113102](https://github.com/elastic/elasticsearch/pull/113102)


Reindex
:   * Change Reindexing metrics unit from millis to seconds [#115721](https://github.com/elastic/elasticsearch/pull/115721)


Relevance
:   * Add query rules retriever [#114855](https://github.com/elastic/elasticsearch/pull/114855)
* Add tracking for query rule types [#116357](https://github.com/elastic/elasticsearch/pull/116357)


Search
:   * Add Search Phase APM metrics [#113194](https://github.com/elastic/elasticsearch/pull/113194)
* Add `docvalue_fields` Support for `dense_vector` Fields [#114484](https://github.com/elastic/elasticsearch/pull/114484) (issue: [#108470](https://github.com/elastic/elasticsearch/issues/108470))
* Add initial support for `semantic_text` field type [#113920](https://github.com/elastic/elasticsearch/pull/113920)
* Adds access to flags no_sub_matches and no_overlapping_matches to hyphenation-decompounder-tokenfilter [#115459](https://github.com/elastic/elasticsearch/pull/115459) (issue: [#97849](https://github.com/elastic/elasticsearch/issues/97849))
* Better sizing `BytesRef` for Strings in Queries [#115655](https://github.com/elastic/elasticsearch/pull/115655)
* Enable `_tier` based coordinator rewrites for all indices (not just mounted indices) [#115797](https://github.com/elastic/elasticsearch/pull/115797)
* Only aggregations require at least one shard request [#115314](https://github.com/elastic/elasticsearch/pull/115314)
* ESQL - Add match operator (:) [#116819](https://github.com/elastic/elasticsearch/pull/116819)
* Upgrade to Lucene 10 [#114741](https://github.com/elastic/elasticsearch/pull/114741)


Security
:   * Add refresh `.security` index call between security migrations [#114879](https://github.com/elastic/elasticsearch/pull/114879)


Snapshot/Restore
:   * Improve message about insecure S3 settings [#116915](https://github.com/elastic/elasticsearch/pull/116915)
* Retry `S3BlobContainer#getRegister` on all exceptions [#114813](https://github.com/elastic/elasticsearch/pull/114813)
* Split searchable snapshot into multiple repo operations [#116918](https://github.com/elastic/elasticsearch/pull/116918)
* Track shard snapshot progress during node shutdown [#112567](https://github.com/elastic/elasticsearch/pull/112567)


Vector Search
:   * Add support for bitwise inner-product in painless [#116082](https://github.com/elastic/elasticsearch/pull/116082)

### Fixes [elasticsearch-900-fixes]
Aggregations
:   * Handle with `illegalArgumentExceptions` negative values in HDR percentile aggregations [#116174](https://github.com/elastic/elasticsearch/pull/116174) (issue: [#115777](https://github.com/elastic/elasticsearch/issues/115777))


Analysis
:   * Adjust analyze limit exception to be a `bad_request` [#116325](https://github.com/elastic/elasticsearch/pull/116325)


CCS
:   * Fix long metric deserialize & add - auto-resize needs to be set manually [#117105](https://github.com/elastic/elasticsearch/pull/117105) (issue: [#116914](https://github.com/elastic/elasticsearch/issues/116914))


CRUD
:   * Preserve thread context when waiting for segment generation in RTG [#114623](https://github.com/elastic/elasticsearch/pull/114623)
* Standardize error code when bulk body is invalid [#114869](https://github.com/elastic/elasticsearch/pull/114869)


Data streams
:   * Add missing header in `put_data_lifecycle` rest-api-spec [#116292](https://github.com/elastic/elasticsearch/pull/116292)


EQL
:   * Don’t use a `BytesStreamOutput` to copy keys in `BytesRefBlockHash` [#114819](https://github.com/elastic/elasticsearch/pull/114819) (issue: [#114599](https://github.com/elastic/elasticsearch/issues/114599))


ES|QL
:   * Added stricter range type checks and runtime warnings for ENRICH [#115091](https://github.com/elastic/elasticsearch/pull/115091) (issues: [#107357](https://github.com/elastic/elasticsearch/issues/107357), [#116799](https://github.com/elastic/elasticsearch/issues/116799))
* Don’t return TEXT type for functions that take TEXT [#114334](https://github.com/elastic/elasticsearch/pull/114334) (issues: [#111537](https://github.com/elastic/elasticsearch/issues/111537), [#114333](https://github.com/elastic/elasticsearch/issues/114333))
* ESQL: Fix sorts containing `_source` [#116980](https://github.com/elastic/elasticsearch/pull/116980) (issue: [#116659](https://github.com/elastic/elasticsearch/issues/116659))
* ESQL: fix the column position in errors [#117153](https://github.com/elastic/elasticsearch/pull/117153)
* ES|QL: Fix stats by constant expression [#114899](https://github.com/elastic/elasticsearch/pull/114899)
* Fix NPE in `EnrichLookupService` on mixed clusters with <8.14 versions [#116583](https://github.com/elastic/elasticsearch/pull/116583) (issues: [#116529](https://github.com/elastic/elasticsearch/issues/116529), [#116544](https://github.com/elastic/elasticsearch/issues/116544))
* Fix TDigestState.read CB leaks [#114303](https://github.com/elastic/elasticsearch/pull/114303) (issue: [#114194](https://github.com/elastic/elasticsearch/issues/114194))
* Fixing remote ENRICH by pushing the Enrich inside `FragmentExec` [#114665](https://github.com/elastic/elasticsearch/pull/114665) (issue: [#105095](https://github.com/elastic/elasticsearch/issues/105095))
* Use `SearchStats` instead of field.isAggregatable in data node planning [#115744](https://github.com/elastic/elasticsearch/pull/115744) (issue: [#115737](https://github.com/elastic/elasticsearch/issues/115737))
* [ESQL] Fix Binary Comparisons on Date Nanos [#116346](https://github.com/elastic/elasticsearch/pull/116346)
* [ES|QL] To_DatePeriod and To_TimeDuration return better error messages on `union_type` fields [#114934](https://github.com/elastic/elasticsearch/pull/114934)


Infra/CLI
:   * Fix NPE on plugin sync [#115640](https://github.com/elastic/elasticsearch/pull/115640) (issue: [#114818](https://github.com/elastic/elasticsearch/issues/114818))


Infra/Metrics
:   * Make `randomInstantBetween` always return value in range [minInstant, `maxInstant]` [#114177](https://github.com/elastic/elasticsearch/pull/114177)


Infra/REST API
:   * Fixed a `NullPointerException` in `_capabilities` API when the `path` parameter is null. [#113413](https://github.com/elastic/elasticsearch/pull/113413) (issue: [#113413](https://github.com/elastic/elasticsearch/issues/113413))


Infra/Settings
:   * Don’t allow secure settings in YML config (109115) [#115779](https://github.com/elastic/elasticsearch/pull/115779) (issue: [#109115](https://github.com/elastic/elasticsearch/issues/109115))


Ingest Node
:   * Add warning headers for ingest pipelines containing special characters [#114837](https://github.com/elastic/elasticsearch/pull/114837) (issue: [#104411](https://github.com/elastic/elasticsearch/issues/104411))
* Reducing error-level stack trace logging for normal events in `GeoIpDownloader` [#114924](https://github.com/elastic/elasticsearch/pull/114924)


Logs
:   * Always check if index mode is logsdb [#116922](https://github.com/elastic/elasticsearch/pull/116922)
* Prohibit changes to index mode, source, and sort settings during resize [#115812](https://github.com/elastic/elasticsearch/pull/115812)


Machine Learning
:   * Fix bug in ML autoscaling when some node info is unavailable [#116650](https://github.com/elastic/elasticsearch/pull/116650)
* Fix deberta tokenizer bug caused by bug in normalizer [#117189](https://github.com/elastic/elasticsearch/pull/117189)
* Hides `hugging_face_elser` service from the `GET _inference/_services API` [#116664](https://github.com/elastic/elasticsearch/pull/116664) (issue: [#116644](https://github.com/elastic/elasticsearch/issues/116644))
* Mitigate IOSession timeouts [#115414](https://github.com/elastic/elasticsearch/pull/115414) (issues: [#114385](https://github.com/elastic/elasticsearch/issues/114385), [#114327](https://github.com/elastic/elasticsearch/issues/114327), [#114105](https://github.com/elastic/elasticsearch/issues/114105), [#114232](https://github.com/elastic/elasticsearch/issues/114232))
* Propagate scoring function through random sampler [#116957](https://github.com/elastic/elasticsearch/pull/116957) (issue: [#110134](https://github.com/elastic/elasticsearch/issues/110134))
* Update Deberta tokenizer [#116358](https://github.com/elastic/elasticsearch/pull/116358)
* Wait for up to 2 seconds for yellow status before starting search [#115938](https://github.com/elastic/elasticsearch/pull/115938) (issues: [#107777](https://github.com/elastic/elasticsearch/issues/107777), [#105955](https://github.com/elastic/elasticsearch/issues/105955), [#107815](https://github.com/elastic/elasticsearch/issues/107815), [#112191](https://github.com/elastic/elasticsearch/issues/112191))


Mapping
:   * Change synthetic source logic for `constant_keyword` [#117182](https://github.com/elastic/elasticsearch/pull/117182) (issue: [#117083](https://github.com/elastic/elasticsearch/issues/117083))
* Ignore conflicting fields during dynamic mapping update [#114227](https://github.com/elastic/elasticsearch/pull/114227) (issue: [#114228](https://github.com/elastic/elasticsearch/issues/114228))


Network
:   * Use underlying `ByteBuf` `refCount` for `ReleasableBytesReference` [#116211](https://github.com/elastic/elasticsearch/pull/116211)


Ranking
:   * Propagating nested `inner_hits` to the parent compound retriever [#116408](https://github.com/elastic/elasticsearch/pull/116408) (issue: [#116397](https://github.com/elastic/elasticsearch/issues/116397))


Relevance
:   * Fix handling of bulk requests with semantic text fields and delete ops [#116942](https://github.com/elastic/elasticsearch/pull/116942)


Search
:   * Catch and handle disconnect exceptions in search [#115836](https://github.com/elastic/elasticsearch/pull/115836)
* Fields caps does not honour ignore_unavailable [#116021](https://github.com/elastic/elasticsearch/pull/116021) (issue: [#107767](https://github.com/elastic/elasticsearch/issues/107767))
* Fix handling of time exceeded exception in fetch phase [#116676](https://github.com/elastic/elasticsearch/pull/116676)
* Fix leak in `DfsQueryPhase` and introduce search disconnect stress test [#116060](https://github.com/elastic/elasticsearch/pull/116060) (issue: [#115056](https://github.com/elastic/elasticsearch/issues/115056))
* Inconsistency in the _analyzer api when the index is not included [#115930](https://github.com/elastic/elasticsearch/pull/115930)
* Semantic text simple partial update [#116478](https://github.com/elastic/elasticsearch/pull/116478)
* Updated Date Range to Follow Documentation When Assuming Missing Values [#112258](https://github.com/elastic/elasticsearch/pull/112258) (issue: [#111484](https://github.com/elastic/elasticsearch/issues/111484))
* Validate missing shards after the coordinator rewrite [#116382](https://github.com/elastic/elasticsearch/pull/116382)
* _validate does not honour ignore_unavailable [#116656](https://github.com/elastic/elasticsearch/pull/116656) (issue: [#116594](https://github.com/elastic/elasticsearch/issues/116594))


Snapshot/Restore
:   * Retry throttled snapshot deletions [#113237](https://github.com/elastic/elasticsearch/pull/113237)


Vector Search
:   * Update Semantic Query To Handle Zero Size Responses [#116277](https://github.com/elastic/elasticsearch/pull/116277) (issue: [#116083](https://github.com/elastic/elasticsearch/issues/116083))


Watcher
:   * Watch Next Run Interval Resets On Shard Move or Node Restart [#115102](https://github.com/elastic/elasticsearch/pull/115102) (issue: [#111433](https://github.com/elastic/elasticsearch/issues/111433))



## Deprecations [deprecation-9.0.0]

Ingest Node
:   * Fix `_type` deprecation on simulate pipeline API [#116259](https://github.com/elastic/elasticsearch/pull/116259)


Machine Learning
:   * [Inference API] Deprecate elser service [#113216](https://github.com/elastic/elasticsearch/pull/113216)


Mapping
:   * Deprecate `_source.mode` in mappings [#116689](https://github.com/elastic/elasticsearch/pull/116689)