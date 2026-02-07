## 9.3.0 [elasticsearch-release-notes-9.3.0]
_[Known issues](/release-notes/known-issues.md#elasticsearch-9.3.0-known-issues) and [Breaking changes](/release-notes/breaking-changes.md#elasticsearch-9.3.0-breaking-changes) and [Deprecations](/release-notes/deprecations.md#elasticsearch-9.3.0-deprecations)._

### Features and enhancements [elasticsearch-9.3.0-features-enhancements]
* New Cloud Connect UI for self-managed installations. 
  Adds Cloud Connect functionality to Kibana, which allows you to use cloud solutions like AutoOps and Elastic Inference Service in your self-managed Elasticsearch clusters.
* Simulate shards moved by explicit commands. [#136066](https://github.com/elastic/elasticsearch/pull/136066) 
* Iterate directly over contents of `RoutingNode`. [#137694](https://github.com/elastic/elasticsearch/pull/137694) 
* Allocation: add duration and count metrics for write load hotspot. [#138465](https://github.com/elastic/elasticsearch/pull/138465) 
* Reapply "Track shardStarted events for simulation in DesiredBalanceComputer". [#135597](https://github.com/elastic/elasticsearch/pull/135597) 
* Add a new setting for s3 API call timeout. [#138072](https://github.com/elastic/elasticsearch/pull/138072) 
* Limit concurrent TLS handshakes. [#136386](https://github.com/elastic/elasticsearch/pull/136386) 
* S3 compareAndExchange using conditional writes. [#139228](https://github.com/elastic/elasticsearch/pull/139228) 
* Use common retry logic for GCS. [#138553](https://github.com/elastic/elasticsearch/pull/138553) 
* Add `ThreadWatchdog` to `ClusterApplierService`. [#134361](https://github.com/elastic/elasticsearch/pull/134361) 
* Disk usage don't include synthetic _id postings. [#138745](https://github.com/elastic/elasticsearch/pull/138745) 
* Shard started reroute high priority. [#137306](https://github.com/elastic/elasticsearch/pull/137306) 
* Allocation: add balancer round summary as metrics. [#136043](https://github.com/elastic/elasticsearch/pull/136043) 
* Allow fast blob-cache introspection by shard-id. [#138282](https://github.com/elastic/elasticsearch/pull/138282) 
* Report recent tasks updates when master starved. [#139518](https://github.com/elastic/elasticsearch/pull/139518) 
* TransportGetBasicStatusAction runs on local. [#137567](https://github.com/elastic/elasticsearch/pull/137567) 

**Aggregations**
* Let terms queries rewrite to a filter on constant_keyword fields. [#139106](https://github.com/elastic/elasticsearch/pull/139106) 

**Authentication**
* Add in-response-to field to saml successful response. [#137599](https://github.com/elastic/elasticsearch/pull/137599) [#128179](https://github.com/elastic/elasticsearch/issues/128179) 
* Improve SAML error handling by adding metadata. [#137598](https://github.com/elastic/elasticsearch/pull/137598) [#128179](https://github.com/elastic/elasticsearch/issues/128179) 
* Additional DEBUG logging on authc failures. [#137941](https://github.com/elastic/elasticsearch/pull/137941) 

**Authorization**
* Add additional privileges to Kibana System role for `.endpoint-scripts-file*` indexes. [#139245](https://github.com/elastic/elasticsearch/pull/139245) 
* Add privileges to Kibana System role for management of internal indexes in support of Elastic Defend features. [#138993](https://github.com/elastic/elasticsearch/pull/138993) 

**CCS**
* CPS usage telemetry support. [#137705](https://github.com/elastic/elasticsearch/pull/137705) 
* CPS: Enable flatworld search and `project_routing` for `_msearch`. [#138822](https://github.com/elastic/elasticsearch/pull/138822) 
* Do not assume we hear back from all linked projects when validating resolved index expressions for CPS. [#137916](https://github.com/elastic/elasticsearch/pull/137916) 
* MRT should default to `true` for CPS searches. [#138105](https://github.com/elastic/elasticsearch/pull/138105) 
* Add support for `project_routing` for `_search` and `_async_search`. [#137566](https://github.com/elastic/elasticsearch/pull/137566) 

**Codec**
* Remove feature flag to enable binary doc value compression. [#138524](https://github.com/elastic/elasticsearch/pull/138524) 
* Simple bulk loading of compressed binary doc values. [#138541](https://github.com/elastic/elasticsearch/pull/138541) 
* Enable large numeric blocks for TSDB codec in production. [#139503](https://github.com/elastic/elasticsearch/pull/139503) 
* Improved bulk loading for binary doc values. [#138631](https://github.com/elastic/elasticsearch/pull/138631) 
* Integrate stored fields format bloom filter with synthetic _id. [#138515](https://github.com/elastic/elasticsearch/pull/138515) 

**Data streams**
* Adding `match_only_text` subfield to `*.display_name` fields. [#136265](https://github.com/elastic/elasticsearch/pull/136265) 
* Support choosing the downsampling method in data stream lifecycle. [#137023](https://github.com/elastic/elasticsearch/pull/137023) 

**Downsampling**
* Add new sampling method to the Downsample API. [#136813](https://github.com/elastic/elasticsearch/pull/136813) 

**EQL**
* EQL: enable CPS. [#137833](https://github.com/elastic/elasticsearch/pull/137833) 
* EQL: accept project_routing as query parameter. [#138559](https://github.com/elastic/elasticsearch/pull/138559) 

**ES|QL**
* ESQL: introduce support for mapping-unavailable fields (Fork from #139417). [#140463](https://github.com/elastic/elasticsearch/pull/140463) 
* ES|QL: Push down COUNT(*) BY DATE_TRUNC. [#138023](https://github.com/elastic/elasticsearch/pull/138023) 
* Enable the TEXT_EMBEDDING function in non-snapshot build. [#136103](https://github.com/elastic/elasticsearch/pull/136103) 
* ESQL: Timezone support in DATE_TRUNC, BUCKET and TBUCKET. [#137450](https://github.com/elastic/elasticsearch/pull/137450) 
* ESQL: Fuse MV_MIN and MV_MAX and document process. [#138029](https://github.com/elastic/elasticsearch/pull/138029) 
* ESQL: DateDiff timezone support. [#138316](https://github.com/elastic/elasticsearch/pull/138316) 
* ES|QL: Release decay function. [#137830](https://github.com/elastic/elasticsearch/pull/137830) 
* ES|QL: Late materialization after TopN (Node level). [#132757](https://github.com/elastic/elasticsearch/pull/132757) 
* ESQL: Add time_zone request param support to KQL and QSTR functions. [#138695](https://github.com/elastic/elasticsearch/pull/138695) 
* Feature/count by trunc with filter. [#138765](https://github.com/elastic/elasticsearch/pull/138765) 
* ES|QL: Enable score function in release builds. [#136988](https://github.com/elastic/elasticsearch/pull/136988) 
* ESQL: Enable nullify and fail unmapped resolution in tech-preview. [#140528](https://github.com/elastic/elasticsearch/pull/140528) 
* Run aggregations on aggregate metric double with default metric. [#138647](https://github.com/elastic/elasticsearch/pull/138647) [#136297](https://github.com/elastic/elasticsearch/issues/136297) 
* ES|QL: Improve value loading for match_only_text mapping. [#137026](https://github.com/elastic/elasticsearch/pull/137026) 
* ESQL: Push filters past inline stats. [#137572](https://github.com/elastic/elasticsearch/pull/137572) 
* ES|QL: Let include_execution_metadata always return data, also in local only. [#137641](https://github.com/elastic/elasticsearch/pull/137641) 
* Introduce usage limits for COMPLETION and RERANK. [#139074](https://github.com/elastic/elasticsearch/pull/139074) 
* ESQL: Locale and timezone argument for date_parse. [#136548](https://github.com/elastic/elasticsearch/pull/136548) [#132487](https://github.com/elastic/elasticsearch/issues/132487) 
* ESQL: Fill in topn values if competitive. [#135734](https://github.com/elastic/elasticsearch/pull/135734) 
* Add TOP_SNIPPETS function to return the best snippets for a field. [#138940](https://github.com/elastic/elasticsearch/pull/138940) 
* Take TOP_SNIPPETS out of snapshot. [#139272](https://github.com/elastic/elasticsearch/pull/139272) 
* Non-Correlated Subquery in FROM command. [#135744](https://github.com/elastic/elasticsearch/pull/135744) 
* ESQL: Implement `network_direction` function. [#136133](https://github.com/elastic/elasticsearch/pull/136133) 
* ES|QL - Remove vectors from _source when applicable. [#138013](https://github.com/elastic/elasticsearch/pull/138013) 
* Inference command: support for CCS. [#139244](https://github.com/elastic/elasticsearch/pull/139244) [#136860](https://github.com/elastic/elasticsearch/issues/136860) 
* ESQL: Add support for Full Text Functions and Lucene Pushable Predicates for Lookup Join. [#136104](https://github.com/elastic/elasticsearch/pull/136104) 
* Add optional parameters support to KQL function. [#135895](https://github.com/elastic/elasticsearch/pull/135895) [#135823](https://github.com/elastic/elasticsearch/issues/135823) 
* ESQL: introduce a new interface to declare functions depending on the `@timestamp` attribute. [#137040](https://github.com/elastic/elasticsearch/pull/137040) [#136772](https://github.com/elastic/elasticsearch/issues/136772) 
* Release CHUNK function as tech preview. [#138621](https://github.com/elastic/elasticsearch/pull/138621) 
* ESQL: Multiple patterns for grok command. [#136541](https://github.com/elastic/elasticsearch/pull/136541) [#132486](https://github.com/elastic/elasticsearch/issues/132486) 
* ES|QL Update CHUNK to support chunking_settings as optional argument. [#138123](https://github.com/elastic/elasticsearch/pull/138123) 
* Support extra output field in TOP function. [#135434](https://github.com/elastic/elasticsearch/pull/135434) [#128630](https://github.com/elastic/elasticsearch/issues/128630) 
* Support window functions in time-series aggregations. [#138139](https://github.com/elastic/elasticsearch/pull/138139) 
* ESQL: Fix slowness in ValuesFromManyReader.estimatedRamBytesUsed. [#139397](https://github.com/elastic/elasticsearch/pull/139397) 
* Add CHUNK function. [#134320](https://github.com/elastic/elasticsearch/pull/134320) 
* ES|QL: Add MV_INTERSECTION Function. [#139379](https://github.com/elastic/elasticsearch/pull/139379) 
* Enable new exponential histograms field type. [#138968](https://github.com/elastic/elasticsearch/pull/138968) 
* Enable CCS tests for subqueries. [#137776](https://github.com/elastic/elasticsearch/pull/137776) 
* ESQL: Pull `OrderBy` followed by `InlineJoin` on top of it. [#137648](https://github.com/elastic/elasticsearch/pull/137648) 
* ES|QL - Avoid retrieving unnecessary fields on node-reduce phase. [#137920](https://github.com/elastic/elasticsearch/pull/137920) [#134363](https://github.com/elastic/elasticsearch/issues/134363) 
* Fix a validation message in TimeSeriesGroupByAll. [#139882](https://github.com/elastic/elasticsearch/pull/139882) 
* Add `m` alias for `minute` duration literal. [#136448](https://github.com/elastic/elasticsearch/pull/136448) [#135552](https://github.com/elastic/elasticsearch/issues/135552) 
* ES|QL: support for parameters in LIKE and RLIKE. [#138051](https://github.com/elastic/elasticsearch/pull/138051) 
* ESQL - Add planning detailed timing to profile information. [#138564](https://github.com/elastic/elasticsearch/pull/138564) 
* Allow single fork branch. [#136805](https://github.com/elastic/elasticsearch/pull/136805) [#135825](https://github.com/elastic/elasticsearch/issues/135825) 
* ES|QL: Release CCS support for FORK. [#139630](https://github.com/elastic/elasticsearch/pull/139630) 
* ESQL: Make field fusion generic. [#137382](https://github.com/elastic/elasticsearch/pull/137382) 
* ES|QL - Add vector similarity functions to tech preview, allow pushdown of blockloader functions. [#139365](https://github.com/elastic/elasticsearch/pull/139365) 
* ES|QL - KNN function option changes. [#138372](https://github.com/elastic/elasticsearch/pull/138372) 
* Enable TDigest field mapper and ES|QL type. [#139607](https://github.com/elastic/elasticsearch/pull/139607) 
* Release histogram data type. [#139703](https://github.com/elastic/elasticsearch/pull/139703) 
* ES|QL completion command constant folding. [#138112](https://github.com/elastic/elasticsearch/pull/138112) [#136863](https://github.com/elastic/elasticsearch/issues/136863) 

**Engine**
* Improve SingleValueMatchQuery performance. [#135714](https://github.com/elastic/elasticsearch/pull/135714) 
* BlockSourceReader should always apply source filtering. [#136438](https://github.com/elastic/elasticsearch/pull/136438) 
* Further simplify SingleValueMatchQuery. [#136195](https://github.com/elastic/elasticsearch/pull/136195) 

**Geo**
* Optimize geogrid functions to read points from doc-values. [#138917](https://github.com/elastic/elasticsearch/pull/138917) 

**Health**
* Add settings for health indicator shard_capacity thresholds. [#136141](https://github.com/elastic/elasticsearch/pull/136141) [#116697](https://github.com/elastic/elasticsearch/issues/116697) 
* Deterministic shard availability key order. [#138260](https://github.com/elastic/elasticsearch/pull/138260) [#138043](https://github.com/elastic/elasticsearch/issues/138043) 

**Indices APIs**
* Add convenience API key param to remote reindex. [#135949](https://github.com/elastic/elasticsearch/pull/135949) 
* Add small optimizations to `PUT _component_template` API. [#135644](https://github.com/elastic/elasticsearch/pull/135644) 

**Infra/Plugins**
* Add upgrade.rollbacks mapping to .fleet-agents system index. [#139363](https://github.com/elastic/elasticsearch/pull/139363) [#6039](https://github.com/elastic/fleet-server/issues/6039) 

**Infra/REST API**
* Cat API: added endpoint for Circuit Breakers. [#136890](https://github.com/elastic/elasticsearch/pull/136890) 

**Infra/Scripting**
* Bump mustache.java to 0.9.14. [#138923](https://github.com/elastic/elasticsearch/pull/138923) 

**Ingest node**
* Bump jruby/joni to 2.2.6. [#139075](https://github.com/elastic/elasticsearch/pull/139075) 
* Add CEF processor to Ingest node. [#122491](https://github.com/elastic/elasticsearch/pull/122491) 

**Machine learning**
* Implementing the completion task type on EIS. [#137677](https://github.com/elastic/elasticsearch/pull/137677) 
* Transition EIS auth polling to persistent task on a single node. [#136713](https://github.com/elastic/elasticsearch/pull/136713) 
* Add NVIDIA support to Inference Plugin. [#132388](https://github.com/elastic/elasticsearch/pull/132388) 
* Add Azure OpenAI chat completion support. [#138726](https://github.com/elastic/elasticsearch/pull/138726) 
* Add "close_job" parameter to the stop datafeed API. [#138634](https://github.com/elastic/elasticsearch/pull/138634) [#138010](https://github.com/elastic/elasticsearch/issues/138010) 
* Add Embedding inference task type. [#138198](https://github.com/elastic/elasticsearch/pull/138198) 
* Add daily task to manage .ml-state indices. [#137653](https://github.com/elastic/elasticsearch/pull/137653) 
* Add cached tokens to Unified API response. [#136412](https://github.com/elastic/elasticsearch/pull/136412) 
* Add Groq inference service. [#138251](https://github.com/elastic/elasticsearch/pull/138251) 
* Add late chunking configuration for JinaAI embedding task settings. [#137263](https://github.com/elastic/elasticsearch/pull/137263) 
* Implement OpenShift AI integration for chat completion, embeddings, and reranking. [#136624](https://github.com/elastic/elasticsearch/pull/136624) 
* Add Google Model Garden's Meta, Mistral, Hugging Face and Ai21 providers support to Inference Plugin. [#135701](https://github.com/elastic/elasticsearch/pull/135701) 
* Manage AD results indices. [#136065](https://github.com/elastic/elasticsearch/pull/136065) 
* Require basic licence for the Elastic Inference Service. [#137434](https://github.com/elastic/elasticsearch/pull/137434) 

**Mapping**
* Simple bulk loading for binary doc values. [#137860](https://github.com/elastic/elasticsearch/pull/137860) 
* AggregateMetricDouble fields should not build BKD indexes. [#138724](https://github.com/elastic/elasticsearch/pull/138724) 
* Enable doc_values skippers. [#138723](https://github.com/elastic/elasticsearch/pull/138723) 
  Doc_values skippers add a sparse index to doc_values fields, allowing efficient querying and filtering on a field without having to build a separate BKD or terms index.  These are now enabled automatically on any field configured with  and  if the index setting  is set to  (default , or  for TSDB indexes). TSDB indexes now default to using skippers in place of indexes for their , , and   fields, greatly reducing their on-disk footprint.  To disable skippers in TSDB indexes, set  to .
* Add index.mapping.nested_parents.limit and raise nested fields limit to 100. [#138961](https://github.com/elastic/elasticsearch/pull/138961) 
* Improve no-op check in PUT _mapping API. [#138367](https://github.com/elastic/elasticsearch/pull/138367) 
* Optionally ignore field when indexed field name exceeds length limit. [#136143](https://github.com/elastic/elasticsearch/pull/136143) [#135700](https://github.com/elastic/elasticsearch/issues/135700) 
* OTLP: store units in mappings. [#134709](https://github.com/elastic/elasticsearch/pull/134709) 
* Improve block loader for source only runtime geo_point fields. [#135883](https://github.com/elastic/elasticsearch/pull/135883) 
* Improve bulk loading of binary doc values. [#137995](https://github.com/elastic/elasticsearch/pull/137995) 
* Use binary doc values for pattern_text args column. [#139466](https://github.com/elastic/elasticsearch/pull/139466) 
* Use existing `DocumentMapper` when creating new `MapperService`. [#138489](https://github.com/elastic/elasticsearch/pull/138489) 

**Monitoring**
* Add missing fields in monitoring logstash mb template mapping. [#127053](https://github.com/elastic/elasticsearch/pull/127053) 

**Relevance**
* Semantic search CCS support when ccs_minimize_roundtrips=false. [#138982](https://github.com/elastic/elasticsearch/pull/138982) 
* Feat: re-enable bfloat16 in semantic text. [#139347](https://github.com/elastic/elasticsearch/pull/139347) 
*   Default semantic_text fields to ELSER on EIS when available. [#134708](https://github.com/elastic/elasticsearch/pull/134708) 
* Adds Internal mechanisms and retriever for MMR based result diversification. [#135880](https://github.com/elastic/elasticsearch/pull/135880) 
* Allow updating inference_id of semantic_text fields. [#136120](https://github.com/elastic/elasticsearch/pull/136120) 
* Add chunk_rescorer usage to output of explain and profile for text_similarity_rank_retriever. [#137249](https://github.com/elastic/elasticsearch/pull/137249) 

**SLM**
* Move force merge from the downsmapling request to the ILM action. [#135834](https://github.com/elastic/elasticsearch/pull/135834) 
* Support different downsampling methods through ILM. [#136951](https://github.com/elastic/elasticsearch/pull/136951) 

**SQL**
* SQL: Add project routing support to JDBC. [#138756](https://github.com/elastic/elasticsearch/pull/138756) 
* SQL: enable CPS. [#138803](https://github.com/elastic/elasticsearch/pull/138803) 
* SQL: add project_routing option. [#138718](https://github.com/elastic/elasticsearch/pull/138718) 

**Search**
* Speed up sorts on secondary sort fields. [#137533](https://github.com/elastic/elasticsearch/pull/137533) 
* Can match search shard phase APM metric. [#136646](https://github.com/elastic/elasticsearch/pull/136646) 
* Field caps to support project_routing also in the body of the request. [#138681](https://github.com/elastic/elasticsearch/pull/138681) 
* Add time range bucketing attribute to APM took time latency metrics. [#135549](https://github.com/elastic/elasticsearch/pull/135549) 
* Improve PIT context relocation. [#135231](https://github.com/elastic/elasticsearch/pull/135231) 
* Can match phase coordinator duration APM metric . [#136828](https://github.com/elastic/elasticsearch/pull/136828) 
* Coordinator phase duration APM metric attributes. [#137409](https://github.com/elastic/elasticsearch/pull/137409) 
* Field caps transport changes to return for each original expression what it was resolved to. [#136632](https://github.com/elastic/elasticsearch/pull/136632) 
* Use DV rewrites where possible in Keyword queries. [#137536](https://github.com/elastic/elasticsearch/pull/137536) 
* Allows PIT to be cross project. [#137966](https://github.com/elastic/elasticsearch/pull/137966) 
* Extend time range bucketing attributes to retrievers. [#136072](https://github.com/elastic/elasticsearch/pull/136072) 
* Fetch search phase coordinator duration APM metric. [#136547](https://github.com/elastic/elasticsearch/pull/136547) 
* Dfs query phase coordinator metric. [#136481](https://github.com/elastic/elasticsearch/pull/136481) 
* Allows Cross Project for close PointInTime. [#138962](https://github.com/elastic/elasticsearch/pull/138962) 
* Allows field caps to be cross project. [#137530](https://github.com/elastic/elasticsearch/pull/137530) 

**Security**
* Add audit log testing for cert-based cross-cluster authentication. [#137302](https://github.com/elastic/elasticsearch/pull/137302) 
* Add periodic PKC JWK set reloading capability to JWT realm. [#136996](https://github.com/elastic/elasticsearch/pull/136996) 
* Include Secure Setting Names and Keystore Modified Time in Reload API Response. [#138052](https://github.com/elastic/elasticsearch/pull/138052) 
* Validate Certificate Identity provided in Cross Cluster API Key Certificate. [#136299](https://github.com/elastic/elasticsearch/pull/136299) 
* Send cross cluster api key signature as header. [#135674](https://github.com/elastic/elasticsearch/pull/135674) 
* Adds certificate identity field to cross-cluster API keys. [#134604](https://github.com/elastic/elasticsearch/pull/134604) 

**TSDB**
* ES|QL: Add TRANGE ES|QL function. [#136441](https://github.com/elastic/elasticsearch/pull/136441) [#135599](https://github.com/elastic/elasticsearch/issues/135599) 
* ESQL: Group by all optimization. [#139130](https://github.com/elastic/elasticsearch/pull/139130) 
* ESQL: GROUP BY ALL with the dimensions output. [#138595](https://github.com/elastic/elasticsearch/pull/138595) 
* Minimize doc values fetches in TSDBSyntheticIdFieldsProducer. [#139053](https://github.com/elastic/elasticsearch/pull/139053) 
* Add ES93BloomFilterStoredFieldsFormat for efficient field existence checks. [#137331](https://github.com/elastic/elasticsearch/pull/137331) 
* Use a new synthetic _id format for time-series datastreams. [#137274](https://github.com/elastic/elasticsearch/pull/137274) 
* Use doc values skipper for _tsid in synthetic _id postings. [#138568](https://github.com/elastic/elasticsearch/pull/138568) 
* ESQL: GROUP BY ALL. [#137367](https://github.com/elastic/elasticsearch/pull/137367) 
* Add TDigest histogram as metric. [#139247](https://github.com/elastic/elasticsearch/pull/139247) 
* Use doc values skipper for @timestamp in synthetic _id postings #138568. [#138876](https://github.com/elastic/elasticsearch/pull/138876) 
* Late materialization of dimension fields in time-series. [#135961](https://github.com/elastic/elasticsearch/pull/135961) 
* Add support for merges in ES93BloomFilterStoredFieldsFormat. [#137622](https://github.com/elastic/elasticsearch/pull/137622) 

**Transform**
* Preview index request. [#137455](https://github.com/elastic/elasticsearch/pull/137455) 

**Vector search**
* Add on-disk rescoring to disk BBQ. [#135778](https://github.com/elastic/elasticsearch/pull/135778) 
* Add bfloat16 support to rank_vectors. [#139463](https://github.com/elastic/elasticsearch/pull/139463) 
* Enable bfloat16 and on-disk rescoring for dense vectors. [#138492](https://github.com/elastic/elasticsearch/pull/138492) 
* Use new bulk scoring dot product for max inner product. [#139409](https://github.com/elastic/elasticsearch/pull/139409) 
* Allow semantic_text fields to use optional GPU indexing for HNSW and int8_hnsw. [#138999](https://github.com/elastic/elasticsearch/pull/138999) 
* Add DirectIO bulk rescoring. [#135380](https://github.com/elastic/elasticsearch/pull/135380) 
* Optimized native bulk dot product scoring for Int7. [#139069](https://github.com/elastic/elasticsearch/pull/139069) 
* Support for centroid filtering for restrictive filters. [#137959](https://github.com/elastic/elasticsearch/pull/137959) 
* Enable early termination for HNSW by default. [#130564](https://github.com/elastic/elasticsearch/pull/130564) 
* GPU: Restrict GPU indexing to FLOAT element types. [#139084](https://github.com/elastic/elasticsearch/pull/139084) 
* Optimized native bulk dot product scoring for Int7. [#138552](https://github.com/elastic/elasticsearch/pull/138552) 
* Use the new merge executor for intra-merge parallelism. [#137853](https://github.com/elastic/elasticsearch/pull/137853) 
* Remove gpu_vectors_indexing feature flag. [#139318](https://github.com/elastic/elasticsearch/pull/139318) 
* Introduce an adaptive HNSW Patience collector. [#138685](https://github.com/elastic/elasticsearch/pull/138685) 
* Adding base64 indexing for vector values. [#137072](https://github.com/elastic/elasticsearch/pull/137072) 

### Fixes [elasticsearch-9.3.0-fixes]
* Fix Decision.Type serialization BWC. [#140199](https://github.com/elastic/elasticsearch/pull/140199) 
* Introduce INDEX_SHARD_COUNT_FORMAT. [#137210](https://github.com/elastic/elasticsearch/pull/137210) 
* Always prefer YES over NOT_PREFERRED when allocating unassigned shards. [#138464](https://github.com/elastic/elasticsearch/pull/138464) 
* Disable _delete_by_query and _update_by_query for CCS/stateful. [#140301](https://github.com/elastic/elasticsearch/pull/140301) 
* Avoiding creating DataStreamShardStats objects with negative timestamps. [#139854](https://github.com/elastic/elasticsearch/pull/139854) 
* Support weaker consistency model for S3 MPUs. [#138663](https://github.com/elastic/elasticsearch/pull/138663) 
* Ignore abort-on-cleanup failure in S3 repo. [#138569](https://github.com/elastic/elasticsearch/pull/138569) 
* Suppress Azure SDK error logs. [#139729](https://github.com/elastic/elasticsearch/pull/139729) 
* Overall Decision for Deciders prioritizes THROTTLE. [#140237](https://github.com/elastic/elasticsearch/pull/140237) 
* Prevent NPE when generating snapshot metrics before initial cluster state is set. [#136350](https://github.com/elastic/elasticsearch/pull/136350) 
* Allow relocation to NOT_PREFERRED node for evacuating shards. [#140197](https://github.com/elastic/elasticsearch/pull/140197) 

**Aggregations**
* Fix SearchContext CB memory accounting. [#138002](https://github.com/elastic/elasticsearch/pull/138002) 

**CCS**
* Set CPS index options only when not using PIT. [#137728](https://github.com/elastic/elasticsearch/pull/137728) 

**Codec**
* Binary doc values have stale value offset array if block contains all empty values. [#139922](https://github.com/elastic/elasticsearch/pull/139922) 

**Data streams**
* Exempt internal request markers from streams request param restrictions. [#139386](https://github.com/elastic/elasticsearch/pull/139386) [#139367](https://github.com/elastic/elasticsearch/issues/139367) 

**Downsampling**
* Fix bug when downsampling exponential histograms with last value. [#139808](https://github.com/elastic/elasticsearch/pull/139808) 

**EQL**
* EQL: fix project_routing. [#139366](https://github.com/elastic/elasticsearch/pull/139366) 

**ES|QL**
* Fix interpolation for data points at bucket boundaries. [#139798](https://github.com/elastic/elasticsearch/pull/139798) [#139732](https://github.com/elastic/elasticsearch/issues/139732) 
* Addressing vector similarity concurrency issue with byte vectors. [#137883](https://github.com/elastic/elasticsearch/pull/137883) [#137625](https://github.com/elastic/elasticsearch/issues/137625) 
* TS Disallow renaming into timestamp prior to implicit use. [#137713](https://github.com/elastic/elasticsearch/pull/137713) [#137655](https://github.com/elastic/elasticsearch/issues/137655) 
* Use sub keyword block loader with ignore_above for text fields. [#140622](https://github.com/elastic/elasticsearch/pull/140622) 
* ES|QL: fix Page.equals(). [#136266](https://github.com/elastic/elasticsearch/pull/136266) 
* ES|QL: Fix wrong pruning of plans with no output columns. [#133405](https://github.com/elastic/elasticsearch/pull/133405) 
* ES|QL: Validate multiple GROK patterns individually. [#137082](https://github.com/elastic/elasticsearch/pull/137082) 
* Support date trunc in TS. [#138947](https://github.com/elastic/elasticsearch/pull/138947) 
* ESQL: Prune InlineJoin right aggregations by delegating to the child plan. [#139357](https://github.com/elastic/elasticsearch/pull/139357) [#138283](https://github.com/elastic/elasticsearch/issues/138283) 
* ESQL: Fix extent reading when missing. [#140034](https://github.com/elastic/elasticsearch/pull/140034) 
* ESQL: Aggressively free topn. [#140126](https://github.com/elastic/elasticsearch/pull/140126) 
* Pushing down eval expression when it requires data access . [#136610](https://github.com/elastic/elasticsearch/pull/136610) [#133462](https://github.com/elastic/elasticsearch/issues/133462) 
* ES|QL: manage INLINE STATS count(*) on result sets with no columns. [#137017](https://github.com/elastic/elasticsearch/pull/137017) 
* ES|QL: Change FUSE KEY BY to receive a list of qualifiedName. [#139071](https://github.com/elastic/elasticsearch/pull/139071) 
* Do not use Min or Max as Top's surrogate when there is an outputField. [#138380](https://github.com/elastic/elasticsearch/pull/138380) [#134083](https://github.com/elastic/elasticsearch/issues/134083) 
* Fixing bug when handling 1d literal vectors. [#136891](https://github.com/elastic/elasticsearch/pull/136891) [#136364](https://github.com/elastic/elasticsearch/issues/136364) 
* Catch-and-rethrow TooComplexToDeterminizeException . [#137024](https://github.com/elastic/elasticsearch/pull/137024) 
* No EsqlIllegalArgumentException for invalid window values. [#139470](https://github.com/elastic/elasticsearch/pull/139470) 
* Do not skip a remote cluster base on the query's execution time status. [#138332](https://github.com/elastic/elasticsearch/pull/138332) 
* Prune columns when using fork. [#137907](https://github.com/elastic/elasticsearch/pull/137907) [#136365](https://github.com/elastic/elasticsearch/issues/136365) 
* ESQL: Fix attribute only in full text function not found. [#137395](https://github.com/elastic/elasticsearch/pull/137395) [#137396](https://github.com/elastic/elasticsearch/issues/137396) 
* ES|QL - fix dense vector enrich bug. [#139774](https://github.com/elastic/elasticsearch/pull/139774) [#137699](https://github.com/elastic/elasticsearch/issues/137699) 
* ES|QL: support dot and parameters in FUSE GROUP BY. [#135901](https://github.com/elastic/elasticsearch/pull/135901) 
* Don't allow MV_EXPAND prior to STATS with TS. [#136931](https://github.com/elastic/elasticsearch/pull/136931) [#136928](https://github.com/elastic/elasticsearch/issues/136928) 
* ES|QL: Fix aggregation on null value. [#139797](https://github.com/elastic/elasticsearch/pull/139797) [#110257](https://github.com/elastic/elasticsearch/issues/110257) [#137544](https://github.com/elastic/elasticsearch/issues/137544) 
* ESQL: Fix metrics for took between 1 and 10 hours. [#139257](https://github.com/elastic/elasticsearch/pull/139257) 

**Engine**
* Fixes memory leak in BytesRefLongBlockHash. [#137050](https://github.com/elastic/elasticsearch/pull/137050) [#137021](https://github.com/elastic/elasticsearch/issues/137021) 

**Indices APIs**
* Don't fail delete index API if an index is deleted during the request. [#138015](https://github.com/elastic/elasticsearch/pull/138015) [#137422](https://github.com/elastic/elasticsearch/issues/137422) 

**Inference**
* Include rerank in supported tasks for IBM watsonx integration. [#140331](https://github.com/elastic/elasticsearch/pull/140331) [#140328](https://github.com/elastic/elasticsearch/issues/140328) 

**Ingest node**
* Respect flexible field access pattern in geoip and ip_location processors. [#138728](https://github.com/elastic/elasticsearch/pull/138728) 

**Machine learning**
* Support chunking settings for sparse embeddings in custom service. [#138776](https://github.com/elastic/elasticsearch/pull/138776) 
* Retry state save indefinitely. [#139668](https://github.com/elastic/elasticsearch/pull/139668) 
* Add missing job_id filter to Anomaly Detection data deleter. [#138160](https://github.com/elastic/elasticsearch/pull/138160) 
* Switch TextExpansionQueryBuilder and TextEmbeddingQueryVectorBuilder to return 400 instead of 500 errors . [#135800](https://github.com/elastic/elasticsearch/pull/135800) 
* Fixing KDE evaluate() to return correct ValueAndMagnitude object. [#128602](https://github.com/elastic/elasticsearch/pull/128602) [#127517](https://github.com/elastic/elasticsearch/issues/127517) 
* Skip dataframes when disabled. [#137220](https://github.com/elastic/elasticsearch/pull/137220) 
* Remove worst-case additional 50ms latency for non-rate limited requests. [#136167](https://github.com/elastic/elasticsearch/pull/136167) 
* Add configurable max_batch_size for GoogleVertexAI embedding service settings. [#138047](https://github.com/elastic/elasticsearch/pull/138047) 
* Add ElasticInferenceServiceDenseTextEmbeddingsServiceSettings to InferenceNamedWriteablesProvider. [#138484](https://github.com/elastic/elasticsearch/pull/138484) 
* Preserve deployments with zero allocations during assignment planning. [#137244](https://github.com/elastic/elasticsearch/pull/137244) [#137134](https://github.com/elastic/elasticsearch/issues/137134) 
* Reject max_number_of_allocations > 1 for low-priority model deployments. [#140163](https://github.com/elastic/elasticsearch/pull/140163) [#111227](https://github.com/elastic/elasticsearch/issues/111227) 

**Mapping**
* Fix index.mapping.use_doc_values_skippers defaults in serverless. [#139526](https://github.com/elastic/elasticsearch/pull/139526) 
* Fixed inconsistency in the isSyntheticSourceEnabled flag. [#137297](https://github.com/elastic/elasticsearch/pull/137297) 
* Don't store keyword multi fields when they trip ignore_above. [#132962](https://github.com/elastic/elasticsearch/pull/132962) 
* Provide defaults for index sort settings. [#135886](https://github.com/elastic/elasticsearch/pull/135886) [#129062](https://github.com/elastic/elasticsearch/issues/129062) 

**Relevance**
* Auto prefiltering for queries on dense semantic_text fields. [#138989](https://github.com/elastic/elasticsearch/pull/138989) 
* Intercept filters to knn queries. [#138457](https://github.com/elastic/elasticsearch/pull/138457) [#138410](https://github.com/elastic/elasticsearch/issues/138410) 
* Delay automaton creation in BinaryDvConfirmedQuery to avoid OOM on queries against WildCard fields. [#136086](https://github.com/elastic/elasticsearch/pull/136086) 
* Update Vector Similarity To Support BFLOAT16. [#139113](https://github.com/elastic/elasticsearch/pull/139113) 

**Rollup**
* Fixing _rollup/data performance for a large number of indices. [#138305](https://github.com/elastic/elasticsearch/pull/138305) 

**SQL**
* SQL: More friendly exceptions for validation errors. [#137560](https://github.com/elastic/elasticsearch/pull/137560) 
* SQL: do not attempt to canonicalize InnerAggregate. [#136854](https://github.com/elastic/elasticsearch/pull/136854) 

**Search**
* Ensure integer sorts are rewritten to long sorts for BWC indexes. [#139293](https://github.com/elastic/elasticsearch/pull/139293) [#139127](https://github.com/elastic/elasticsearch/issues/139127) [#139128](https://github.com/elastic/elasticsearch/issues/139128) 

**Security**
* Consistently prevent using exclusion prefix on its own. [#139337](https://github.com/elastic/elasticsearch/pull/139337) [#45504](https://github.com/elastic/elasticsearch/issues/45504) 
* Always treat dash-prefixed expression as index exclusion. [#138467](https://github.com/elastic/elasticsearch/pull/138467) 

**TSDB**
* ESQL: Use DEFAULT_UNSORTABLE topN encoder for the TSID_DATA_TYPE. [#137706](https://github.com/elastic/elasticsearch/pull/137706) 

**Vector search**
* DiskBBQ - missing min competitive similarity check on tail docs. [#135851](https://github.com/elastic/elasticsearch/pull/135851) 
* Disk bbq license enforcement. [#139087](https://github.com/elastic/elasticsearch/pull/139087) 

### Other changes [elasticsearch-9.3.0-other]

% **Geo**
% * Bumps jts version to 1.20.0. [#138351](https://github.com/elastic/elasticsearch/pull/138351) 

% **Vector search**
% * Bump cuvs-java to 25.12. [#139747](https://github.com/elastic/elasticsearch/pull/139747) 
% * GPU: add support for cosine with cuvs 2025.12. [#139821](https://github.com/elastic/elasticsearch/pull/139821) 
