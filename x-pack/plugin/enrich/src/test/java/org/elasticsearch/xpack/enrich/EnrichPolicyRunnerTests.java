/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.action.EnrichReindexAction;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class EnrichPolicyRunnerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ReindexPlugin.class, IngestCommonPlugin.class, SpatialPlugin.class, LocalStateEnrich.class);
    }

    private static ThreadPool testThreadPool;
    private static TaskManager testTaskManager;

    @BeforeClass
    public static void beforeCLass() {
        testThreadPool = new TestThreadPool("EnrichPolicyRunnerTests");
        testTaskManager = new TaskManager(Settings.EMPTY, testThreadPool, Collections.emptySet());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
    }

    public void testRunner() throws Exception {
        final String sourceIndex = "source-index";
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "field1": "value1",
              "field2": 2,
              "field3": "ignored",
              "field4": "ignored",
              "field5": "value5"
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = searchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
            }
        );

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
              "field1": {
                "type": "keyword",
                "doc_values": false
              },
              "field2": {
                "type": "long",
                "index": false
              },
              "field5": {
                "type": "text",
                "index": false
              }
            }
            """);
        // Validate document structure
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(3)));
                assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
                assertThat(enrichDocument.get("field2"), is(equalTo(2)));
                assertThat(enrichDocument.get("field5"), is(equalTo("value5")));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerGeoMatchType() throws Exception {
        final String sourceIndex = "source-index";
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {"location":"POINT(10.0 10.0)","zipcode":90210}""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("location"), is(equalTo("POINT(10.0 10.0)")));
                assertThat(sourceDocMap.get("zipcode"), is(equalTo(90210)));
            }
        );
        List<String> enrichFields = List.of("zipcode");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.GEO_MATCH_TYPE, null, List.of(sourceIndex), "location", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
                "location": {
                    "type": "geo_shape"
                },
                "zipcode": {
                    "type": "long",
                    "index": false
                }
            }
            """);
        // Validate document structure
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(2)));
                assertThat(enrichDocument.get("location"), is(equalTo("POINT(10.0 10.0)")));
                assertThat(enrichDocument.get("zipcode"), is(equalTo(90210)));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerIntegerRangeMatchType() throws Exception {
        testNumberRangeMatchType("integer");
    }

    public void testRunnerLongRangeMatchType() throws Exception {
        testNumberRangeMatchType("long");
    }

    public void testRunnerFloatRangeMatchType() throws Exception {
        testNumberRangeMatchType("float");
    }

    public void testRunnerDoubleRangeMatchType() throws Exception {
        testNumberRangeMatchType("double");
    }

    private void testNumberRangeMatchType(String rangeType) throws Exception {
        final String sourceIndex = "source-index";
        createIndex(sourceIndex, Settings.EMPTY, "_doc", "range", "type=" + rangeType + "_range");
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {"range":{"gt":1,"lt":10},"zipcode":90210}""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("range"), is(equalTo(Map.of("lt", 10, "gt", 1))));
                assertThat(sourceDocMap.get("zipcode"), is(equalTo(90210)));
            }
        );
        List<String> enrichFields = List.of("zipcode");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.RANGE_TYPE, null, List.of(sourceIndex), "range", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, String.format(Locale.ROOT, """
            {
                "range": {
                    "type": "%s",
                    "doc_values": false
                },
                "zipcode": {
                    "type": "long",
                    "index": false
                }
            }
            """, rangeType + "_range"));

        // Validate document structure
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(2)));
                assertThat(enrichDocument.get("range"), is(equalTo(Map.of("lt", 10, "gt", 1))));
                assertThat(enrichDocument.get("zipcode"), is(equalTo(90210)));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    private GetIndexResponse getGetIndexResponseAndCheck(String createdEnrichIndex) {
        GetIndexResponse enrichIndex = indicesAdmin().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));
        return enrichIndex;
    }

    public void testRunnerRangeTypeWithIpRange() throws Exception {
        final String sourceIndexName = "source-index";
        createIndex(sourceIndexName, Settings.EMPTY, "_doc", "subnet", "type=ip_range");
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndexName).id("id").source("""
            {"subnet":"10.0.0.0/8","department":"research"}""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        GetIndexResponse sourceIndex = indicesAdmin().getIndex(new GetIndexRequest().indices(sourceIndexName)).actionGet();
        // Validate Mapping
        Map<String, Object> sourceIndexMapping = sourceIndex.getMappings().get(sourceIndexName).sourceAsMap();
        Map<?, ?> sourceIndexProperties = (Map<?, ?>) sourceIndexMapping.get("properties");
        Map<?, ?> subnetField = (Map<?, ?>) sourceIndexProperties.get("subnet");
        assertNotNull(subnetField);
        assertThat(subnetField.get("type"), is(equalTo("ip_range")));

        assertResponse(
            client().search(
                new SearchRequest(sourceIndexName).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("subnet"), is(equalTo("10.0.0.0/8")));
                assertThat(sourceDocMap.get("department"), is(equalTo("research")));
            }
        );
        List<String> enrichFields = List.of("department");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.RANGE_TYPE, null, List.of(sourceIndexName), "subnet", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
                "subnet": {
                    "type": "ip_range",
                    "doc_values": false
                },
                "department": {
                    "type": "text",
                    "index": false
                }
            }
            """);
        // Validate document structure and lookup of element in range
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("subnet", "10.0.0.1"))
                )
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(2)));
                assertThat(enrichDocument.get("subnet"), is(equalTo("10.0.0.0/8")));
                assertThat(enrichDocument.get("department"), is(equalTo("research")));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerMultiSource() throws Exception {
        String baseSourceName = "source-index-";
        int numberOfSourceIndices = 3;
        for (int idx = 0; idx < numberOfSourceIndices; idx++) {
            final String sourceIndex = baseSourceName + idx;
            DocWriteResponse indexRequest = client().index(
                new IndexRequest().index(sourceIndex).id(randomAlphaOfLength(10)).source(Strings.format("""
                    {
                      "idx": %s,
                      "key": "key%s",
                      "field1": "value1",
                      "field2": 2,
                      "field3": "ignored",
                      "field4": "ignored",
                      "field5": "value5"
                    }""", idx, idx), XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertEquals(RestStatus.CREATED, indexRequest.status());
            final int targetIdx = idx;
            assertResponse(
                client().search(
                    new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
                ),
                sourceSearchResponse -> {
                    assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                    Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                    assertNotNull(sourceDocMap);
                    assertThat(sourceDocMap.get("idx"), is(equalTo(targetIdx)));
                    assertThat(sourceDocMap.get("key"), is(equalTo("key" + targetIdx)));
                    assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                    assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                    assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                    assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                    assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
                }
            );
        }

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = List.of("idx", "field1", "field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexPattern), "key", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
              "key": {
                "type": "keyword",
                "doc_values": false
              },
              "idx": {
                "type": "long",
                "index": false
              },
              "field1": {
                "type": "text",
                "index": false
              },
              "field2": {
                "type": "long",
                "index": false
              },
              "field5": {
                "type": "text",
                "index": false
              }
            }
            """);
        // Validate document structure
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {
                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(3L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(5)));
                assertThat(enrichDocument.get("key"), is(equalTo("key0")));
                assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
                assertThat(enrichDocument.get("field2"), is(equalTo(2)));
                assertThat(enrichDocument.get("field5"), is(equalTo("value5")));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 3);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerMultiSourceDocIdCollisions() throws Exception {
        String baseSourceName = "source-index-";
        int numberOfSourceIndices = 3;
        String collidingDocId = randomAlphaOfLength(10);
        for (int idx = 0; idx < numberOfSourceIndices; idx++) {
            final String sourceIndex = baseSourceName + idx;
            DocWriteResponse indexRequest = client().index(
                new IndexRequest().index(sourceIndex).id(collidingDocId).routing(collidingDocId + idx).source(Strings.format("""
                    {
                      "idx": %s,
                      "key": "key%s",
                      "field1": "value1",
                      "field2": 2,
                      "field3": "ignored",
                      "field4": "ignored",
                      "field5": "value5"
                    }""", idx, idx), XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertEquals(RestStatus.CREATED, indexRequest.status());
            final int targetIdx = idx;
            assertResponse(
                client().search(
                    new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
                ),
                sourceSearchResponse -> {
                    assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                    Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                    assertNotNull(sourceDocMap);
                    assertThat(sourceDocMap.get("idx"), is(equalTo(targetIdx)));
                    assertThat(sourceDocMap.get("key"), is(equalTo("key" + targetIdx)));
                    assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                    assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                    assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                    assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                    assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
                }
            );
            assertHitCount(
                client().search(
                    new SearchRequest(sourceIndex).source(
                        SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("_routing", collidingDocId + idx))
                    )
                ),
                1L
            );
        }

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = List.of("idx", "field1", "field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexPattern), "key", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertEnrichMapping(mapping, """
            {
              "key": {
                "type": "keyword",
                "doc_values": false
              },
              "idx": {
                "type": "long",
                "index": false
              },
              "field1": {
                "type": "text",
                "index": false
              },
              "field2": {
                "type": "long",
                "index": false
              },
              "field5": {
                "type": "text",
                "index": false
              }
            }
            """);
        // Validate document structure
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {
                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(3L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(5)));
                assertThat(enrichDocument.get("key"), is(equalTo("key0")));
                assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
                assertThat(enrichDocument.get("field2"), is(equalTo(2)));
                assertThat(enrichDocument.get("field5"), is(equalTo("value5")));
            }
        );
        // Validate removal of routing values
        for (int idx = 0; idx < numberOfSourceIndices; idx++) {
            final int targetIdx = idx;
            assertHitCount(
                client().search(
                    new SearchRequest(".enrich-test1").source(
                        SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("_routing", collidingDocId + targetIdx))
                    )
                ),
                0
            );
        }

        // Validate segments
        validateSegments(createdEnrichIndex, 3);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerMultiSourceEnrichKeyCollisions() throws Exception {
        String baseSourceName = "source-index-";
        int numberOfSourceIndices = 3;
        for (int idx = 0; idx < numberOfSourceIndices; idx++) {
            final String sourceIndex = baseSourceName + idx;
            DocWriteResponse indexRequest = client().index(
                new IndexRequest().index(sourceIndex).id(randomAlphaOfLength(10)).source(Strings.format("""
                    {
                      "idx": %s,
                      "key": "key",
                      "field1": "value1",
                      "field2": 2,
                      "field3": "ignored",
                      "field4": "ignored",
                      "field5": "value5"
                    }""", idx), XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertEquals(RestStatus.CREATED, indexRequest.status());

            final int targetIdx = idx;
            assertResponse(
                client().search(
                    new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
                ),
                sourceSearchResponse -> {
                    assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                    Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                    assertNotNull(sourceDocMap);
                    assertThat(sourceDocMap.get("idx"), is(equalTo(targetIdx)));
                    assertThat(sourceDocMap.get("key"), is(equalTo("key")));
                    assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                    assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                    assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                    assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                    assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
                }
            );
        }

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = List.of("idx", "field1", "field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexPattern), "key", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertEnrichMapping(mapping, """
            {
              "key": {
                "type": "keyword",
                "doc_values": false
              },
              "field1": {
                "type": "text",
                "index": false
              },
              "field2": {
                "type": "long",
                "index": false
              },
              "field5": {
                "type": "text",
                "index": false
              },
              "idx": {
                "type": "long",
                "index": false
              }
            }
            """);
        // Validate document structure
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {
                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(3L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(5)));
                assertThat(enrichDocument.get("key"), is(equalTo("key")));
                assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
                assertThat(enrichDocument.get("field2"), is(equalTo(2)));
                assertThat(enrichDocument.get("field5"), is(equalTo("value5")));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 3);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerNoSourceIndex() {
        final String sourceIndex = "source-index";

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        assertThat(
            asInstanceOf(IndexNotFoundException.class, safeExecuteExpectFailure(enrichPolicyRunner)).getMessage(),
            containsString("no such index [" + sourceIndex + "]")
        );
    }

    public void testRunnerNoSourceMapping() {
        final String sourceIndex = "source-index";
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        assertThat(
            asInstanceOf(ElasticsearchException.class, safeExecuteExpectFailure(enrichPolicyRunner)).getMessage(),
            containsString(
                "Enrich policy execution for ["
                    + policyName
                    + "] failed. No mapping available on source ["
                    + sourceIndex
                    + "] included in [["
                    + sourceIndex
                    + "]]"
            )
        );
    }

    public void testRunnerKeyNestedSourceMapping() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("nesting")
            .field("type", "nested")
            .startObject("properties")
            .startObject("key")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        String policyName = "test1";
        List<String> enrichFields = List.of("field2");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "nesting.key", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        final var thrown = asInstanceOf(ElasticsearchException.class, safeExecuteExpectFailure(enrichPolicyRunner));
        assertThat(
            thrown.getMessage(),
            containsString(
                "Enrich policy execution for [" + policyName + "] failed while validating field mappings for index [" + sourceIndex + "]"
            )
        );
        assertThat(
            thrown.getCause().getMessage(),
            containsString(
                "Could not traverse mapping to field [nesting.key]. The [nesting" + "] field must be regular object but was [nested]."
            )
        );
    }

    public void testRunnerValueNestedSourceMapping() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("key")
            .field("type", "keyword")
            .endObject()
            .startObject("nesting")
            .field("type", "nested")
            .startObject("properties")
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        String policyName = "test1";
        List<String> enrichFields = List.of("nesting.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "key", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        final var thrown = asInstanceOf(ElasticsearchException.class, safeExecuteExpectFailure(enrichPolicyRunner));
        assertThat(
            thrown.getMessage(),
            containsString(
                "Enrich policy execution for [" + policyName + "] failed while validating field mappings for index [" + sourceIndex + "]"
            )
        );
        assertThat(
            thrown.getCause().getMessage(),
            containsString(
                "Could not traverse mapping to field [nesting.field2]. " + "The [nesting] field must be regular object but was [nested]."
            )
        );
    }

    public void testRunnerObjectSourceMapping() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("data")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .startObject("field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        DocWriteResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source("""
                    {"data":{"field1":"value1","field2":2,"field3":"ignored"}}""", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
                assertNotNull(dataField);
                assertThat(dataField.get("field1"), is(equalTo("value1")));
                assertThat(dataField.get("field2"), is(equalTo(2)));
                assertThat(dataField.get("field3"), is(equalTo("ignored")));
            }
        );

        String policyName = "test1";
        List<String> enrichFields = List.of("data.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
              "data": {
                "properties": {
                  "field1": {
                    "type": "keyword",
                    "doc_values": false
                  },
                  "field2": {
                    "type": "integer",
                    "index": false
                  }
                }
              }
            }
            """);
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(1)));
                Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
                assertNotNull(resultDataField);
                assertThat(resultDataField.size(), is(equalTo(2)));
                assertThat(resultDataField.get("field1"), is(equalTo("value1")));
                assertThat(resultDataField.get("field2"), is(equalTo(2)));
                assertNull(resultDataField.get("field3"));
            }
        );

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerExplicitObjectSourceMapping() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("data")
            .field("type", "object")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .startObject("field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        DocWriteResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source("""
                    {"data":{"field1":"value1","field2":2,"field3":"ignored"}}""", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
                assertNotNull(dataField);
                assertThat(dataField.get("field1"), is(equalTo("value1")));
                assertThat(dataField.get("field2"), is(equalTo(2)));
                assertThat(dataField.get("field3"), is(equalTo("ignored")));
            }
        );
        String policyName = "test1";
        List<String> enrichFields = List.of("data.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
              "data": {
                "properties": {
                  "field1": {
                    "type": "keyword",
                    "doc_values": false
                  },
                  "field2": {
                    "type": "integer",
                    "index": false
                  }
                }
              }
            }
            """);
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(1)));
                Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
                assertNotNull(resultDataField);
                assertThat(resultDataField.size(), is(equalTo(2)));
                assertThat(resultDataField.get("field1"), is(equalTo("value1")));
                assertThat(resultDataField.get("field2"), is(equalTo(2)));
                assertNull(resultDataField.get("field3"));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerExplicitObjectSourceMappingRangePolicy() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("data")
            .field("type", "object")
            .startObject("properties")
            .startObject("subnet")
            .field("type", "ip_range")
            .endObject()
            .startObject("department")
            .field("type", "keyword")
            .endObject()
            .startObject("field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "data": {
                "subnet": "10.0.0.0/8",
                "department": "research",
                "field3": "ignored"
              }
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
                assertNotNull(dataField);
                assertThat(dataField.get("subnet"), is(equalTo("10.0.0.0/8")));
                assertThat(dataField.get("department"), is(equalTo("research")));
                assertThat(dataField.get("field3"), is(equalTo("ignored")));
            }
        );
        String policyName = "test1";
        List<String> enrichFields = List.of("data.department", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.RANGE_TYPE, null, List.of(sourceIndex), "data.subnet", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
                "data": {
                    "properties": {
                        "subnet": {
                            "type": "ip_range",
                            "doc_values": false
                        },
                        "department": {
                            "type": "keyword",
                            "index": false
                        }
                    }
                }
            }
            """);
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("data.subnet", "10.0.0.1"))
                )
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(1)));
                Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
                assertNotNull(resultDataField);
                assertThat(resultDataField.size(), is(equalTo(2)));
                assertThat(resultDataField.get("subnet"), is(equalTo("10.0.0.0/8")));
                assertThat(resultDataField.get("department"), is(equalTo("research")));
                assertNull(resultDataField.get("field3"));
            }
        );

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerTwoObjectLevelsSourceMapping() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("data")
            .startObject("properties")
            .startObject("fields")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .startObject("field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "data": {
                "fields": {
                  "field1": "value1",
                  "field2": 2,
                  "field3": "ignored"
                }
              }
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
                assertNotNull(dataField);
                Map<?, ?> fieldsField = ((Map<?, ?>) dataField.get("fields"));
                assertNotNull(fieldsField);
                assertThat(fieldsField.get("field1"), is(equalTo("value1")));
                assertThat(fieldsField.get("field2"), is(equalTo(2)));
                assertThat(fieldsField.get("field3"), is(equalTo("ignored")));
            }
        );
        String policyName = "test1";
        List<String> enrichFields = List.of("data.fields.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.fields.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
              "data": {
                "properties": {
                  "fields": {
                    "properties": {
                      "field1": {
                        "type": "keyword",
                        "doc_values": false
                      },
                      "field2": {
                        "type": "integer",
                        "index": false
                      }
                    }
                  }
                }
              }
            }
            """);

        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(1)));
                Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
                assertNotNull(resultDataField);
                Map<?, ?> resultFieldsField = ((Map<?, ?>) resultDataField.get("fields"));
                assertNotNull(resultFieldsField);
                assertThat(resultFieldsField.size(), is(equalTo(2)));
                assertThat(resultFieldsField.get("field1"), is(equalTo("value1")));
                assertThat(resultFieldsField.get("field2"), is(equalTo(2)));
                assertNull(resultFieldsField.get("field3"));
            }
        );

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerTwoObjectLevelsSourceMappingRangePolicy() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("data")
            .startObject("properties")
            .startObject("fields")
            .startObject("properties")
            .startObject("subnet")
            .field("type", "ip_range")
            .endObject()
            .startObject("department")
            .field("type", "keyword")
            .endObject()
            .startObject("field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "data": {
                "fields": {
                  "subnet": "10.0.0.0/8",
                  "department": "research",
                  "field3": "ignored"
                }
              }
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
                assertNotNull(dataField);
                Map<?, ?> fieldsField = ((Map<?, ?>) dataField.get("fields"));
                assertNotNull(fieldsField);
                assertThat(fieldsField.get("subnet"), is(equalTo("10.0.0.0/8")));
                assertThat(fieldsField.get("department"), is(equalTo("research")));
                assertThat(fieldsField.get("field3"), is(equalTo("ignored")));
            }
        );

        String policyName = "test1";
        List<String> enrichFields = List.of("data.fields.department", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.RANGE_TYPE, null, List.of(sourceIndex), "data.fields.subnet", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
                "data": {
                    "properties": {
                        "fields": {
                            "properties": {
                                "subnet": {
                                    "type": "ip_range",
                                    "doc_values": false
                                },
                                "department": {
                                    "type": "keyword",
                                    "index": false
                                }
                            }
                        }
                    }
                }
            }
            """);
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {
                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(1)));
                Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
                assertNotNull(resultDataField);
                Map<?, ?> resultFieldsField = ((Map<?, ?>) resultDataField.get("fields"));
                assertNotNull(resultFieldsField);
                assertThat(resultFieldsField.size(), is(equalTo(2)));
                assertThat(resultFieldsField.get("subnet"), is(equalTo("10.0.0.0/8")));
                assertThat(resultFieldsField.get("department"), is(equalTo("research")));
                assertNull(resultFieldsField.get("field3"));
            }
        );
        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerTwoObjectLevelsSourceMappingDateRangeWithFormat() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("data")
            .startObject("properties")
            .startObject("fields")
            .startObject("properties")
            .startObject("period")
            .field("type", "date_range")
            .field("format", "yyyy'/'MM'/'dd' at 'HH':'mm||strict_date_time")
            .endObject()
            .startObject("status")
            .field("type", "keyword")
            .endObject()
            .startObject("field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "data": {
                "fields": {
                  "period": {
                    "gte": "2021/08/20 at 12:00",
                    "lte": "2021/08/28 at 23:00"
                  },
                  "status": "enrolled",
                  "field3": "ignored"
                }
              }
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
                assertNotNull(dataField);
                Map<?, ?> fieldsField = ((Map<?, ?>) dataField.get("fields"));
                assertNotNull(fieldsField);
                Map<?, ?> periodField = ((Map<?, ?>) fieldsField.get("period"));
                assertNotNull(periodField);
                assertThat(periodField.get("gte"), is(equalTo("2021/08/20 at 12:00")));
                assertThat(periodField.get("lte"), is(equalTo("2021/08/28 at 23:00")));
                assertThat(fieldsField.get("status"), is(equalTo("enrolled")));
                assertThat(fieldsField.get("field3"), is(equalTo("ignored")));
            }
        );

        String policyName = "test1";
        List<String> enrichFields = List.of("data.fields.status", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.RANGE_TYPE, null, List.of(sourceIndex), "data.fields.period", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
             "data": {
                "properties": {
                    "fields": {
                        "properties": {
                            "period": {
                                "type": "date_range",
                                "format": "yyyy'/'MM'/'dd' at 'HH':'mm||strict_date_time",
                                "doc_values": false
                            },
                            "status": {
                                "type": "keyword",
                                "index": false
                            }
                        }
                    }
                }
             }
            }
            """);

        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("data.fields.period", "2021-08-19T14:00:00Z"))
                )
            ),
            enrichSearchResponse -> assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(0L))
        );

        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("data.fields.period", "2021-08-20T14:00:00Z"))
                )
            ),
            enrichSearchResponse -> {
                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(1)));
                Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
                assertNotNull(resultDataField);
                Map<?, ?> resultFieldsField = ((Map<?, ?>) resultDataField.get("fields"));
                assertNotNull(resultFieldsField);
                assertThat(resultFieldsField.size(), is(equalTo(2)));
                Map<?, ?> resultsPeriodField = ((Map<?, ?>) resultFieldsField.get("period"));
                assertNotNull(resultsPeriodField);
                assertThat(resultsPeriodField.get("gte"), is(equalTo("2021/08/20 at 12:00")));
                assertThat(resultsPeriodField.get("lte"), is(equalTo("2021/08/28 at 23:00")));
                assertThat(resultFieldsField.get("status"), is(equalTo("enrolled")));
                assertNull(resultFieldsField.get("field3"));
            }
        );

        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("data.fields.period", "2021/08/20 at 14:00"))
                )
            ),
            enrichSearchResponse -> assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L))
        );

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerDottedKeyNameSourceMapping() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("data.field1")
            .field("type", "keyword")
            .endObject()
            .startObject("data.field2")
            .field("type", "integer")
            .endObject()
            .startObject("data.field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        DocWriteResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source("""
                    {"data.field1":"value1","data.field2":2,"data.field3":"ignored"}""", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("data.field1"), is(equalTo("value1")));
                assertThat(sourceDocMap.get("data.field2"), is(equalTo(2)));
                assertThat(sourceDocMap.get("data.field3"), is(equalTo("ignored")));
            }
        );
        String policyName = "test1";
        List<String> enrichFields = List.of("data.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertEnrichMapping(mapping, """
            {
              "data": {
                "properties": {
                  "field1": {
                    "type": "keyword",
                    "doc_values": false
                  },
                  "field2": {
                    "type": "integer",
                    "index": false
                  }
                }
              }
            }
            """);
        assertResponse(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            enrichSearchResponse -> {

                assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                assertNotNull(enrichDocument);
                assertThat(enrichDocument.size(), is(equalTo(2)));
                assertThat(enrichDocument.get("data.field1"), is(equalTo("value1")));
                assertThat(enrichDocument.get("data.field2"), is(equalTo(2)));
                assertNull(enrichDocument.get("data.field3"));
            }
        );

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerWithForceMergeRetry() throws Exception {
        final String sourceIndex = "source-index";
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "field1": "value1",
              "field2": 2,
              "field3": "ignored",
              "field4": "ignored",
              "field5": "value5"
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
            }
        );
        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);
        Task asyncTask = testTaskManager.register("enrich", "policy_execution", new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new ExecuteEnrichPolicyTask(id, type, action, getDescription(), parentTaskId, headers);
            }

            @Override
            public String getDescription() {
                return policyName;
            }
        });
        ExecuteEnrichPolicyTask task = ((ExecuteEnrichPolicyTask) asyncTask);
        AtomicInteger forceMergeAttempts = new AtomicInteger(0);
        final XContentBuilder unmergedDocument = SmileXContent.contentBuilder()
            .startObject()
            .field("field1", "value1.1")
            .field("field2", 2)
            .field("field5", "value5")
            .endObject();
        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(
            policyName,
            policy,
            task,
            clusterService,
            getInstanceFromNode(IndicesService.class),
            client(),
            resolver,
            createdEnrichIndex,
            randomIntBetween(1, 10000),
            randomIntBetween(3, 10)
        ) {
            @Override
            public void run(ActionListener<ExecuteEnrichPolicyStatus> listener) {
                // The executor would wrap the listener in order to clean up the task in the
                // task manager, but we're just testing the runner, so we make sure to clean
                // up after ourselves.
                super.run(ActionListener.runBefore(listener, () -> testTaskManager.unregister(task)));
            }

            @Override
            protected void afterRefreshEnrichIndex(ActionListener<Void> listener) {
                final var attempt = forceMergeAttempts.incrementAndGet();
                if (attempt == 1) {
                    // Put and flush a document to increase the number of segments, simulating not
                    // all segments were merged on the first try.
                    client().index(
                        new IndexRequest().index(createdEnrichIndex)
                            .source(unmergedDocument)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                        listener.delegateFailureAndWrap((l, response) -> {
                            assertEquals(RestStatus.CREATED, response.status());
                            super.afterRefreshEnrichIndex(l);
                        })
                    );
                } else {
                    super.afterRefreshEnrichIndex(listener);
                }
            }
        };

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate number of force merges
        assertThat(forceMergeAttempts.get(), equalTo(2));

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertEnrichMapping(mapping, """
            {
              "field1": {
                "type": "keyword",
                "doc_values": false
              },
              "field2": {
                "type": "long",
                "index": false
              },
              "field5": {
                "type": "text",
                "index": false
              }
            }
            """);
        // Validate document structure
        assertHitCount(
            client().search(
                new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ),
            2L
        );
        for (String keyValue : List.of("value1", "value1.1")) {
            assertResponse(
                client().search(
                    new SearchRequest(".enrich-test1").source(
                        SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("field1", keyValue))
                    )
                ),
                enrichSearchResponse -> {

                    assertThat(enrichSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                    Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
                    assertNotNull(enrichDocument);
                    assertThat(enrichDocument.size(), is(equalTo(3)));
                    assertThat(enrichDocument.get("field1"), is(equalTo(keyValue)));
                    assertThat(enrichDocument.get("field2"), is(equalTo(2)));
                    assertThat(enrichDocument.get("field5"), is(equalTo("value5")));
                }
            );
        }

        // Validate segments
        validateSegments(createdEnrichIndex, 2);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerWithEmptySegmentsResponse() throws Exception {
        final String sourceIndex = "source-index";
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "field1": "value1",
              "field2": 2,
              "field3": "ignored",
              "field4": "ignored",
              "field5": "value5"
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
            }
        );
        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);
        Task asyncTask = testTaskManager.register("enrich", "policy_execution", new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new ExecuteEnrichPolicyTask(id, type, action, getDescription(), parentTaskId, headers);
            }

            @Override
            public String getDescription() {
                return policyName;
            }
        });
        ExecuteEnrichPolicyTask task = ((ExecuteEnrichPolicyTask) asyncTask);

        // Wrap the client so that when we receive the indices segments action, we intercept the request and complete it on another thread
        // with an empty segments response.
        Client client = new FilterClient(client()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(IndicesSegmentsAction.INSTANCE)) {
                    testThreadPool.generic().execute(() -> {
                        @SuppressWarnings("unchecked")
                        ActionListener<IndicesSegmentResponse> castListener = ((ActionListener<IndicesSegmentResponse>) listener);
                        castListener.onResponse(new IndicesSegmentResponse(new ShardSegments[0], 0, 0, 0, List.of()));
                    });
                } else {
                    super.doExecute(action, request, listener);
                }
            }
        };

        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(
            policyName,
            policy,
            task,
            clusterService,
            getInstanceFromNode(IndicesService.class),
            client,
            resolver,
            createdEnrichIndex,
            randomIntBetween(1, 10000),
            randomIntBetween(3, 10)
        ) {
            @Override
            public void run(ActionListener<ExecuteEnrichPolicyStatus> listener) {
                // The executor would wrap the listener in order to clean up the task in the
                // task manager, but we're just testing the runner, so we make sure to clean
                // up after ourselves.
                super.run(ActionListener.runBefore(listener, () -> testTaskManager.unregister(task)));
            }
        };

        logger.info("Starting policy run");
        assertThat(
            asInstanceOf(ElasticsearchException.class, safeExecuteExpectFailure(enrichPolicyRunner)).getMessage(),
            containsString("Could not locate segment information for newly created index")
        );
    }

    public void testRunnerWithShardFailuresInSegmentResponse() throws Exception {
        final String sourceIndex = "source-index";
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "field1": "value1",
              "field2": 2,
              "field3": "ignored",
              "field4": "ignored",
              "field5": "value5"
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
            }
        );
        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);
        Task asyncTask = testTaskManager.register("enrich", "policy_execution", new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new ExecuteEnrichPolicyTask(id, type, action, getDescription(), parentTaskId, headers);
            }

            @Override
            public String getDescription() {
                return policyName;
            }
        });
        ExecuteEnrichPolicyTask task = ((ExecuteEnrichPolicyTask) asyncTask);
        // The executor would wrap the listener in order to clean up the task in the
        // task manager, but we're just testing the runner, so we make sure to clean
        // up after ourselves.

        // Wrap the client so that when we receive the indices segments action, we intercept the request and complete it on another thread
        // with an failed segments response.
        Client client = new FilterClient(client()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(IndicesSegmentsAction.INSTANCE)) {
                    testThreadPool.generic().execute(() -> {
                        @SuppressWarnings("unchecked")
                        ActionListener<IndicesSegmentResponse> castListener = ((ActionListener<IndicesSegmentResponse>) listener);
                        castListener.onResponse(
                            new IndicesSegmentResponse(
                                new ShardSegments[0],
                                0,
                                0,
                                3,
                                List.of(
                                    new DefaultShardOperationFailedException(createdEnrichIndex, 1, new ElasticsearchException("failure1")),
                                    new DefaultShardOperationFailedException(createdEnrichIndex, 2, new ElasticsearchException("failure2")),
                                    new DefaultShardOperationFailedException(createdEnrichIndex, 3, new ElasticsearchException("failure3"))
                                )
                            )
                        );
                    });
                } else {
                    super.doExecute(action, request, listener);
                }
            }
        };

        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(
            policyName,
            policy,
            task,
            clusterService,
            getInstanceFromNode(IndicesService.class),
            client,
            resolver,
            createdEnrichIndex,
            randomIntBetween(1, 10000),
            randomIntBetween(3, 10)
        ) {
            @Override
            public void run(ActionListener<ExecuteEnrichPolicyStatus> listener) {
                // The executor would wrap the listener in order to clean up the task in the
                // task manager, but we're just testing the runner, so we make sure to clean
                // up after ourselves.
                super.run(ActionListener.runBefore(listener, () -> testTaskManager.unregister(task)));
            }
        };

        logger.info("Starting policy run");
        final var exceptionThrown = asInstanceOf(ElasticsearchException.class, safeExecuteExpectFailure(enrichPolicyRunner));
        assertThat(exceptionThrown.getMessage(), containsString("Could not obtain segment information for newly created index"));
        assertThat(asInstanceOf(ElasticsearchException.class, exceptionThrown.getCause()).getMessage(), containsString("failure1"));
    }

    public void testRunnerCancel() {
        final String sourceIndex = "source-index";
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "field1": "value1",
              "field2": 2,
              "field3": "ignored",
              "field4": "ignored",
              "field5": "value5"
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;

        ActionType<?> randomActionType = randomFrom(
            EnrichReindexAction.INSTANCE,
            GetIndexAction.INSTANCE,
            TransportCreateIndexAction.TYPE,
            ForceMergeAction.INSTANCE,
            RefreshAction.INSTANCE,
            IndicesSegmentsAction.INSTANCE,
            TransportUpdateSettingsAction.TYPE,
            TransportClusterHealthAction.TYPE
        );
        logger.info("Selected [{}] to perform cancel", randomActionType.name());
        Client client = new FilterClient(client()) {

            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(randomActionType)) {
                    testTaskManager.getCancellableTasks()
                        .values()
                        .stream()
                        .filter(cancellableTask -> cancellableTask instanceof ExecuteEnrichPolicyTask)
                        .forEach(task -> testTaskManager.cancel(task, "", () -> {}));
                }
                super.doExecute(action, request, listener);
            }
        };

        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(client, policyName, policy, createdEnrichIndex);
        logger.info("Starting policy run");
        assertThat(
            safeExecuteExpectFailure(enrichPolicyRunner).getMessage(),
            containsString("cancelled policy execution [test1], status [")
        );
    }

    public void testRunRangePolicyWithObjectFieldAsMatchField() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1");
        if (randomBoolean()) {
            mapping.field("type", "object");
        }
        mapping.startObject("properties")
            .startObject("sub")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .startObject("field3")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex(sourceIndex, Settings.EMPTY, mapping);

        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.RANGE_TYPE, null, List.of(sourceIndex), "field1", List.of("field2"));
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        assertThat(
            safeExecuteExpectFailure(enrichPolicyRunner).getMessage(),
            equalTo("Field 'field1' has type [object] which doesn't appear to be a range type")
        );
    }

    public void testEnrichFieldsConflictMappingTypes() throws Exception {
        createIndex("source-1", Settings.EMPTY, "_doc", "user", "type=keyword", "name", "type=text", "zipcode", "type=long");
        prepareIndex("source-1").setSource("user", "u1", "name", "n", "zipcode", 90000)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        createIndex("source-2", Settings.EMPTY, "_doc", "user", "type=keyword", "zipcode", "type=long");

        prepareIndex("source-2").setSource("""
            {
              "user": "u2",
              "name": {
                "first": "f",
                "last": "l"
              },
              "zipcode": 90001
            }
            """, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        EnrichPolicy policy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of("source-1", "source-2"),
            "user",
            List.of("name", "zipcode")
        );
        String policyName = "test1";
        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        safeExecute(createPolicyRunner(policyName, policy, createdEnrichIndex));

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertEnrichMapping(mapping, """
            {
              "user": {
                "type": "keyword",
                "doc_values": false
              },
              "zipcode": {
                "type": "long",
                "index": false
              }
            }
            """);
        // Validate document structure
        assertResponse(client().search(new SearchRequest(".enrich-test1")), searchResponse -> {
            ElasticsearchAssertions.assertHitCount(searchResponse, 2L);
            Map<String, Object> hit0 = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(hit0, equalTo(Map.of("user", "u1", "name", "n", "zipcode", 90000)));
            Map<String, Object> hit1 = searchResponse.getHits().getAt(1).getSourceAsMap();
            assertThat(hit1, equalTo(Map.of("user", "u2", "name", Map.of("first", "f", "last", "l"), "zipcode", 90001)));
        });
    }

    public void testEnrichMappingConflictFormats() throws ExecutionException, InterruptedException {
        createIndex("source-1", Settings.EMPTY, "_doc", "user", "type=keyword", "date", "type=date,format=yyyy");
        prepareIndex("source-1").setSource("user", "u1", "date", "2023").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        createIndex("source-2", Settings.EMPTY, "_doc", "user", "type=keyword", "date", "type=date,format=yyyy-MM");

        prepareIndex("source-2").setSource("""
            {
              "user": "u2",
              "date": "2023-05"
            }
            """, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source-1", "source-2"), "user", List.of("date"));
        String policyName = "test1";
        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        safeExecute(createPolicyRunner(policyName, policy, createdEnrichIndex));

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertEnrichMapping(mapping, """
            {
              "user": {
                "type": "keyword",
                "doc_values": false
              }
            }
            """);
        // Validate document structure
        assertResponse(client().search(new SearchRequest(".enrich-test1")), searchResponse -> {
            ElasticsearchAssertions.assertHitCount(searchResponse, 2L);
            Map<String, Object> hit0 = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(hit0, equalTo(Map.of("user", "u1", "date", "2023")));
            Map<String, Object> hit1 = searchResponse.getHits().getAt(1).getSourceAsMap();
            assertThat(hit1, equalTo(Map.of("user", "u2", "date", "2023-05")));
        });
    }

    public void testEnrichObjectField() throws ExecutionException, InterruptedException {
        createIndex("source-1", Settings.EMPTY, "_doc", "id", "type=keyword", "name.first", "type=keyword", "name.last", "type=keyword");
        prepareIndex("source-1").setSource("user", "u1", "name.first", "F1", "name.last", "L1")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source-1"), "user", List.of("name"));
        String policyName = "test1";
        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        safeExecute(createPolicyRunner(policyName, policy, createdEnrichIndex));

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertEnrichMapping(mapping, """
            {
              "user": {
                "type": "keyword",
                "doc_values": false
              },
              "name": {
                "type": "object"
              }
            }
            """);
        assertResponse(client().search(new SearchRequest(".enrich-test1")), searchResponse -> {
            ElasticsearchAssertions.assertHitCount(searchResponse, 1L);
            Map<String, Object> hit0 = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(hit0, equalTo(Map.of("user", "u1", "name.first", "F1", "name.last", "L1")));
        });
    }

    public void testEnrichNestedField() throws Exception {
        final String sourceIndex = "source-index";
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject()
            .startObject(MapperService.SINGLE_MAPPING_NAME)
            .startObject("properties")
            .startObject("user")
            .field("type", "keyword")
            .endObject()
            .startObject("nesting")
            .field("type", "nested")
            .startObject("properties")
            .startObject("key")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .startObject("field2")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        CreateIndexResponse createResponse = indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        String policyName = "test1";
        List<String> enrichFields = List.of("nesting", "field2");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "user", enrichFields);

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        safeExecute(enrichPolicyRunner);

        // Validate Index definition
        GetIndexResponse enrichIndex = getGetIndexResponseAndCheck(createdEnrichIndex);
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertEnrichMapping(mapping, """
            {
              "user": {
                "type": "keyword",
                "doc_values": false
              },
              "field2": {
                "type": "integer",
                "index": false
              },
              "nesting": {
                "type": "nested"
              }
            }
            """);
    }

    public void testRunnerValidatesIndexIntegrity() throws Exception {
        final String sourceIndex = "source-index";
        DocWriteResponse indexRequest = client().index(new IndexRequest().index(sourceIndex).id("id").source("""
            {
              "field1": "value1",
              "field2": 2,
              "field3": "ignored",
              "field4": "ignored",
              "field5": "value5"
            }""", XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        assertResponse(
            client().search(new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))),
            sourceSearchResponse -> {
                assertThat(sourceSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
                assertNotNull(sourceDocMap);
                assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
                assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
                assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
                assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
            }
        );

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;

        // Wrap the client so that when we receive the reindex action, we delete the index then resume operation. This mimics an invalid
        // state for the resulting index.
        Client client = new FilterClient(client()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(EnrichReindexAction.INSTANCE)) {
                    super.doExecute(
                        TransportDeleteIndexAction.TYPE,
                        new DeleteIndexRequest(createdEnrichIndex),
                        listener.delegateFailureAndWrap((delegate, response) -> {
                            if (response.isAcknowledged() == false) {
                                fail("Enrich index should have been deleted but was not");
                            }
                            super.doExecute(action, request, delegate);
                        })
                    );
                } else {
                    super.doExecute(action, request, listener);
                }
            }
        };
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(client, policyName, policy, createdEnrichIndex);

        logger.info("Starting policy run");
        assertThat(
            asInstanceOf(ElasticsearchException.class, safeExecuteExpectFailure(enrichPolicyRunner)).getMessage(),
            allOf(containsString("Could not verify enrich index"), containsString("mapping meta field missing"))
        );
    }

    private EnrichPolicyRunner createPolicyRunner(String policyName, EnrichPolicy policy, String targetIndex) {
        return createPolicyRunner(client(), policyName, policy, targetIndex);
    }

    private EnrichPolicyRunner createPolicyRunner(Client client, String policyName, EnrichPolicy policy, String targetIndex) {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);
        Task asyncTask = testTaskManager.register("enrich", "policy_execution", new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new ExecuteEnrichPolicyTask(id, type, action, getDescription(), parentTaskId, headers);
            }

            @Override
            public String getDescription() {
                return policyName;
            }
        });
        ExecuteEnrichPolicyTask task = ((ExecuteEnrichPolicyTask) asyncTask);
        return new EnrichPolicyRunner(
            policyName,
            policy,
            task,
            clusterService,
            getInstanceFromNode(IndicesService.class),
            client,
            resolver,
            targetIndex,
            randomIntBetween(1, 10000),
            randomIntBetween(1, 10)
        ) {
            @Override
            public void run(ActionListener<ExecuteEnrichPolicyStatus> listener) {
                // The executor would wrap the listener in order to clean up the task in the
                // task manager, but we're just testing the runner, so we make sure to clean
                // up after ourselves.
                super.run(ActionListener.runBefore(listener, () -> testTaskManager.unregister(task)));
            }
        };
    }

    private void safeExecute(EnrichPolicyRunner enrichPolicyRunner) {
        safeAwait(enrichPolicyRunner::run);
        logger.debug("Run complete");
    }

    private Exception safeExecuteExpectFailure(EnrichPolicyRunner enrichPolicyRunner) {
        return safeAwaitFailure(enrichPolicyRunner::run);
    }

    private void validateMappingMetadata(Map<?, ?> mapping, String policyName, EnrichPolicy policy) {
        Object metadata = mapping.get("_meta");
        assertThat(metadata, is(notNullValue()));
        Map<?, ?> metadataMap = (Map<?, ?>) metadata;
        assertThat(metadataMap.get(EnrichPolicyRunner.ENRICH_README_FIELD_NAME), equalTo(EnrichPolicyRunner.ENRICH_INDEX_README_TEXT));
        assertThat(metadataMap.get(EnrichPolicyRunner.ENRICH_POLICY_NAME_FIELD_NAME), equalTo(policyName));
        assertThat(metadataMap.get(EnrichPolicyRunner.ENRICH_MATCH_FIELD_NAME), equalTo(policy.getMatchField()));
        assertThat(metadataMap.get(EnrichPolicyRunner.ENRICH_POLICY_TYPE_FIELD_NAME), equalTo(policy.getType()));
    }

    private void validateSegments(String createdEnrichIndex, int expectedDocs) {
        IndicesSegmentResponse indicesSegmentResponse = indicesAdmin().segments(new IndicesSegmentsRequest(createdEnrichIndex)).actionGet();
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(createdEnrichIndex);
        assertNotNull(indexSegments);
        assertThat(indexSegments.getShards().size(), is(equalTo(1)));
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);
        assertThat(shardSegments.shards().length, is(equalTo(1)));
        ShardSegments shard = shardSegments.shards()[0];
        assertThat(shard.getSegments().size(), is(equalTo(1)));
        Segment segment = shard.getSegments().iterator().next();
        assertThat(segment.getNumDocs(), is(equalTo(expectedDocs)));
    }

    private void ensureEnrichIndexIsReadOnly(String createdEnrichIndex) {
        ElasticsearchException expected = expectThrows(
            ElasticsearchException.class,
            () -> client().index(
                new IndexRequest().index(createdEnrichIndex)
                    .id(randomAlphaOfLength(10))
                    .source(Map.of(randomAlphaOfLength(6), randomAlphaOfLength(10)))
            ).actionGet()
        );

        assertThat(expected.getMessage(), containsString("index [" + createdEnrichIndex + "] blocked by: [FORBIDDEN/8/index write (api)]"));
    }

    private static void assertEnrichMapping(Map<String, Object> actual, String expectedMapping) {
        assertThat(actual.get("dynamic"), is("false"));
        Object actualProperties = actual.get("properties");
        Map<String, Object> mappings = XContentHelper.convertToMap(JsonXContent.jsonXContent, expectedMapping, false);
        assertThat(actualProperties, equalTo(mappings));
    }
}
