/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.geo.GeoPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class EnrichPolicyRunnerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ReindexPlugin.class, IngestCommonPlugin.class, GeoPlugin.class, LocalStateEnrich.class);
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
        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source(
                    "{"
                        + "\"field1\":\"value1\","
                        + "\"field2\":2,"
                        + "\"field3\":\"ignored\","
                        + "\"field4\":\"ignored\","
                        + "\"field5\":\"value5\""
                        + "}",
                    XContentType.JSON
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
        assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
        assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) properties.get("field1");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("keyword")));
        assertThat(field1.get("doc_values"), is(false));

        // Validate document structure
        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(3)));
        assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocument.get("field2"), is(equalTo(2)));
        assertThat(enrichDocument.get("field5"), is(equalTo("value5")));

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerGeoMatchType() throws Exception {
        final String sourceIndex = "source-index";
        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source("{" + "\"location\":" + "\"POINT(10.0 10.0)\"," + "\"zipcode\":90210" + "}", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("location"), is(equalTo("POINT(10.0 10.0)")));
        assertThat(sourceDocMap.get("zipcode"), is(equalTo(90210)));

        List<String> enrichFields = List.of("zipcode");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.GEO_MATCH_TYPE, null, List.of(sourceIndex), "location", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) properties.get("location");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("geo_shape")));
        assertNull(field1.get("doc_values"));

        // Validate document structure
        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(2)));
        assertThat(enrichDocument.get("location"), is(equalTo("POINT(10.0 10.0)")));
        assertThat(enrichDocument.get("zipcode"), is(equalTo(90210)));

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerMatchTypeWithIpRange() throws Exception {
        final String sourceIndexName = "source-index";
        createIndex(sourceIndexName,Settings.EMPTY,"_doc","subnet", "type=ip_range");
        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndexName)
                .id("id")
                .source("{" + "\"subnet\":" + "\"10.0.0.0/8\"," + "\"department\":\"research\"" + "}", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        GetIndexResponse sourceIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(sourceIndexName)).actionGet();
        // Validate Mapping
        Map<String, Object> sourceIndexMapping = sourceIndex.getMappings().get(sourceIndexName).sourceAsMap();
        Map<?, ?> sourceIndexProperties = (Map<?, ?>) sourceIndexMapping.get("properties");
        Map<?, ?> subnetField = (Map<?, ?>) sourceIndexProperties.get("subnet");
        assertNotNull(subnetField);
        assertThat(subnetField.get("type"), is(equalTo("ip_range")));

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndexName).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("subnet"), is(equalTo("10.0.0.0/8")));
        assertThat(sourceDocMap.get("department"), is(equalTo("research")));

        List<String> enrichFields = List.of("department");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexName), "subnet", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) properties.get("subnet");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("ip_range")));
        assertThat(field1.get("doc_values"), is(false));

        // Validate document structure and lookup of element in range
        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("subnet", "10.0.0.1")))
        ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(2)));
        assertThat(enrichDocument.get("subnet"), is(equalTo("10.0.0.0/8")));
        assertThat(enrichDocument.get("department"), is(equalTo("research")));

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
            IndexResponse indexRequest = client().index(
                new IndexRequest().index(sourceIndex)
                    .id(randomAlphaOfLength(10))
                    .source(
                        "{"
                            + "\"idx\":"
                            + idx
                            + ","
                            + "\"key\":"
                            + "\"key"
                            + idx
                            + "\","
                            + "\"field1\":\"value1\","
                            + "\"field2\":2,"
                            + "\"field3\":\"ignored\","
                            + "\"field4\":\"ignored\","
                            + "\"field5\":\"value5\""
                            + "}",
                        XContentType.JSON
                    )
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertEquals(RestStatus.CREATED, indexRequest.status());

            SearchResponse sourceSearchResponse = client().search(
                new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ).actionGet();
            assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
            Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
            assertNotNull(sourceDocMap);
            assertThat(sourceDocMap.get("idx"), is(equalTo(idx)));
            assertThat(sourceDocMap.get("key"), is(equalTo("key" + idx)));
            assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
            assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
            assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
        }

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = List.of("idx", "field1", "field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexPattern), "key", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> keyfield = (Map<?, ?>) properties.get("key");
        assertNotNull(keyfield);
        assertThat(keyfield.get("type"), is(equalTo("keyword")));
        assertThat(keyfield.get("doc_values"), is(false));

        // Validate document structure
        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(3L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(5)));
        assertThat(enrichDocument.get("key"), is(equalTo("key0")));
        assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocument.get("field2"), is(equalTo(2)));
        assertThat(enrichDocument.get("field5"), is(equalTo("value5")));

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
            IndexResponse indexRequest = client().index(
                new IndexRequest().index(sourceIndex)
                    .id(collidingDocId)
                    .routing(collidingDocId + idx)
                    .source(
                        "{"
                            + "\"idx\":"
                            + idx
                            + ","
                            + "\"key\":"
                            + "\"key"
                            + idx
                            + "\","
                            + "\"field1\":\"value1\","
                            + "\"field2\":2,"
                            + "\"field3\":\"ignored\","
                            + "\"field4\":\"ignored\","
                            + "\"field5\":\"value5\""
                            + "}",
                        XContentType.JSON
                    )
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertEquals(RestStatus.CREATED, indexRequest.status());

            SearchResponse sourceSearchResponse = client().search(
                new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ).actionGet();
            assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
            Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
            assertNotNull(sourceDocMap);
            assertThat(sourceDocMap.get("idx"), is(equalTo(idx)));
            assertThat(sourceDocMap.get("key"), is(equalTo("key" + idx)));
            assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
            assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
            assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));

            SearchResponse routingSearchResponse = client().search(
                new SearchRequest(sourceIndex).source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("_routing", collidingDocId + idx))
                )
            ).actionGet();
            assertEquals(1L, routingSearchResponse.getHits().getTotalHits().value);
        }

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = List.of("idx", "field1", "field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexPattern), "key", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> keyfield = (Map<?, ?>) properties.get("key");
        assertNotNull(keyfield);
        assertThat(keyfield.get("type"), is(equalTo("keyword")));
        assertThat(keyfield.get("doc_values"), is(false));

        // Validate document structure
        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(3L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(5)));
        assertThat(enrichDocument.get("key"), is(equalTo("key0")));
        assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocument.get("field2"), is(equalTo(2)));
        assertThat(enrichDocument.get("field5"), is(equalTo("value5")));

        // Validate removal of routing values
        for (int idx = 0; idx < numberOfSourceIndices; idx++) {
            SearchResponse routingSearchResponse = client().search(
                new SearchRequest(".enrich-test1").source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("_routing", collidingDocId + idx))
                )
            ).actionGet();
            assertEquals(0L, routingSearchResponse.getHits().getTotalHits().value);
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
            IndexResponse indexRequest = client().index(
                new IndexRequest().index(sourceIndex)
                    .id(randomAlphaOfLength(10))
                    .source(
                        "{"
                            + "\"idx\":"
                            + idx
                            + ","
                            + "\"key\":"
                            + "\"key\","
                            + "\"field1\":\"value1\","
                            + "\"field2\":2,"
                            + "\"field3\":\"ignored\","
                            + "\"field4\":\"ignored\","
                            + "\"field5\":\"value5\""
                            + "}",
                        XContentType.JSON
                    )
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertEquals(RestStatus.CREATED, indexRequest.status());

            SearchResponse sourceSearchResponse = client().search(
                new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
            ).actionGet();
            assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
            Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
            assertNotNull(sourceDocMap);
            assertThat(sourceDocMap.get("idx"), is(equalTo(idx)));
            assertThat(sourceDocMap.get("key"), is(equalTo("key")));
            assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
            assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
            assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
        }

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = List.of("idx", "field1", "field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndexPattern), "key", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> keyfield = (Map<?, ?>) properties.get("key");
        assertNotNull(keyfield);
        assertThat(keyfield.get("type"), is(equalTo("keyword")));
        assertThat(keyfield.get("doc_values"), is(false));

        // Validate document structure
        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(3L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(5)));
        assertThat(enrichDocument.get("key"), is(equalTo("key")));
        assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocument.get("field2"), is(equalTo(2)));
        assertThat(enrichDocument.get("field5"), is(equalTo("value5")));

        // Validate segments
        validateSegments(createdEnrichIndex, 3);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerNoSourceIndex() throws Exception {
        final String sourceIndex = "source-index";

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            Exception thrown = exception.get();
            assertThat(thrown, instanceOf(IndexNotFoundException.class));
            assertThat(thrown.getMessage(), containsString("no such index [" + sourceIndex + "]"));
        } else {
            fail("Expected exception but nothing was thrown");
        }
    }

    public void testRunnerNoSourceMapping() throws Exception {
        final String sourceIndex = "source-index";
        CreateIndexResponse createResponse = client().admin().indices().create(new CreateIndexRequest(sourceIndex)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            Exception thrown = exception.get();
            assertThat(thrown, instanceOf(ElasticsearchException.class));
            assertThat(
                thrown.getMessage(),
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
        } else {
            fail("Expected exception but nothing was thrown");
        }
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
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder))
            .actionGet();
        assertTrue(createResponse.isAcknowledged());

        String policyName = "test1";
        List<String> enrichFields = List.of("field2");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "nesting.key", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            Exception thrown = exception.get();
            assertThat(thrown, instanceOf(ElasticsearchException.class));
            assertThat(
                thrown.getMessage(),
                containsString(
                    "Enrich policy execution for ["
                        + policyName
                        + "] failed while validating field mappings for index ["
                        + sourceIndex
                        + "]"
                )
            );
            assertThat(
                thrown.getCause().getMessage(),
                containsString(
                    "Could not traverse mapping to field [nesting.key]. The [nesting" + "] field must be regular object but was [nested]."
                )
            );
        } else {
            fail("Expected exception but nothing was thrown");
        }
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
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder))
            .actionGet();
        assertTrue(createResponse.isAcknowledged());

        String policyName = "test1";
        List<String> enrichFields = List.of("nesting.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "key", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            Exception thrown = exception.get();
            assertThat(thrown, instanceOf(ElasticsearchException.class));
            assertThat(
                thrown.getMessage(),
                containsString(
                    "Enrich policy execution for ["
                        + policyName
                        + "] failed while validating field mappings for index ["
                        + sourceIndex
                        + "]"
                )
            );
            assertThat(
                thrown.getCause().getMessage(),
                containsString(
                    "Could not traverse mapping to field [nesting.field2]. "
                        + "The [nesting] field must be regular object but was [nested]."
                )
            );
        } else {
            fail("Expected exception but nothing was thrown");
        }
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
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder))
            .actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source(
                    "{" + "\"data\":{" + "\"field1\":\"value1\"," + "\"field2\":2," + "\"field3\":\"ignored\"" + "}" + "}",
                    XContentType.JSON
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
        assertNotNull(dataField);
        assertThat(dataField.get("field1"), is(equalTo("value1")));
        assertThat(dataField.get("field2"), is(equalTo(2)));
        assertThat(dataField.get("field3"), is(equalTo("ignored")));

        String policyName = "test1";
        List<String> enrichFields = List.of("data.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> data = (Map<?, ?>) properties.get("data");
        assertNotNull(data);
        assertThat(data.size(), is(equalTo(1)));
        Map<?, ?> dataProperties = (Map<?, ?>) data.get("properties");
        assertNotNull(dataProperties);
        assertThat(dataProperties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) dataProperties.get("field1");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("keyword")));
        assertThat(field1.get("doc_values"), is(false));

        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(1)));
        Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
        assertNotNull(resultDataField);
        assertThat(resultDataField.size(), is(equalTo(2)));
        assertThat(resultDataField.get("field1"), is(equalTo("value1")));
        assertThat(resultDataField.get("field2"), is(equalTo(2)));
        assertNull(resultDataField.get("field3"));

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
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder))
            .actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source(
                    "{" + "\"data\":{" + "\"field1\":\"value1\"," + "\"field2\":2," + "\"field3\":\"ignored\"" + "}" + "}",
                    XContentType.JSON
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
        assertNotNull(dataField);
        assertThat(dataField.get("field1"), is(equalTo("value1")));
        assertThat(dataField.get("field2"), is(equalTo(2)));
        assertThat(dataField.get("field3"), is(equalTo("ignored")));

        String policyName = "test1";
        List<String> enrichFields = List.of("data.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> data = (Map<?, ?>) properties.get("data");
        assertNotNull(data);
        assertThat(data.size(), is(equalTo(1)));
        Map<?, ?> dataProperties = (Map<?, ?>) data.get("properties");
        assertNotNull(dataProperties);
        assertThat(dataProperties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) dataProperties.get("field1");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("keyword")));
        assertThat(field1.get("doc_values"), is(false));

        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(1)));
        Map<?, ?> resultDataField = ((Map<?, ?>) enrichDocument.get("data"));
        assertNotNull(resultDataField);
        assertThat(resultDataField.size(), is(equalTo(2)));
        assertThat(resultDataField.get("field1"), is(equalTo("value1")));
        assertThat(resultDataField.get("field2"), is(equalTo(2)));
        assertNull(resultDataField.get("field3"));

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
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder))
            .actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source(
                    "{"
                        + "\"data\":{"
                        + "\"fields\":{"
                        + "\"field1\":\"value1\","
                        + "\"field2\":2,"
                        + "\"field3\":\"ignored\""
                        + "}"
                        + "}"
                        + "}",
                    XContentType.JSON
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        Map<?, ?> dataField = ((Map<?, ?>) sourceDocMap.get("data"));
        assertNotNull(dataField);
        Map<?, ?> fieldsField = ((Map<?, ?>) dataField.get("fields"));
        assertNotNull(fieldsField);
        assertThat(fieldsField.get("field1"), is(equalTo("value1")));
        assertThat(fieldsField.get("field2"), is(equalTo(2)));
        assertThat(fieldsField.get("field3"), is(equalTo("ignored")));

        String policyName = "test1";
        List<String> enrichFields = List.of("data.fields.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.fields.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> data = (Map<?, ?>) properties.get("data");
        assertNotNull(data);
        assertThat(data.size(), is(equalTo(1)));
        Map<?, ?> dataProperties = (Map<?, ?>) data.get("properties");
        assertNotNull(dataProperties);
        assertThat(dataProperties.size(), is(equalTo(1)));
        Map<?, ?> fields = (Map<?, ?>) dataProperties.get("fields");
        assertNotNull(fields);
        assertThat(fields.size(), is(equalTo(1)));
        Map<?, ?> fieldsProperties = (Map<?, ?>) fields.get("properties");
        assertNotNull(fieldsProperties);
        assertThat(fieldsProperties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) fieldsProperties.get("field1");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("keyword")));
        assertThat(field1.get("doc_values"), is(false));

        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
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
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .create(new CreateIndexRequest(sourceIndex).mapping(mappingBuilder))
            .actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source("{" + "\"data.field1\":\"value1\"," + "\"data.field2\":2," + "\"data.field3\":\"ignored\"" + "}", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("data.field1"), is(equalTo("value1")));
        assertThat(sourceDocMap.get("data.field2"), is(equalTo(2)));
        assertThat(sourceDocMap.get("data.field3"), is(equalTo("ignored")));

        String policyName = "test1";
        List<String> enrichFields = List.of("data.field2", "missingField");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> data = (Map<?, ?>) properties.get("data");
        assertNotNull(data);
        assertThat(data.size(), is(equalTo(1)));
        Map<?, ?> dataProperties = (Map<?, ?>) data.get("properties");
        assertNotNull(dataProperties);
        assertThat(dataProperties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) dataProperties.get("field1");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("keyword")));
        assertThat(field1.get("doc_values"), is(false));

        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(2)));
        assertThat(enrichDocument.get("data.field1"), is(equalTo("value1")));
        assertThat(enrichDocument.get("data.field2"), is(equalTo(2)));
        assertNull(enrichDocument.get("data.field3"));

        // Validate segments
        validateSegments(createdEnrichIndex, 1);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    public void testRunnerWithForceMergeRetry() throws Exception {
        final String sourceIndex = "source-index";
        IndexResponse indexRequest = client().index(
            new IndexRequest().index(sourceIndex)
                .id("id")
                .source(
                    "{"
                        + "\"field1\":\"value1\","
                        + "\"field2\":2,"
                        + "\"field3\":\"ignored\","
                        + "\"field4\":\"ignored\","
                        + "\"field5\":\"value5\""
                        + "}",
                    XContentType.JSON
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex).source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
        assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
        assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<ExecuteEnrichPolicyStatus> listener = createTestListener(latch, exception::set);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);
        Task asyncTask = testTaskManager.register("enrich", "policy_execution", new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

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
        ActionListener<ExecuteEnrichPolicyStatus> wrappedListener = new ActionListener<>() {
            @Override
            public void onResponse(ExecuteEnrichPolicyStatus policyExecutionResult) {
                testTaskManager.unregister(task);
                listener.onResponse(policyExecutionResult);
            }

            @Override
            public void onFailure(Exception e) {
                testTaskManager.unregister(task);
                listener.onFailure(e);
            }
        };
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
            wrappedListener,
            clusterService,
            client(),
            resolver,
            () -> createTime,
            randomIntBetween(1, 10000),
            randomIntBetween(3, 10)
        ) {
            @Override
            protected void ensureSingleSegment(String destinationIndexName, int attempt) {
                forceMergeAttempts.incrementAndGet();
                if (attempt == 1) {
                    // Put and flush a document to increase the number of segments, simulating not
                    // all segments were merged on the first try.
                    IndexResponse indexRequest = client().index(
                        new IndexRequest().index(createdEnrichIndex)
                            .source(unmergedDocument)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    ).actionGet();
                    assertEquals(RestStatus.CREATED, indexRequest.status());
                }
                super.ensureSingleSegment(destinationIndexName, attempt);
            }
        };

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate number of force merges
        assertThat(forceMergeAttempts.get(), equalTo(2));

        // Validate Index definition
        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(createdEnrichIndex));
        Settings settings = enrichIndex.getSettings().get(createdEnrichIndex);
        assertNotNull(settings);
        assertThat(settings.get("index.auto_expand_replicas"), is(equalTo("0-all")));

        // Validate Mapping
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).sourceAsMap();
        validateMappingMetadata(mapping, policyName, policy);
        assertThat(mapping.get("dynamic"), is("false"));
        Map<?, ?> properties = (Map<?, ?>) mapping.get("properties");
        assertNotNull(properties);
        assertThat(properties.size(), is(equalTo(1)));
        Map<?, ?> field1 = (Map<?, ?>) properties.get("field1");
        assertNotNull(field1);
        assertThat(field1.get("type"), is(equalTo("keyword")));
        assertThat(field1.get("doc_values"), is(false));

        // Validate document structure
        SearchResponse allEnrichDocs = client().search(
            new SearchRequest(".enrich-test1").source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()))
        ).actionGet();
        assertThat(allEnrichDocs.getHits().getTotalHits().value, equalTo(2L));
        for (String keyValue : List.of("value1", "value1.1")) {
            SearchResponse enrichSearchResponse = client().search(
                new SearchRequest(".enrich-test1").source(
                    SearchSourceBuilder.searchSource().query(QueryBuilders.matchQuery("field1", keyValue))
                )
            ).actionGet();

            assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
            Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
            assertNotNull(enrichDocument);
            assertThat(enrichDocument.size(), is(equalTo(3)));
            assertThat(enrichDocument.get("field1"), is(equalTo(keyValue)));
            assertThat(enrichDocument.get("field2"), is(equalTo(2)));
            assertThat(enrichDocument.get("field5"), is(equalTo("value5")));
        }

        // Validate segments
        validateSegments(createdEnrichIndex, 2);

        // Validate Index is read only
        ensureEnrichIndexIsReadOnly(createdEnrichIndex);
    }

    private EnrichPolicyRunner createPolicyRunner(
        String policyName,
        EnrichPolicy policy,
        ActionListener<ExecuteEnrichPolicyStatus> listener,
        Long createTime
    ) {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);
        Task asyncTask = testTaskManager.register("enrich", "policy_execution", new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

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
        ActionListener<ExecuteEnrichPolicyStatus> wrappedListener = new ActionListener<>() {
            @Override
            public void onResponse(ExecuteEnrichPolicyStatus policyExecutionResult) {
                testTaskManager.unregister(task);
                listener.onResponse(policyExecutionResult);
            }

            @Override
            public void onFailure(Exception e) {
                testTaskManager.unregister(task);
                listener.onFailure(e);
            }
        };
        return new EnrichPolicyRunner(
            policyName,
            policy,
            task,
            wrappedListener,
            clusterService,
            client(),
            resolver,
            () -> createTime,
            randomIntBetween(1, 10000),
            randomIntBetween(1, 10)
        );
    }

    private ActionListener<ExecuteEnrichPolicyStatus> createTestListener(
        final CountDownLatch latch,
        final Consumer<Exception> exceptionConsumer
    ) {
        return new LatchedActionListener<>(ActionListener.wrap((r) -> logger.info("Run complete"), exceptionConsumer), latch);
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
        IndicesSegmentResponse indicesSegmentResponse = client().admin()
            .indices()
            .segments(new IndicesSegmentsRequest(createdEnrichIndex))
            .actionGet();
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(createdEnrichIndex);
        assertNotNull(indexSegments);
        assertThat(indexSegments.getShards().size(), is(equalTo(1)));
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);
        assertThat(shardSegments.getShards().length, is(equalTo(1)));
        ShardSegments shard = shardSegments.getShards()[0];
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
}
