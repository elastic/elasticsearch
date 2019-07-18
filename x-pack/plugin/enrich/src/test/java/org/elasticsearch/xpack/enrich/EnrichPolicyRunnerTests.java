/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
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
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicyDefinition;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

public class EnrichPolicyRunnerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(ReindexPlugin.class);
    }

    public void testRunner() throws Exception {
        final String sourceIndex = "source-index";
        IndexResponse indexRequest = client().index(new IndexRequest()
            .index(sourceIndex)
            .id("id")
            .source(
                "{" +
                    "\"field1\":\"value1\"," +
                    "\"field2\":2," +
                    "\"field3\":\"ignored\"," +
                    "\"field4\":\"ignored\"," +
                    "\"field5\":\"value5\"" +
                "}",
                XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
        assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
        assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));

        List<String> enrichFields = List.of("field2", "field5");
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
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
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).get("_doc").sourceAsMap();
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
            new SearchRequest(".enrich-test1")
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();

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

    public void testRunnerMultiSource() throws Exception {
        String baseSourceName = "source-index-";
        int numberOfSourceIndices = 3;
        for (int idx = 0; idx < numberOfSourceIndices; idx++) {
            final String sourceIndex = baseSourceName + idx;
            IndexResponse indexRequest = client().index(new IndexRequest()
                .index(sourceIndex)
                .id(randomAlphaOfLength(10))
                .source(
                    "{" +
                        "\"idx\":" + idx + "," +
                        "\"field1\":\"value1\"," +
                        "\"field2\":2," +
                        "\"field3\":\"ignored\"," +
                        "\"field4\":\"ignored\"," +
                        "\"field5\":\"value5\"" +
                    "}",
                    XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();
            assertEquals(RestStatus.CREATED, indexRequest.status());

            SearchResponse sourceSearchResponse = client().search(
                new SearchRequest(sourceIndex)
                    .source(SearchSourceBuilder.searchSource()
                        .query(QueryBuilders.matchAllQuery()))).actionGet();
            assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
            Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
            assertNotNull(sourceDocMap);
            assertThat(sourceDocMap.get("idx"), is(equalTo(idx)));
            assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
            assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
            assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
            assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));
        }

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = List.of("idx", "field2", "field5");
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndexPattern), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
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
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).get("_doc").sourceAsMap();
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
            new SearchRequest(".enrich-test1")
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();
        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(3L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(4)));
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
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
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
        EnrichPolicyDefinition policy = new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex),
            "field1", enrichFields);
        String policyName = "test1";

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            Exception thrown = exception.get();
            assertThat(thrown, instanceOf(ElasticsearchException.class));
            assertThat(thrown.getMessage(), containsString("Enrich policy execution for [" + policyName +
                "] failed. No mapping available on source [" + sourceIndex + "] included in [[" + sourceIndex + "]]"));
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
        CreateIndexResponse createResponse = client().admin().indices().create(new CreateIndexRequest(sourceIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        String policyName = "test1";
        List<String> enrichFields = List.of("field2");
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "nesting.key", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            Exception thrown = exception.get();
            assertThat(thrown, instanceOf(ElasticsearchException.class));
            assertThat(thrown.getMessage(), containsString("Enrich policy execution for [" + policyName +
                "] failed while validating field mappings for index [" + sourceIndex + "]"));
            assertThat(thrown.getCause().getMessage(), containsString("Could not traverse mapping to field [nesting.key]. The [nesting" +
                "] field must be regular object but was [nested]."));
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
        CreateIndexResponse createResponse = client().admin().indices().create(new CreateIndexRequest(sourceIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        String policyName = "test1";
        List<String> enrichFields = List.of("nesting.field2", "missingField");
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "key", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
        EnrichPolicyRunner enrichPolicyRunner = createPolicyRunner(policyName, policy, listener, createTime);

        logger.info("Starting policy run");
        enrichPolicyRunner.run();
        latch.await();
        if (exception.get() != null) {
            Exception thrown = exception.get();
            assertThat(thrown, instanceOf(ElasticsearchException.class));
            assertThat(thrown.getMessage(), containsString("Enrich policy execution for [" + policyName +
                "] failed while validating field mappings for index [" + sourceIndex + "]"));
            assertThat(thrown.getCause().getMessage(), containsString("Could not traverse mapping to field [nesting.field2]. " +
                "The [nesting] field must be regular object but was [nested]."));
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
        CreateIndexResponse createResponse = client().admin().indices().create(new CreateIndexRequest(sourceIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(new IndexRequest()
            .index(sourceIndex)
            .id("id")
            .source(
                "{" +
                    "\"data\":{" +
                        "\"field1\":\"value1\"," +
                        "\"field2\":2," +
                        "\"field3\":\"ignored\"" +
                    "}" +
                "}",
                XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();
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
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
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
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).get("_doc").sourceAsMap();
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
            new SearchRequest(".enrich-test1")
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();

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
        CreateIndexResponse createResponse = client().admin().indices().create(new CreateIndexRequest(sourceIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(new IndexRequest()
            .index(sourceIndex)
            .id("id")
            .source(
                "{" +
                    "\"data\":{" +
                        "\"field1\":\"value1\"," +
                        "\"field2\":2," +
                        "\"field3\":\"ignored\"" +
                    "}" +
                "}",
                XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();
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
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
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
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).get("_doc").sourceAsMap();
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
            new SearchRequest(".enrich-test1")
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();

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
        CreateIndexResponse createResponse = client().admin().indices().create(new CreateIndexRequest(sourceIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(new IndexRequest()
            .index(sourceIndex)
            .id("id")
            .source(
                "{" +
                    "\"data\":{" +
                        "\"fields\":{" +
                            "\"field1\":\"value1\"," +
                            "\"field2\":2," +
                            "\"field3\":\"ignored\"" +
                        "}" +
                    "}" +
                "}",
                XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();
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
        EnrichPolicyDefinition policy = new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex),
            "data.fields.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
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
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).get("_doc").sourceAsMap();
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
            new SearchRequest(".enrich-test1")
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();

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
        CreateIndexResponse createResponse = client().admin().indices().create(new CreateIndexRequest(sourceIndex)
            .mapping(MapperService.SINGLE_MAPPING_NAME, mappingBuilder)).actionGet();
        assertTrue(createResponse.isAcknowledged());

        IndexResponse indexRequest = client().index(new IndexRequest()
            .index(sourceIndex)
            .id("id")
            .source(
                "{" +
                    "\"data.field1\":\"value1\"," +
                    "\"data.field2\":2," +
                    "\"data.field3\":\"ignored\"" +
                "}",
                XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertEquals(RestStatus.CREATED, indexRequest.status());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();
        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("data.field1"), is(equalTo("value1")));
        assertThat(sourceDocMap.get("data.field2"), is(equalTo(2)));
        assertThat(sourceDocMap.get("data.field3"), is(equalTo("ignored")));

        String policyName = "test1";
        List<String> enrichFields = List.of("data.field2", "missingField");
        EnrichPolicyDefinition policy =
            new EnrichPolicyDefinition(EnrichPolicyDefinition.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "data.field1", enrichFields);

        final long createTime = randomNonNegativeLong();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<PolicyExecutionResult> listener = createTestListener(latch, exception::set);
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
        Map<String, Object> mapping = enrichIndex.getMappings().get(createdEnrichIndex).get("_doc").sourceAsMap();
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
            new SearchRequest(".enrich-test1")
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).actionGet();

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

    private EnrichPolicyRunner createPolicyRunner(String policyName, EnrichPolicyDefinition policy,
                                                  ActionListener<PolicyExecutionResult> listener,
                                                  Long createTime) {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);
        return new EnrichPolicyRunner(policyName, policy, listener, clusterService, client(), resolver, () -> createTime,
            randomIntBetween(1, 10000));
    }

    private ActionListener<PolicyExecutionResult> createTestListener(final CountDownLatch latch,
                                                                     final Consumer<Exception> exceptionConsumer) {
        return new LatchedActionListener<>(ActionListener.wrap((r) -> logger.info("Run complete"), exceptionConsumer), latch);
    }

    private void validateSegments(String createdEnrichIndex, int expectedDocs) {
        IndicesSegmentResponse indicesSegmentResponse = client().admin().indices()
            .segments(new IndicesSegmentsRequest(createdEnrichIndex)).actionGet();
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
        ElasticsearchException expected = expectThrows(ElasticsearchException.class, () -> client().index(new IndexRequest()
            .index(createdEnrichIndex)
            .id(randomAlphaOfLength(10))
            .source(Map.of(randomAlphaOfLength(6), randomAlphaOfLength(10)))).actionGet());

        assertThat(expected.getMessage(), containsString("index [" + createdEnrichIndex +
            "] blocked by: [FORBIDDEN/8/index write (api)]"));
    }
}
