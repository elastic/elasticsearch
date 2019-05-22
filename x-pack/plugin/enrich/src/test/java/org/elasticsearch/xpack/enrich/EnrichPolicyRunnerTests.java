/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import static org.hamcrest.CoreMatchers.equalTo;
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
        ).get();

        logger.info("Status: " + indexRequest.status().getStatus());

        SearchResponse sourceSearchResponse = client().search(
            new SearchRequest(sourceIndex)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).get();

        assertThat(sourceSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> sourceDocMap = sourceSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(sourceDocMap);
        assertThat(sourceDocMap.get("field1"), is(equalTo("value1")));
        assertThat(sourceDocMap.get("field2"), is(equalTo(2)));
        assertThat(sourceDocMap.get("field3"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field4"), is(equalTo("ignored")));
        assertThat(sourceDocMap.get("field5"), is(equalTo("value5")));

        logger.info("Created Doc");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);

        final long createTime = randomNonNegativeLong();

        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        List<String> enrichFields = new ArrayList<>();
        enrichFields.add("field2");
        enrichFields.add("field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of(sourceIndex), "field1", enrichFields);
        String policyName = "test1";

        ActionListener<PolicyExecutionResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(PolicyExecutionResult policyExecutionResult) {
                logger.info("Run complete");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Run failed");
                exception.set(e);
                latch.countDown();
            }
        };

        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(policyName, policy, listener, clusterService,
            client(), resolver, () -> createTime, randomIntBetween(1, 10000));

        logger.info("Starting policy run");

        enrichPolicyRunner.run();

        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        // Validate Index definition
        String createdEnrichIndex = ".enrich-test1-" + createTime;
        GetIndexResponse enrichIndex = client().admin().indices()
            .getIndex(new GetIndexRequest().indices(".enrich-test1")).get();
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
                    .query(QueryBuilders.matchAllQuery()))).get();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(3)));
        assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocument.get("field2"), is(equalTo(2)));
        assertThat(enrichDocument.get("field5"), is(equalTo("value5")));

        // Validate segments
        IndicesSegmentResponse indicesSegmentResponse = client().admin().indices()
            .segments(new IndicesSegmentsRequest(createdEnrichIndex)).get();
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(createdEnrichIndex);
        assertNotNull(indexSegments);
        assertThat(indexSegments.getShards().size(), is(equalTo(1)));
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);
        assertThat(shardSegments.getShards().length, is(equalTo(1)));
        ShardSegments shard = shardSegments.getShards()[0];
        assertThat(shard.getSegments().size(), is(equalTo(1)));
        Segment segment = shard.getSegments().iterator().next();
        assertThat(segment.getNumDocs(), is(equalTo(1)));
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

            logger.info("Status: " + indexRequest.status().getStatus());

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

        logger.info("Created Docs");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);

        final long createTime = randomNonNegativeLong();

        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = new ArrayList<>();
        enrichFields.add("idx");
        enrichFields.add("field2");
        enrichFields.add("field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of(sourceIndexPattern), "field1",
            enrichFields);
        String policyName = "test1";

        ActionListener<PolicyExecutionResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(PolicyExecutionResult policyExecutionResult) {
                logger.info("Run complete");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Run failed");
                exception.set(e);
                latch.countDown();
            }
        };

        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(policyName, policy, listener, clusterService,
            client(), resolver, () -> createTime, randomIntBetween(1, 10000));

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
                    .query(QueryBuilders.matchAllQuery()))).get();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(3L));
        Map<String, Object> enrichDocument = enrichSearchResponse.getHits().iterator().next().getSourceAsMap();
        assertNotNull(enrichDocument);
        assertThat(enrichDocument.size(), is(equalTo(4)));
        assertThat(enrichDocument.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocument.get("field2"), is(equalTo(2)));
        assertThat(enrichDocument.get("field5"), is(equalTo("value5")));

        // Validate segments
        IndicesSegmentResponse indicesSegmentResponse = client().admin().indices()
            .segments(new IndicesSegmentsRequest(createdEnrichIndex)).get();
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(createdEnrichIndex);
        assertNotNull(indexSegments);
        assertThat(indexSegments.getShards().size(), is(equalTo(1)));
        IndexShardSegments shardSegments = indexSegments.getShards().get(0);
        assertNotNull(shardSegments);
        assertThat(shardSegments.getShards().length, is(equalTo(1)));
        ShardSegments shard = shardSegments.getShards()[0];
        assertThat(shard.getSegments().size(), is(equalTo(1)));
        Segment segment = shard.getSegments().iterator().next();
        assertThat(segment.getNumDocs(), is(equalTo(3)));
    }
}
