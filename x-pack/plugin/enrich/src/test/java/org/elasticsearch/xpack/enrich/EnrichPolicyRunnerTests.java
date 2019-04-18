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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class EnrichPolicyRunnerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(ReindexPlugin.class);
    }

    public void testRunner() throws Exception {
        logger.info("Starting Test");

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
        EnrichStore enrichStore = new EnrichStore(clusterService);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);

        final long createTime = randomNonNegativeLong();

        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(clusterService, client(), enrichStore, resolver,
            () -> createTime);

        logger.info("Runner created");

        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        List<String> enrichFields = new ArrayList<>();
        enrichFields.add("field2");
        enrichFields.add("field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, sourceIndex, "field1", enrichFields, "");
        String policyName = "test1";

        logger.info("Starting policy run");

        enrichPolicyRunner.runPolicy(policyName, policy, new ActionListener<PolicyExecutionResult>() {
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
        });

        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        GetIndexResponse enrichIndex = client().admin().indices()
            .getIndex(new GetIndexRequest().indices(".enrich-test1")).get();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(".enrich-test1-" + createTime));

        SearchResponse enrichSearchResponse = client().search(
            new SearchRequest(".enrich-test1")
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.matchAllQuery()))).get();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(1L));
        Map<String, Object> enrichDocMap = enrichSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(enrichDocMap);
        assertThat(enrichDocMap.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocMap.get("field2"), is(equalTo(2)));
        assertThat(enrichDocMap.get("field3"), is(nullValue()));
        assertThat(enrichDocMap.get("field4"), is(nullValue()));
        assertThat(enrichDocMap.get("field5"), is(equalTo("value5")));
    }

    public void testRunnerMultiSource() throws Exception {
        logger.info("Starting Test");

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
        EnrichStore enrichStore = new EnrichStore(clusterService);
        IndexNameExpressionResolver resolver = getInstanceFromNode(IndexNameExpressionResolver.class);

        final long createTime = randomNonNegativeLong();

        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(clusterService, client(), enrichStore, resolver,
            () -> createTime);

        logger.info("Runner created");

        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        String sourceIndexPattern = baseSourceName + "*";
        List<String> enrichFields = new ArrayList<>();
        enrichFields.add("idx");
        enrichFields.add("field2");
        enrichFields.add("field5");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, sourceIndexPattern, "field1", enrichFields, "");
        String policyName = "test1";

        logger.info("Starting policy run");

        enrichPolicyRunner.runPolicy(policyName, policy, new ActionListener<PolicyExecutionResult>() {
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
        });

        latch.await();
        if (exception.get() != null) {
            throw exception.get();
        }

        GetIndexResponse enrichIndex = client().admin().indices().getIndex(new GetIndexRequest().indices(".enrich-test1")).actionGet();
        assertThat(enrichIndex.getIndices().length, equalTo(1));
        assertThat(enrichIndex.getIndices()[0], equalTo(".enrich-test1-" + createTime));

        SearchResponse enrichSearchResponse = client()
            .search(
                new SearchRequest(".enrich-test1")
                    .source(
                        SearchSourceBuilder.searchSource()
                            .query(QueryBuilders.matchAllQuery())
                            .sort("idx")
                    )
            ).actionGet();

        assertThat(enrichSearchResponse.getHits().getTotalHits().value, equalTo(3L));
        Map<String, Object> enrichDocMap = enrichSearchResponse.getHits().getAt(0).getSourceAsMap();
        assertNotNull(enrichDocMap);
        assertThat(enrichDocMap.get("idx"), is(equalTo(0)));
        assertThat(enrichDocMap.get("field1"), is(equalTo("value1")));
        assertThat(enrichDocMap.get("field2"), is(equalTo(2)));
        assertThat(enrichDocMap.get("field3"), is(nullValue()));
        assertThat(enrichDocMap.get("field4"), is(nullValue()));
        assertThat(enrichDocMap.get("field5"), is(equalTo("value5")));
    }
}
