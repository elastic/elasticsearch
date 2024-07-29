/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class RetrieverRewriteIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockSearchService.TestPlugin.class);
    }

    private static String INDEX_DOCS = "docs";
    private static String INDEX_QUERIES = "queries";
    private static final String ID_FIELD = "_id";
    private static final String QUERY_FIELD = "query";

    @Before
    public void setup() throws Exception {
        createIndex(INDEX_DOCS);
        index(INDEX_DOCS, "doc_0", "{}");
        index(INDEX_DOCS, "doc_1", "{}");
        index(INDEX_DOCS, "doc_2", "{}");
        refresh(INDEX_DOCS);

        createIndex(INDEX_QUERIES);
        index(INDEX_QUERIES, "query_0", "{ \"" + QUERY_FIELD + "\": \"doc_2\"}");
        index(INDEX_QUERIES, "query_1", "{ \"" + QUERY_FIELD + "\": \"doc_1\"}");
        index(INDEX_QUERIES, "query_2", "{ \"" + QUERY_FIELD + "\": \"doc_0\"}");
        refresh(INDEX_QUERIES);
    }

    public void testRewrite() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        StandardRetrieverBuilder standard = new StandardRetrieverBuilder();
        standard.queryBuilder = QueryBuilders.termQuery(ID_FIELD, "doc_0");
        source.retriever(new AssertingRetrieverBuilder(standard));
        SearchRequestBuilder req = client().prepareSearch(INDEX_DOCS, INDEX_QUERIES).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
            assertThat(resp.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_0"));
        });
    }

    public void testRewriteCompound() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.retriever(new AssertingCompoundRetrieverBuilder("query_0"));
        SearchRequestBuilder req = client().prepareSearch(INDEX_DOCS, INDEX_QUERIES).setSource(source);
        ElasticsearchAssertions.assertResponse(req, resp -> {
            assertNull(resp.pointInTimeId());
            assertNotNull(resp.getHits().getTotalHits());
            assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
            assertThat(resp.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
        });
    }

    public void testRewriteCompoundRetrieverShouldThrowForPartialResults() throws Exception {
        final String testIndex = "test";
        createIndex(testIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        for (int i = 0; i < 50; i++) {
            index(testIndex, "doc_" + i, "{}");
        }
        refresh(testIndex);

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.retriever(new AssertingCompoundRetrieverBuilder("doc_0"));
        final String randomDataNode = internalCluster().getNodeNameThat(
            settings -> DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE)
        );
        try {
            ensureGreen(testIndex);
            if (false == internalCluster().stopNode(randomDataNode)) {
                throw new IllegalStateException("node did not stop");
            }
            assertBusy(() -> {
                ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth(testIndex)
                    .setWaitForStatus(ClusterHealthStatus.RED) // we are now known red because the primary shard is missing
                    .setWaitForEvents(Priority.LANGUID) // ensures that the update has occurred
                    .execute()
                    .actionGet();
                assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
            });
            SearchPhaseExecutionException ex = expectThrows(
                SearchPhaseExecutionException.class,
                client().prepareSearch(testIndex).setSource(source)::get
            );
            assertThat(
                ex.getDetailedMessage(),
                containsString("[open_point_in_time] action requires all shards to be available. Missing shards")
            );
        } finally {
            internalCluster().restartNode(randomDataNode);
        }
    }

    private static class AssertingRetrieverBuilder extends RetrieverBuilder {
        private final RetrieverBuilder innerRetriever;

        private AssertingRetrieverBuilder(RetrieverBuilder innerRetriever) {
            this.innerRetriever = innerRetriever;
        }

        @Override
        public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
            assertNull(ctx.getPointInTimeBuilder());
            assertNull(ctx.convertToInnerHitsRewriteContext());
            assertNull(ctx.convertToCoordinatorRewriteContext());
            assertNull(ctx.convertToIndexMetadataContext());
            assertNull(ctx.convertToSearchExecutionContext());
            assertNull(ctx.convertToDataRewriteContext());
            var newRetriever = innerRetriever.rewrite(ctx);
            if (newRetriever != innerRetriever) {
                return new AssertingRetrieverBuilder(newRetriever);
            }
            return this;
        }

        @Override
        public void extractToSearchSourceBuilder(SearchSourceBuilder sourceBuilder, boolean compoundUsed) {
            assertNull(sourceBuilder.retriever());
            innerRetriever.extractToSearchSourceBuilder(sourceBuilder, compoundUsed);
        }

        @Override
        public String getName() {
            return "asserting";
        }

        @Override
        protected void doToXContent(XContentBuilder builder, Params params) throws IOException {}

        @Override
        protected boolean doEquals(Object o) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return innerRetriever.doHashCode();
        }
    }

    private static class AssertingCompoundRetrieverBuilder extends RetrieverBuilder {
        private final String id;
        private final SetOnce<RetrieverBuilder> innerRetriever;

        private AssertingCompoundRetrieverBuilder(String id) {
            this.id = id;
            this.innerRetriever = new SetOnce<>(null);
        }

        private AssertingCompoundRetrieverBuilder(String id, SetOnce<RetrieverBuilder> innerRetriever) {
            this.id = id;
            this.innerRetriever = innerRetriever;
        }

        @Override
        public boolean isCompound() {
            return true;
        }

        @Override
        public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
            assertNotNull(ctx.getPointInTimeBuilder());
            assertNull(ctx.convertToInnerHitsRewriteContext());
            assertNull(ctx.convertToCoordinatorRewriteContext());
            assertNull(ctx.convertToIndexMetadataContext());
            assertNull(ctx.convertToSearchExecutionContext());
            assertNull(ctx.convertToDataRewriteContext());
            if (innerRetriever.get() != null) {
                return this;
            }
            SetOnce<RetrieverBuilder> innerRetriever = new SetOnce<>();
            ctx.registerAsyncAction((client, actionListener) -> {
                SearchSourceBuilder source = new SearchSourceBuilder().pointInTimeBuilder(ctx.getPointInTimeBuilder())
                    .query(QueryBuilders.termQuery(ID_FIELD, id))
                    .fetchField(QUERY_FIELD);
                client.search(new SearchRequest().source(source), new ActionListener<>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        String query = response.getHits().getAt(0).field(QUERY_FIELD).getValue();
                        StandardRetrieverBuilder standard = new StandardRetrieverBuilder();
                        standard.queryBuilder = QueryBuilders.termQuery(ID_FIELD, query);
                        innerRetriever.set(standard);
                        actionListener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        actionListener.onFailure(e);
                    }
                });
            });
            return new AssertingCompoundRetrieverBuilder(id, innerRetriever);
        }

        @Override
        public void extractToSearchSourceBuilder(SearchSourceBuilder sourceBuilder, boolean compoundUsed) {
            assertNull(sourceBuilder.retriever());
            innerRetriever.get().extractToSearchSourceBuilder(sourceBuilder, compoundUsed);
        }

        @Override
        public String getName() {
            return "asserting";
        }

        @Override
        protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
            throw new AssertionError("not implemented");
        }

        @Override
        protected boolean doEquals(Object o) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return id.hashCode();
        }
    }
}
