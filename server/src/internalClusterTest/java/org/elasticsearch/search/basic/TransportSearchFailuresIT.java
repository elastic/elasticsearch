/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.basic;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportSearchFailuresIT extends ESIntegTestCase {

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FailingQueryPlugin.class);
    }

    public void testFailedSearchWithWrongQuery() throws Exception {
        logger.info("Start Testing failed search with wrong query");
        assertAcked(prepareCreate("test", 1).setMapping("foo", "type=geo_point"));

        NumShards test = getNumShards("test");

        for (int i = 0; i < 100; i++) {
            index(client(), Integer.toString(i), "test", i);
        }
        RefreshResponse refreshResponse = client().admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(test.numPrimaries));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(
                        searchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz"))))
                        .actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
                fail("search should fail");
            } catch (ElasticsearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        allowNodes("test", 2);
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes(">=2").get()
                .isTimedOut(), equalTo(false));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client()
                .admin()
                .cluster()
                .health(clusterHealthRequest("test").waitForYellowStatus().waitForNoRelocatingShards(true).waitForEvents(Priority.LANGUID)
                        .waitForActiveShards(test.totalNumShards)).actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), anyOf(equalTo(ClusterHealthStatus.YELLOW), equalTo(ClusterHealthStatus.GREEN)));
        assertThat(clusterHealth.getActiveShards(), equalTo(test.totalNumShards));

        refreshResponse = client().admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));

        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(
                        searchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz"))))
                        .actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
                fail("search should fail");
            } catch (ElasticsearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        logger.info("Done Testing failed search");
    }

    private void index(Client client, String id, String nameValue, int age) throws IOException {
        client.index(Requests.indexRequest("test").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private XContentBuilder source(String id, String nameValue, int age) throws IOException {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return jsonBuilder().startObject()
                .field("id", id)
                .field("name", nameValue + id)
                .field("age", age)
                .field("multi", multi.toString())
                .endObject();
    }

    public void testRetryQueryPhase() throws Exception {
        assertAcked(prepareCreate("test"));

        NumShards numShards = getNumShards("test");
        int numPrimaries = numShards.numPrimaries;

        for (int i = 0; i < 10; i++) {
            index(client(), Integer.toString(i), "test", i);
        }
        refresh("test");

        SearchResponse searchResponse = client().prepareSearch("test")
            .setSource(new SearchSourceBuilder().query(new FailingQueryBuilder()))
            .setSearchType(SearchType.QUERY_RETRY_THEN_FETCH)
            .get();
        assertThat(searchResponse.getTotalShards(), equalTo(numPrimaries));
        assertThat(searchResponse.getSuccessfulShards(), equalTo(numPrimaries));
        assertThat(searchResponse.getFailedShards(), equalTo(0));
    }

    public static class FailingQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<>(FailingQueryBuilder.NAME, FailingQueryBuilder::new, parserContext -> new FailingQueryBuilder()));
        }
    }

    // TODO(jtibs): this is a big hack and not a solid testing strategy
    private static class FailingQueryBuilder extends AbstractQueryBuilder<FailingQueryBuilder> {
        public static final String NAME = "failing";
        private static final AtomicInteger counter = new AtomicInteger();

        FailingQueryBuilder() {}

        FailingQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            printBoostAndQueryName(builder);
            builder.endObject();
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            if (context.getShardRequestIndex() == 0 && counter.getAndIncrement() == 0) {
                throw new RuntimeException("Failed to execute query");
            }
            return new MatchAllDocsQuery();
        }

        @Override
        protected boolean doEquals(FailingQueryBuilder other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
