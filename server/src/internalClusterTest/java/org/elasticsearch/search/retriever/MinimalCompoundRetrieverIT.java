/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class MinimalCompoundRetrieverIT extends AbstractMultiClustersTestCase {

    // CrossClusterSearchIT
    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    public void testSimpleSearch() throws ExecutionException, InterruptedException {
        RetrieverBuilder compoundRetriever = new CompoundRetriever(Collections.emptyList());
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.source(new SearchSourceBuilder().retriever(compoundRetriever));
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            SearchResponse.Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.FAILED), equalTo(0));
            assertThat(response.getHits().getTotalHits().value(), equalTo(testClusterInfo.get("total_docs")));
        });
    }

    private Map<String, Object> setupTwoClusters() {
        int totalDocs = 0;
        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(2, 10);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(localSettings)
                .setMapping("some_field", "type=keyword")
        );
        totalDocs += indexDocs(client(LOCAL_CLUSTER), localIndex);

        String remoteIndex = "prod";
        int numShardsRemote = randomIntBetween(2, 10);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(indexSettings(numShardsRemote, randomIntBetween(0, 1)))
                .setMapping("some_field", "type=keyword")
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        totalDocs += indexDocs(client(REMOTE_CLUSTER), remoteIndex);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", remoteIndex);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);
        clusterInfo.put("total_docs", (long) totalDocs);
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(500, 1200);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("some_field", i).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    public static class CompoundRetriever extends RetrieverBuilder {

        private final List<RetrieverBuilder> sources;

        public CompoundRetriever(List<RetrieverBuilder> sources) {
            this.sources = sources;
        }

        @Override
        public boolean isCompound() {
            return true;
        }

        @Override
        public QueryBuilder topDocsQuery() {
            throw new UnsupportedOperationException("should not be called");
        }

        @Override
        public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
            if (ctx.getPointInTimeBuilder() == null) {
                throw new IllegalStateException("PIT is required");
            }
            if (sources.isEmpty()) {
                StandardRetrieverBuilder standardRetrieverBuilder = new StandardRetrieverBuilder();
                standardRetrieverBuilder.queryBuilder = new MatchAllQueryBuilder();
                return standardRetrieverBuilder;
            }
            return sources.get(0);
        }

        @Override
        public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
            throw new UnsupportedOperationException("should not be called");
        }

        @Override
        public String getName() {
            return "compound_retriever";
        }

        @Override
        protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
            // no-op
        }

        @Override
        protected boolean doEquals(Object o) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }
}
