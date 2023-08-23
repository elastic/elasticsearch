/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.ccs.CrossClusterSearchIT;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CCSPointInTimeIT extends AbstractMultiClustersTestCase {

    public static final String REMOTE_CLUSTER = "remote_cluster";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), CrossClusterSearchIT.TestQueryBuilderPlugin.class);
    }

    public static class TestQueryBuilderPlugin extends Plugin implements SearchPlugin {
        public TestQueryBuilderPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            QuerySpec<ThrowingQueryBuilder> throwingSpec = new QuerySpec<>(ThrowingQueryBuilder.NAME, ThrowingQueryBuilder::new, p -> {
                throw new IllegalStateException("not implemented");
            });

            return List.of(throwingSpec);
        }
    }

    void indexDocs(Client client, String index, int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(index).setId(id).setSource("value", i).get();
        }
        client.admin().indices().prepareRefresh(index).get();
    }

    public void testBasic() {
        final Client localClient = client(LOCAL_CLUSTER);
        final Client remoteClient = client(REMOTE_CLUSTER);
        int localNumDocs = randomIntBetween(10, 50);
        assertAcked(localClient.admin().indices().prepareCreate("local_test"));
        indexDocs(localClient, "local_test", localNumDocs);

        int remoteNumDocs = randomIntBetween(10, 50);
        assertAcked(remoteClient.admin().indices().prepareCreate("remote_test"));
        indexDocs(remoteClient, "remote_test", remoteNumDocs);
        boolean includeLocalIndex = randomBoolean();
        List<String> indices = new ArrayList<>();
        if (includeLocalIndex) {
            indices.add(randomFrom("*", "local_*", "local_test"));
        }
        indices.add(randomFrom("*:*", "remote_cluster:*", "remote_cluster:remote_test"));
        String pitId = openPointInTime(indices.toArray(new String[0]), TimeValue.timeValueMinutes(2));
        try {
            if (randomBoolean()) {
                localClient.prepareIndex("local_test").setId("local_new").setSource().get();
                localClient.admin().indices().prepareRefresh().get();
            }
            if (randomBoolean()) {
                remoteClient.prepareIndex("remote_test").setId("remote_new").setSource().get();
                remoteClient.admin().indices().prepareRefresh().get();
            }
            SearchResponse resp = localClient.prepareSearch()
                .setPreference(null)
                .setQuery(new MatchAllQueryBuilder())
                .setPointInTime(new PointInTimeBuilder(pitId))
                .setSize(1000)
                .get();
            assertNoFailures(resp);
            assertHitCount(resp, (includeLocalIndex ? localNumDocs : 0) + remoteNumDocs);

            SearchResponse.Clusters clusters = resp.getClusters();
            int expectedNumClusters = 1 + (includeLocalIndex ? 1 : 0);
            assertThat(clusters.getTotal(), equalTo(expectedNumClusters));
            assertThat(clusters.getSuccessful(), equalTo(expectedNumClusters));
            assertThat(clusters.getSkipped(), equalTo(0));

            if (includeLocalIndex) {
                AtomicReference<SearchResponse.Cluster> localClusterRef = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                assertNotNull(localClusterRef);
                SearchResponse.Cluster localCluster = localClusterRef.get();
                assertThat(localCluster.getTotalShards(), equalTo(1));
                assertThat(localCluster.getSuccessfulShards(), equalTo(1));
                assertThat(localCluster.getFailedShards(), equalTo(0));
                assertThat(localCluster.getFailures().size(), equalTo(0));
                assertThat(localCluster.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertFalse(localCluster.isTimedOut());
            }

            AtomicReference<SearchResponse.Cluster> remoteClusterRef = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterRef);
            SearchResponse.Cluster remoteCluster = remoteClusterRef.get();
            assertThat(remoteCluster.getTotalShards(), equalTo(1));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(1));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));
            assertThat(remoteCluster.getFailures().size(), equalTo(0));
            assertThat(remoteCluster.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertFalse(remoteCluster.isTimedOut());

        } finally {
            closePointInTime(pitId);
        }
    }

    public void testFailuresOnOneShardsWithPointInTime() throws ExecutionException, InterruptedException {
        final Client localClient = client(LOCAL_CLUSTER);
        final Client remoteClient = client(REMOTE_CLUSTER);
        int localNumDocs = randomIntBetween(10, 50);
        int numShards = randomIntBetween(2, 4);
        Settings clusterSettings = indexSettings(numShards, randomIntBetween(0, 1)).build();
        assertAcked(localClient.admin().indices().prepareCreate("local_test").setSettings(clusterSettings));
        indexDocs(localClient, "local_test", localNumDocs);

        int remoteNumDocs = randomIntBetween(10, 50);
        assertAcked(remoteClient.admin().indices().prepareCreate("remote_test").setSettings(clusterSettings));
        indexDocs(remoteClient, "remote_test", remoteNumDocs);
        boolean includeLocalIndex = randomBoolean();
        List<String> indices = new ArrayList<>();
        if (includeLocalIndex) {
            indices.add(randomFrom("*", "local_*", "local_test"));
        }
        indices.add(randomFrom("*:*", "remote_cluster:*", "remote_cluster:remote_test"));
        String pitId = openPointInTime(indices.toArray(new String[0]), TimeValue.timeValueMinutes(2));
        try {
            if (randomBoolean()) {
                localClient.prepareIndex("local_test").setId("local_new").setSource().get();
                localClient.admin().indices().prepareRefresh().get();
            }
            if (randomBoolean()) {
                remoteClient.prepareIndex("remote_test").setId("remote_new").setSource().get();
                remoteClient.admin().indices().prepareRefresh().get();
            }
            PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();

            // shardId 0 means to throw the Exception only on shard 0; all others should work
            ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10).pointInTimeBuilder(new PointInTimeBuilder(pitId)));
            client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

            SearchResponse searchResponse = queryFuture.get();

            SearchResponse.Clusters clusters = searchResponse.getClusters();
            int expectedNumClusters = 1 + (includeLocalIndex ? 1 : 0);
            assertThat(clusters.getTotal(), equalTo(expectedNumClusters));
            assertThat(clusters.getSuccessful(), equalTo(expectedNumClusters));
            assertThat(clusters.getSkipped(), equalTo(0));

            if (includeLocalIndex) {
                AtomicReference<SearchResponse.Cluster> localClusterRef = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                assertNotNull(localClusterRef);
                SearchResponse.Cluster localCluster = localClusterRef.get();
                assertThat(localCluster.getTotalShards(), equalTo(numShards));
                assertThat(localCluster.getSuccessfulShards(), equalTo(numShards - 1));
                assertThat(localCluster.getFailedShards(), equalTo(1));
                assertThat(localCluster.getFailures().size(), equalTo(1));
                assertThat(localCluster.getFailures().get(0).reason(), containsString("index corrupted"));
                assertThat(localCluster.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertFalse(localCluster.isTimedOut());
            }

            AtomicReference<SearchResponse.Cluster> remoteClusterRef = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterRef);
            SearchResponse.Cluster remoteCluster = remoteClusterRef.get();
            assertThat(remoteCluster.getTotalShards(), equalTo(numShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(numShards - 1));
            assertThat(remoteCluster.getFailedShards(), equalTo(1));
            assertThat(remoteCluster.getFailures().size(), equalTo(1));
            assertThat(remoteCluster.getFailures().get(0).reason(), containsString("index corrupted"));
            assertThat(remoteCluster.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertFalse(remoteCluster.isTimedOut());

        } finally {
            closePointInTime(pitId);
        }
    }

    private String openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(OpenPointInTimeAction.INSTANCE, request).actionGet();
        return response.getPointInTimeId();
    }

    private void closePointInTime(String readerId) {
        client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(readerId)).actionGet();
    }
}
