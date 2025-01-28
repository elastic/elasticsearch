/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.ccs.CrossClusterSearchIT;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.hamcrest.MatcherAssert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CCSPointInTimeIT extends AbstractMultiClustersTestCase {

    public static final String REMOTE_CLUSTER = "remote_cluster";

    @Override
    protected List<String> remoteClusterAlias() {
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
        assertAcked(
            localClient.admin().indices().prepareCreate("local_test").setSettings(Settings.builder().put("index.number_of_shards", 3))
        );
        indexDocs(localClient, "local_test", localNumDocs);

        int remoteNumDocs = randomIntBetween(10, 50);
        assertAcked(
            remoteClient.admin().indices().prepareCreate("remote_test").setSettings(Settings.builder().put("index.number_of_shards", 3))
        );
        indexDocs(remoteClient, "remote_test", remoteNumDocs);
        boolean includeLocalIndex = randomBoolean();
        List<String> indices = new ArrayList<>();
        if (includeLocalIndex) {
            indices.add(randomFrom("*", "local_*", "local_test"));
        }
        indices.add(randomFrom("*:*", "remote_cluster:*", "remote_cluster:remote_test"));
        BytesReference pitId = openPointInTime(indices.toArray(new String[0]), TimeValue.timeValueMinutes(2));
        try {
            if (randomBoolean()) {
                localClient.prepareIndex("local_test").setId("local_new").setSource().get();
                localClient.admin().indices().prepareRefresh().get();
            }
            if (randomBoolean()) {
                remoteClient.prepareIndex("remote_test").setId("remote_new").setSource().get();
                remoteClient.admin().indices().prepareRefresh().get();
            }
            assertNoFailuresAndResponse(
                localClient.prepareSearch()
                    .setPreference(null)
                    .setQuery(new MatchAllQueryBuilder())
                    .setPointInTime(new PointInTimeBuilder(pitId))
                    .setSize(1000),
                resp -> {
                    assertHitCount(resp, (includeLocalIndex ? localNumDocs : 0) + remoteNumDocs);

                    SearchResponse.Clusters clusters = resp.getClusters();
                    int expectedNumClusters = 1 + (includeLocalIndex ? 1 : 0);
                    MatcherAssert.assertThat(clusters.getTotal(), equalTo(expectedNumClusters));
                    MatcherAssert.assertThat(
                        clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
                        equalTo(expectedNumClusters)
                    );
                    MatcherAssert.assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));

                    if (includeLocalIndex) {
                        SearchResponse.Cluster localCluster = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertNotNull(localCluster);
                        assertAllSuccessfulShards(localCluster, 3, 0);
                    }

                    SearchResponse.Cluster remoteCluster = clusters.getCluster(REMOTE_CLUSTER);
                    assertNotNull(remoteCluster);
                    assertAllSuccessfulShards(remoteCluster, 3, 0);
                }
            );
        } finally {
            closePointInTime(pitId);
        }
    }

    public void testOpenPITWithIndexFilter() {
        final Client localClient = client(LOCAL_CLUSTER);
        final Client remoteClient = client(REMOTE_CLUSTER);

        assertAcked(
            localClient.admin().indices().prepareCreate("local_test").setSettings(Settings.builder().put("index.number_of_shards", 3))
        );
        localClient.prepareIndex("local_test").setId("1").setSource("value", "1", "@timestamp", "2024-03-01").get();
        localClient.prepareIndex("local_test").setId("2").setSource("value", "2", "@timestamp", "2023-12-01").get();
        localClient.admin().indices().prepareRefresh("local_test").get();

        assertAcked(
            remoteClient.admin().indices().prepareCreate("remote_test").setSettings(Settings.builder().put("index.number_of_shards", 3))
        );
        remoteClient.prepareIndex("remote_test").setId("1").setSource("value", "1", "@timestamp", "2024-01-01").get();
        remoteClient.prepareIndex("remote_test").setId("2").setSource("value", "2", "@timestamp", "2023-12-01").get();
        remoteClient.admin().indices().prepareRefresh("remote_test").get();

        List<String> indices = new ArrayList<>();
        indices.add(randomFrom("*", "local_*", "local_test"));
        indices.add(randomFrom("*:*", "remote_cluster:*", "remote_cluster:remote_test"));

        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices.toArray(new String[0]));
        request.keepAlive(TimeValue.timeValueMinutes(2));
        request.indexFilter(new RangeQueryBuilder("@timestamp").gte("2023-12-15"));
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        BytesReference pitId = response.getPointInTimeId();

        if (randomBoolean()) {
            localClient.prepareIndex("local_test").setId("local_new").setSource().get();
            localClient.admin().indices().prepareRefresh().get();
        }
        if (randomBoolean()) {
            remoteClient.prepareIndex("remote_test").setId("remote_new").setSource().get();
            remoteClient.admin().indices().prepareRefresh().get();
        }

        try {
            assertNoFailuresAndResponse(
                localClient.prepareSearch()
                    .setPreference(null)
                    .setQuery(new MatchAllQueryBuilder())
                    .setPointInTime(new PointInTimeBuilder(pitId)),
                resp -> {
                    assertHitCount(resp, 2);

                    SearchResponse.Clusters clusters = resp.getClusters();
                    int expectedNumClusters = 2;
                    MatcherAssert.assertThat(clusters.getTotal(), equalTo(expectedNumClusters));
                    MatcherAssert.assertThat(
                        clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
                        equalTo(expectedNumClusters)
                    );
                    MatcherAssert.assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));

                    // both indices (local and remote) have shards, but there is a single shard left after can match
                    SearchResponse.Cluster localCluster = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertNotNull(localCluster);
                    assertAllSuccessfulShards(localCluster, 1, 0);
                    SearchResponse.Cluster remoteCluster = clusters.getCluster(REMOTE_CLUSTER);
                    assertNotNull(remoteCluster);
                    assertAllSuccessfulShards(remoteCluster, 1, 0);
                }
            );

            assertNoFailuresAndResponse(
                localClient.prepareSearch()
                    .setPreference(null)
                    // test the scenario where search also runs can match and filters additional shards out
                    .setPreFilterShardSize(1)
                    .setQuery(new RangeQueryBuilder("@timestamp").gte("2024-02-01"))
                    .setPointInTime(new PointInTimeBuilder(pitId)),
                resp -> {
                    assertHitCount(resp, 1);

                    SearchResponse.Clusters clusters = resp.getClusters();
                    int expectedNumClusters = 2;
                    MatcherAssert.assertThat(clusters.getTotal(), equalTo(expectedNumClusters));
                    MatcherAssert.assertThat(
                        clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
                        equalTo(expectedNumClusters)
                    );
                    MatcherAssert.assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));

                    // both indices (local and remote) have shards, but there is a single shard left after can match
                    SearchResponse.Cluster localCluster = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertNotNull(localCluster);
                    assertAllSuccessfulShards(localCluster, 1, 0);
                    SearchResponse.Cluster remoteCluster = clusters.getCluster(REMOTE_CLUSTER);
                    assertNotNull(remoteCluster);
                    assertAllSuccessfulShards(remoteCluster, 1, 1);
                }
            );
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
        BytesReference pitId = openPointInTime(indices.toArray(new String[0]), TimeValue.timeValueMinutes(2));
        try {
            if (randomBoolean()) {
                localClient.prepareIndex("local_test").setId("local_new").setSource().get();
                localClient.admin().indices().prepareRefresh().get();
            }
            if (randomBoolean()) {
                remoteClient.prepareIndex("remote_test").setId("remote_new").setSource().get();
                remoteClient.admin().indices().prepareRefresh().get();
            }
            // shardId 0 means to throw the Exception only on shard 0; all others should work
            ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10).pointInTimeBuilder(new PointInTimeBuilder(pitId)));
            assertResponse(client(LOCAL_CLUSTER).search(searchRequest), searchResponse -> {
                SearchResponse.Clusters clusters = searchResponse.getClusters();
                int expectedNumClusters = 1 + (includeLocalIndex ? 1 : 0);
                assertThat(clusters.getTotal(), equalTo(expectedNumClusters));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED), equalTo(0));
                assertThat(clusters.getClusterStateCount(SearchResponse.Cluster.Status.PARTIAL), equalTo(expectedNumClusters));
                if (includeLocalIndex) {
                    SearchResponse.Cluster localCluster = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertNotNull(localCluster);
                    assertOneFailedShard(localCluster, numShards);
                }
                SearchResponse.Cluster remoteCluster = clusters.getCluster(REMOTE_CLUSTER);
                assertNotNull(remoteCluster);
                assertOneFailedShard(remoteCluster, numShards);
            });
        } finally {
            closePointInTime(pitId);
        }
    }

    private static void assertOneFailedShard(SearchResponse.Cluster cluster, int totalShards) {
        assertThat(cluster.getSuccessfulShards(), equalTo(totalShards - 1));
        assertThat(cluster.getFailedShards(), equalTo(1));
        assertThat(cluster.getFailures().size(), equalTo(1));
        assertThat(cluster.getFailures().get(0).reason(), containsString("index corrupted"));
        assertThat(cluster.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
        assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertFalse(cluster.isTimedOut());
    }

    private static void assertAllSuccessfulShards(SearchResponse.Cluster cluster, int numShards, int skippedShards) {
        assertThat(cluster.getTotalShards(), equalTo(numShards));
        assertThat(cluster.getSkippedShards(), equalTo(skippedShards));
        assertThat(cluster.getSuccessfulShards(), equalTo(numShards));
        assertThat(cluster.getFailedShards(), equalTo(0));
        assertThat(cluster.getFailures().size(), equalTo(0));
        assertThat(cluster.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertFalse(cluster.isTimedOut());
    }

    private BytesReference openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    private void closePointInTime(BytesReference readerId) {
        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(readerId)).actionGet();
    }
}
