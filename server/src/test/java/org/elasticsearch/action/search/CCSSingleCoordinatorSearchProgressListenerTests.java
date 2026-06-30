/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class CCSSingleCoordinatorSearchProgressListenerTests extends ESTestCase {

    /** Verifies stale aliases trip the invariant assert. */
    public void testOnListShardsAssertsOnStaleClusterAlias() {
        String clusterA = "project-a";
        String clusterB = "project-b";
        // This alias was excluded by TransportSearchAction.reconcileProjects (alias not hosted on that cluster) but its
        // entry survived in numSkippedShards and is now arriving via skippedByClusterAlias.
        String staleCluster = "project-stale";

        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(clusterA, new SearchResponse.Cluster(clusterA, "my-alias", false, null));
        clusterMap.put(clusterB, new SearchResponse.Cluster(clusterB, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);

        // One active (non-skipped) shard per real cluster
        List<SearchShard> shards = List.of(
            new SearchShard(clusterA, new ShardId("my-index", "uuid-a", 0)),
            new SearchShard(clusterB, new ShardId("my-index", "uuid-b", 0))
        );

        // The stale cluster appears only in skippedByClusterAlias, not in the shards list or clusters map.
        // Before the fix, depending on HashMap iteration order, this caused NPE aborting the loop.
        Map<String, Integer> skippedByClusterAlias = new HashMap<>();
        skippedByClusterAlias.put(staleCluster, 2);

        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0L);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        AssertionError assertionError = expectThrows(
            AssertionError.class,
            () -> listener.onListShards(shards, skippedByClusterAlias, clusters, true, timeProvider)
        );
        assertThat(assertionError.getMessage(), equalTo("cluster alias [project-stale] not present in clusters map"));
    }

    /**
     * Verifies the normal {@link CCSSingleCoordinatorSearchProgressListener#onListShards} flow when
     * all shards on a cluster were skipped during the can-match phase: the cluster must transition
     * directly from {@code RUNNING} to {@code SUCCESSFUL} with {@code took} set, because there are
     * no shards left to query.
     */
    public void testOnListShardsAllShardsSkippedTransitionsToSuccessful() {
        String cluster = "project-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(cluster, new SearchResponse.Cluster(cluster, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);

        // No active shards; all 2 shards were skipped before the query phase.
        List<SearchShard> shards = List.of();
        Map<String, Integer> skippedByClusterAlias = Map.of(cluster, 2);

        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0L);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, skippedByClusterAlias, clusters, false, timeProvider);

        SearchResponse.Cluster updated = clusters.getCluster(cluster);
        assertThat(updated.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(updated.getTotalShards(), equalTo(2));
        assertThat(updated.getSkippedShards(), equalTo(2));
        assertThat("took must be set when cluster transitions to SUCCESSFUL in onListShards", updated.getTook(), notNullValue());
    }

    /**
     * Verifies that multiple clusters are all updated correctly in a typical minimize_roundtrips=false
     * scenario where each cluster has some active shards (no skipping).
     */
    public void testOnListShardsUpdatesAllClustersWithShards() {
        String clusterA = "project-a";
        String clusterB = "project-b";

        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(clusterA, new SearchResponse.Cluster(clusterA, "my-alias", false, null));
        clusterMap.put(clusterB, new SearchResponse.Cluster(clusterB, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);

        List<SearchShard> shards = List.of(
            new SearchShard(clusterA, new ShardId("my-index", "uuid-a", 0)),
            new SearchShard(clusterA, new ShardId("my-index", "uuid-a", 1)),
            new SearchShard(clusterB, new ShardId("my-index", "uuid-b", 0))
        );

        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0L);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, Map.of(), clusters, true, timeProvider);

        SearchResponse.Cluster updatedA = clusters.getCluster(clusterA);
        SearchResponse.Cluster updatedB = clusters.getCluster(clusterB);

        // Status stays RUNNING (not all shards are skipped); shard counts must be set.
        assertThat(updatedA.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        assertThat(updatedA.getTotalShards(), equalTo(2));
        assertThat(updatedB.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        assertThat(updatedB.getTotalShards(), equalTo(1));
    }

    public void testFetchResultRefreshesTookWhenFetchPhaseEnabled() {
        String cluster = "project-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(cluster, new SearchResponse.Cluster(cluster, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);
        List<SearchShard> shards = List.of(new SearchShard(cluster, new ShardId("my-index", "uuid-a", 0)));

        AtomicLong nowNanos = new AtomicLong(TimeValue.timeValueMillis(1).nanos());
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, nowNanos::get);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, Map.of(), clusters, true, timeProvider);
        listener.onFinalReduce(shards, null, null, 1);
        listener.onQueryResult(0, queryResultForShard(cluster, "my-index", "uuid-a", 0));

        SearchResponse.Cluster afterQuery = clusters.getCluster(cluster);
        assertThat(afterQuery.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(afterQuery.getTook().millis(), equalTo(1L));

        nowNanos.set(TimeValue.timeValueMillis(5).nanos());
        listener.onFetchResult(0);

        SearchResponse.Cluster afterFetch = clusters.getCluster(cluster);
        assertThat(afterFetch.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(afterFetch.getTook().millis(), equalTo(5L));
    }

    public void testFetchResultDoesNotRefreshTookWhenFetchPhaseDisabled() {
        String cluster = "project-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(cluster, new SearchResponse.Cluster(cluster, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);
        List<SearchShard> shards = List.of(new SearchShard(cluster, new ShardId("my-index", "uuid-a", 0)));

        AtomicLong nowNanos = new AtomicLong(TimeValue.timeValueMillis(1).nanos());
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, nowNanos::get);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, Map.of(), clusters, false, timeProvider);
        listener.onFinalReduce(shards, null, null, 1);
        listener.onQueryResult(0, queryResultForShard(cluster, "my-index", "uuid-a", 0));
        assertThat(clusters.getCluster(cluster).getTook().millis(), equalTo(1L));

        nowNanos.set(TimeValue.timeValueMillis(5).nanos());
        listener.onFetchResult(0);

        assertThat(clusters.getCluster(cluster).getTook().millis(), equalTo(1L));
    }

    public void testFetchResultDoesNotRefreshTookWhenClusterStillRunning() {
        String cluster = "project-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(cluster, new SearchResponse.Cluster(cluster, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);
        List<SearchShard> shards = List.of(new SearchShard(cluster, new ShardId("my-index", "uuid-a", 0)));

        AtomicLong nowNanos = new AtomicLong(TimeValue.timeValueMillis(1).nanos());
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, nowNanos::get);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, Map.of(), clusters, true, timeProvider);
        listener.onQueryResult(0, queryResultForShard(cluster, "my-index", "uuid-a", 0));

        SearchResponse.Cluster afterQuery = clusters.getCluster(cluster);
        assertThat(afterQuery.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        assertNull(afterQuery.getTook());

        nowNanos.set(TimeValue.timeValueMillis(9).nanos());
        listener.onFetchResult(0);

        SearchResponse.Cluster afterFetch = clusters.getCluster(cluster);
        assertThat(afterFetch.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        assertNull(afterFetch.getTook());
    }

    public void testFetchFailureRefreshesTookWhenFetchPhaseEnabled() {
        String cluster = "project-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(cluster, new SearchResponse.Cluster(cluster, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);
        List<SearchShard> shards = List.of(new SearchShard(cluster, new ShardId("my-index", "uuid-a", 0)));

        AtomicLong nowNanos = new AtomicLong(TimeValue.timeValueMillis(1).nanos());
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, nowNanos::get);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, Map.of(), clusters, true, timeProvider);
        listener.onFinalReduce(shards, null, null, 1);
        listener.onQueryResult(0, queryResultForShard(cluster, "my-index", "uuid-a", 0));
        assertThat(clusters.getCluster(cluster).getTook().millis(), equalTo(1L));

        nowNanos.set(TimeValue.timeValueMillis(7).nanos());
        listener.onFetchFailure(
            0,
            new SearchShardTarget("node-0", new ShardId("my-index", "uuid-a", 0), cluster),
            new RuntimeException("simulated fetch failure")
        );

        SearchResponse.Cluster afterFetchFailure = clusters.getCluster(cluster);
        assertThat(afterFetchFailure.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(afterFetchFailure.getTook().millis(), equalTo(7L));
    }

    public void testFetchFailureDoesNotRefreshTookWhenFetchPhaseDisabled() {
        String cluster = "project-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(cluster, new SearchResponse.Cluster(cluster, "my-alias", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);
        List<SearchShard> shards = List.of(new SearchShard(cluster, new ShardId("my-index", "uuid-a", 0)));

        AtomicLong nowNanos = new AtomicLong(TimeValue.timeValueMillis(1).nanos());
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, nowNanos::get);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, Map.of(), clusters, false, timeProvider);
        listener.onFinalReduce(shards, null, null, 1);
        listener.onQueryResult(0, queryResultForShard(cluster, "my-index", "uuid-a", 0));
        assertThat(clusters.getCluster(cluster).getTook().millis(), equalTo(1L));

        nowNanos.set(TimeValue.timeValueMillis(9).nanos());
        listener.onFetchFailure(
            0,
            new SearchShardTarget("node-0", new ShardId("my-index", "uuid-a", 0), cluster),
            new RuntimeException("simulated fetch failure")
        );

        assertThat(clusters.getCluster(cluster).getTook().millis(), equalTo(1L));
    }

    public void testFetchResultOnlyRefreshesClusterForShardIndex() {
        String localCluster = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        String remoteCluster = "remote-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(localCluster, new SearchResponse.Cluster(localCluster, "local-index", false, "_origin"));
        clusterMap.put(remoteCluster, new SearchResponse.Cluster(remoteCluster, "remote-index", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);
        // Keep index 0 bound to remote cluster; local cluster still has matching shards.
        List<SearchShard> shards = List.of(
            new SearchShard(remoteCluster, new ShardId("remote-index", "uuid-r", 0)),
            new SearchShard(localCluster, new ShardId("local-index", "uuid-l", 0))
        );

        AtomicLong nowNanos = new AtomicLong(TimeValue.timeValueMillis(0).nanos());
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, nowNanos::get);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        listener.onListShards(shards, Map.of(), clusters, true, timeProvider);
        listener.onFinalReduce(shards, null, null, 1);
        listener.onQueryResult(0, queryResultForShard(remoteCluster, "remote-index", "uuid-r", 0));
        listener.onQueryResult(1, queryResultForShard(localCluster, "local-index", "uuid-l", 0));

        assertThat(clusters.getCluster(localCluster).getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(clusters.getCluster(localCluster).getTook().millis(), equalTo(0L));
        assertThat(clusters.getCluster(remoteCluster).getTook().millis(), equalTo(0L));

        nowNanos.set(TimeValue.timeValueMillis(12).nanos());
        // shard index 0 belongs to remoteCluster only
        listener.onFetchResult(0);

        assertThat(clusters.getCluster(localCluster).getTook().millis(), equalTo(0L));
        assertThat(clusters.getCluster(remoteCluster).getTook().millis(), equalTo(12L));
    }

    private static QuerySearchResult queryResultForShard(String clusterAlias, String index, String uuid, int shardId) {
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.setSearchShardTarget(new SearchShardTarget("node-0", new ShardId(index, uuid, shardId), clusterAlias));
        return querySearchResult;
    }
}
