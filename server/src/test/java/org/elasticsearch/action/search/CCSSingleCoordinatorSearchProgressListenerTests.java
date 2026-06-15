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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for {@link CCSSingleCoordinatorSearchProgressListener}, in particular for the
 * {@code minimize_roundtrips=false} path where {@code _cluster/details} is updated progressively.
 */
public class CCSSingleCoordinatorSearchProgressListenerTests extends ESTestCase {

    /**
     * Regression test for the bug where a cluster alias present in {@code skippedByClusterAlias}
     * but absent from the {@link SearchResponse.Clusters} map (a "stale" alias left behind after
     * {@link TransportSearchAction#reconcileProjects} excluded that cluster) would cause a
     * {@code NullPointerException} inside the {@code swapCluster} lambda, silently aborting the
     * entire update loop and leaving all remaining valid clusters permanently in {@code RUNNING}
     * state with no shard counts set.
     *
     * <p>The fix adds a {@code null} guard that skips the {@code swapCluster} call for any alias
     * that is not present in the {@link SearchResponse.Clusters} map, so the loop always runs to
     * completion and every participating cluster is correctly updated.
     */
    public void testOnListShardsStaleClusterAliasDoesNotAbortValidClusterUpdates() {
        String clusterA = "project-a";
        String clusterB = "project-b";
        // This alias was excluded by reconcileProjects (alias not hosted on that cluster) but its
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

        // Must not throw; before the fix this would intermittently NPE inside swapCluster when the
        // stale alias was processed first, silently aborting the loop via notifyListShards's catch block.
        listener.onListShards(shards, skippedByClusterAlias, clusters, true, timeProvider);

        SearchResponse.Cluster updatedA = clusters.getCluster(clusterA);
        SearchResponse.Cluster updatedB = clusters.getCluster(clusterB);

        // Both real clusters must have had their shard counts populated; null here means the loop
        // was aborted before reaching that cluster.
        assertThat("project-a totalShards must be set after onListShards", updatedA.getTotalShards(), notNullValue());
        assertThat("project-b totalShards must be set after onListShards", updatedB.getTotalShards(), notNullValue());
        assertThat(updatedA.getTotalShards(), equalTo(1));
        assertThat(updatedB.getTotalShards(), equalTo(1));

        // The stale cluster must never have been inserted into the Clusters map.
        assertNull("stale cluster must not appear in the Clusters map", clusters.getCluster(staleCluster));
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
        assertThat(clusters.getCluster(cluster).getTook().millis(), equalTo(1L));

        nowNanos.set(TimeValue.timeValueMillis(5).nanos());
        listener.onFetchResult(0);

        assertThat(clusters.getCluster(cluster).getTook().millis(), equalTo(1L));
    }

    public void testFetchResultOnlyRefreshesClusterForShardIndex() {
        String localCluster = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        String remoteCluster = "remote-a";
        Map<String, SearchResponse.Cluster> clusterMap = new HashMap<>();
        clusterMap.put(localCluster, new SearchResponse.Cluster(localCluster, "nomatch*", false, "_origin"));
        clusterMap.put(remoteCluster, new SearchResponse.Cluster(remoteCluster, "remote-index", false, null));
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(clusterMap, false);
        List<SearchShard> shards = List.of(new SearchShard(remoteCluster, new ShardId("remote-index", "uuid-r", 0)));

        AtomicLong nowNanos = new AtomicLong(TimeValue.timeValueMillis(0).nanos());
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, nowNanos::get);
        CCSSingleCoordinatorSearchProgressListener listener = new CCSSingleCoordinatorSearchProgressListener();

        // local cluster has no matching indices/shards and is finalized immediately with took=0
        listener.onListShards(shards, Map.of(localCluster, 0), clusters, true, timeProvider);
        listener.onFinalReduce(shards, null, null, 1);

        assertThat(clusters.getCluster(localCluster).getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(clusters.getCluster(localCluster).getTook().millis(), equalTo(0L));
        assertThat(clusters.getCluster(remoteCluster).getTook().millis(), equalTo(0L));

        nowNanos.set(TimeValue.timeValueMillis(12).nanos());
        // shard index 0 belongs to remoteCluster only
        listener.onFetchResult(0);

        assertThat(clusters.getCluster(localCluster).getTook().millis(), equalTo(0L));
        assertThat(clusters.getCluster(remoteCluster).getTook().millis(), equalTo(12L));
    }
}
