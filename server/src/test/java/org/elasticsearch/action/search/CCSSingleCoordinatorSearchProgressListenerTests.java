/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregationsTests;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.action.search.AbstractSearchAsyncAction.INTERNAL_PARTIAL_RESULTS_CANCEL_REASON;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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
            () -> listener.onListShards(shards, skippedByClusterAlias, clusters, randomBoolean(), timeProvider)
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

        listener.onListShards(shards, skippedByClusterAlias, clusters, randomBoolean(), timeProvider);

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

        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        SearchResponse.Cluster updatedA = clusters.getCluster(clusterA);
        SearchResponse.Cluster updatedB = clusters.getCluster(clusterB);

        // Status stays RUNNING (not all shards are skipped); shard counts must be set.
        assertThat(updatedA.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        assertThat(updatedA.getTotalShards(), equalTo(2));
        assertThat(updatedB.getStatus(), equalTo(SearchResponse.Cluster.Status.RUNNING));
        assertThat(updatedB.getTotalShards(), equalTo(1));
    }

    public void testOnQueryResult_DoesNothingWhenStatusIsNotRunning() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, TimeValue.timeValueMillis(1)::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register phase failure before any of the shards report success and confirm that the state is no longer RUNNING
        listener.onPhaseFailure(new IllegalArgumentException("unused"));
        var initialClusterState = clusters.getCluster(clusterAlias);
        assertThat(initialClusterState.getStatus(), not(SearchResponse.Cluster.Status.RUNNING));

        // Confirm that the cluster state is not modified
        onQueryResultForShardIndex(listener, shards, 0);
        assertThat(clusters.getCluster(clusterAlias), sameInstance(initialClusterState));
    }

    public void testOnQueryResult_UpdatesClustersMetadata() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 0, 0, false);

        // Confirm we update the number of successful shards and the timed out flag after getting a result that timed out
        int shardIndex = 0;
        QuerySearchResult timedOutQueryResult = queryResultForShard(shards, shardIndex);
        timedOutQueryResult.searchTimedOut(true);
        listener.onQueryResult(shardIndex, timedOutQueryResult);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 1, 0, true);

        // Confirm we update the number of successful shards after getting the last result
        onQueryResultForShardIndex(listener, shards, 1);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 2, 0, true);
    }

    public void testOnQueryResult_IsThreadSafe_WithOnPartialReduce() throws InterruptedException {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var numberOfShards = 20;
        var shards = getSearchShardListOneCluster(clusterAlias, numberOfShards);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), numberOfShards, 0, 0, false);

        var threads = new ArrayList<Thread>();
        var startOnQueryResult = new CountDownLatch(1);
        var startOnPartialReduce = new CountDownLatch(1);
        var partialReduceDone = new CountDownLatch(1);
        var successfulShards = new AtomicInteger();
        // Register success for each of the shards. Half of the shards will wait until onPartialReduce has been called to ensure that the
        // calls are happening simultaneously
        for (int i = 0; i < numberOfShards; ++i) {
            final int shardIndex = i;
            Thread onQueryResultThread = new Thread(() -> {
                try {
                    startOnQueryResult.await();
                    if (shardIndex % 2 == 0) {
                        partialReduceDone.await();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                onQueryResultForShardIndex(listener, shards, shardIndex);
                if (successfulShards.incrementAndGet() == 5) {
                    startOnPartialReduce.countDown();
                }
            });
            onQueryResultThread.start();
            threads.add(onQueryResultThread);
        }

        Thread partialReduceThread = new Thread(() -> {
            try {
                startOnPartialReduce.await();
                listener.onPartialReduce(
                    shards.subList(0, successfulShards.get()),
                    new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
                    InternalAggregationsTests.createTestInstance(),
                    randomInt()
                );
                partialReduceDone.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        partialReduceThread.start();
        threads.add(partialReduceThread);

        startOnQueryResult.countDown();
        for (Thread t : threads) {
            t.join(SAFE_AWAIT_TIMEOUT.millis());
        }

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), numberOfShards, numberOfShards, 0, false);
    }

    public void testOnQueryFailure_DoesNothingWhenStatusIsNotRunning() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, TimeValue.timeValueMillis(1)::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register phase failure before any of the shards report failure and confirm that the state is no longer RUNNING
        listener.onPhaseFailure(new IllegalArgumentException("unused"));
        var initialClusterState = clusters.getCluster(clusterAlias);
        assertThat(initialClusterState.getStatus(), not(SearchResponse.Cluster.Status.RUNNING));

        // Confirm that the cluster state is not modified
        onQueryFailureForShardIndex(listener, shards, 0, new IllegalArgumentException());
        assertThat(clusters.getCluster(clusterAlias), sameInstance(initialClusterState));
    }

    public void testOnQueryFailure_UpdatesClusterMetadata() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 3);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 3, 0, 0, false);

        // Register a failure for the first shard and confirm the state
        var exception0 = new IllegalArgumentException("test");
        var shardTarget0 = onQueryFailureForShardIndex(listener, shards, 0, exception0);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 3, 0, 1, false);

        var failure = clusters.getCluster(clusterAlias).getFailures().getLast();
        assertThat(failure.shard(), is(shardTarget0));
        assertThat(failure.getCause(), is(exception0));

        // Register a failure for the second shard and confirm the state
        var exception1 = new NullPointerException("test NPE");
        var shardTarget1 = onQueryFailureForShardIndex(listener, shards, 1, exception1);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 3, 0, 2, false);

        // Confirm the new failure is appended to the list of failures
        failure = clusters.getCluster(clusterAlias).getFailures().getLast();
        assertThat(failure.shard(), is(shardTarget1));
        assertThat(failure.getCause(), is(exception1));
    }

    public void testOnQueryFailure_UpdatesClusterMetadata_AllShardsFailed_SkipUnavailableTrue() {
        testOnQueryFailure_UpdatesClusterMetadata_AllShardsFailed(true, SearchResponse.Cluster.Status.SKIPPED);
    }

    public void testOnQueryFailure_UpdatesClusterMetadata_AllShardsFailed_SkipUnavailableFalse() {
        testOnQueryFailure_UpdatesClusterMetadata_AllShardsFailed(false, SearchResponse.Cluster.Status.FAILED);
    }

    private static void testOnQueryFailure_UpdatesClusterMetadata_AllShardsFailed(
        boolean skipUnavailable,
        SearchResponse.Cluster.Status expectedFinalStatus
    ) {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", skipUnavailable, null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = List.of(new SearchShard(clusterAlias, new ShardId("my-index", "uuid-a", 0)));

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 1, 0, 0, false);

        var exception = new IllegalArgumentException("test");
        var shardTarget = onQueryFailureForShardIndex(listener, shards, 0, exception);

        // Confirm the state
        assertClusterMetadata(clusters.getCluster(clusterAlias), expectedFinalStatus, 1, 0, 1, null, false);

        var failure = clusters.getCluster(clusterAlias).getFailures().getLast();
        assertThat(failure.shard(), is(shardTarget));
        assertThat(failure.getCause(), is(exception));
    }

    public void testOnQueryFailure_UpdatesClusterMetadata_OnlyFinalShardFailed() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register a successful result for the first shard
        onQueryResultForShardIndex(listener, shards, 0);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 1, 0, false);

        // Register a failure for the final shard and confirm the state
        var exception = new IllegalArgumentException("test");
        var shardTarget = onQueryFailureForShardIndex(listener, shards, 1, exception);

        assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.PARTIAL, 2, 1, 1, tookMillis, false);

        var failure = clusters.getCluster(clusterAlias).getFailures().getLast();
        assertThat(failure.shard(), is(shardTarget));
        assertThat(failure.getCause(), is(exception));
    }

    public void testOnQueryFailure_UpdatesClusterMetadata_DoesNotIncludeShardFailure_WhenFailureIsDueToInternalCancel() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", false, null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 4);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 4, 0, 0, false);

        // Register a failure due to an internal TaskCancelledException and confirm the state
        onQueryFailureForShardIndex(listener, shards, 0, new TaskCancelledException(INTERNAL_PARTIAL_RESULTS_CANCEL_REASON));

        // Confirm the failure is included in failed shards count but not appended to the list of shard failures
        assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.RUNNING, 4, 0, 1, 0, null, false);

        // Register a failure due to a nested internal TaskCancelledException and confirm the state
        onQueryFailureForShardIndex(
            listener,
            shards,
            1,
            new RemoteTransportException("test RTE", new TaskCancelledException(INTERNAL_PARTIAL_RESULTS_CANCEL_REASON))
        );

        // Confirm the failure is included in failed shards count but not appended to the list of shard failures
        assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.RUNNING, 4, 0, 2, 0, null, false);

        // Register a failure due to a non-internal TaskCancelledException and confirm the state
        var exception = new TaskCancelledException("not internal cancel");
        var shardTarget = onQueryFailureForShardIndex(listener, shards, 2, exception);

        SearchResponse.Cluster cluster = clusters.getCluster(clusterAlias);
        assertClusterMetadata(cluster, SearchResponse.Cluster.Status.RUNNING, 4, 0, 3, 1, null, false);
        // Confirm the failure is appended to the list of failures
        var failure = clusters.getCluster(clusterAlias).getFailures().getLast();
        assertThat(failure.shard(), is(shardTarget));
        assertThat(failure.getCause(), is(exception));

        // Register a failure due to a TaskCancelledException with a null message and confirm the state
        String nullString = null;
        var nullMessageException = new TaskCancelledException(nullString);
        shardTarget = onQueryFailureForShardIndex(listener, shards, 3, nullMessageException);

        cluster = clusters.getCluster(clusterAlias);
        assertClusterMetadata(cluster, SearchResponse.Cluster.Status.FAILED, 4, 0, 4, 2, null, false);
        // Confirm the failure is appended to the list of failures
        failure = clusters.getCluster(clusterAlias).getFailures().getLast();
        assertThat(failure.shard(), is(shardTarget));
        assertThat(failure.getCause(), is(nullMessageException));
    }

    public void testOnPartialReduce_DoesNothingWhenStatusIsNotRunning() throws Exception {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var successfulShard = new SearchShard(clusterAlias, new ShardId("my-index", "uuid-a", 0));
        var failedShard = new SearchShard(clusterAlias, new ShardId("my-index", "uuid-a", 1));
        var shards = List.of(successfulShard, failedShard);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 0, 0, false);

        onQueryResultForShardIndex(listener, shards, 0);

        // Fail the final shard, which will set the state to something other than RUNNING
        var exception = new IllegalArgumentException("test");
        onQueryFailureForShardIndex(listener, shards, 1, exception);
        SearchResponse.Cluster initialClusterState = clusters.getCluster(clusterAlias);

        assertThat(initialClusterState.getStatus(), not(equalTo(SearchResponse.Cluster.Status.RUNNING)));

        // Only pass successful shards to onPartialReduce()
        listener.onPartialReduce(
            List.of(successfulShard),
            new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
            InternalAggregationsTests.createTestInstance(),
            randomInt()
        );

        // Confirm that the cluster state is not modified
        assertThat(clusters.getCluster(clusterAlias), sameInstance(initialClusterState));
    }

    public void testOnPartialReduce_DoesNothingWhenNotFinalShard() throws Exception {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 3);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register success for the first two shards
        onQueryResultForShardIndex(listener, shards, 0);
        onQueryResultForShardIndex(listener, shards, 1);

        SearchResponse.Cluster initialClusterState = clusters.getCluster(clusterAlias);

        List<SearchShard> partialShards = shards.subList(0, 2);
        listener.onPartialReduce(
            partialShards,
            new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
            InternalAggregationsTests.createTestInstance(),
            randomInt()
        );

        // Confirm that the cluster state is not modified
        assertThat(clusters.getCluster(clusterAlias), sameInstance(initialClusterState));
    }

    public void testOnPartialReduce_UpdatesStatusWhenFinalShard_AllSuccessful() throws Exception {
        testOnPartialReduce_UpdatesStatusWhenFinalShard_AllSuccessful(false);
    }

    public void testOnPartialReduce_UpdatesStatusWhenFinalShard_AllSuccessful_TimedOut() throws Exception {
        testOnPartialReduce_UpdatesStatusWhenFinalShard_AllSuccessful(true);
    }

    private static void testOnPartialReduce_UpdatesStatusWhenFinalShard_AllSuccessful(boolean timedOut) throws Exception {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register success for the shards, optionally setting one as timed out, and check the state
        onQueryResultForShardIndex(listener, shards, 0);

        var maybeTimedOutResult = queryResultForShard(shards, 1);
        maybeTimedOutResult.searchTimedOut(timedOut);
        listener.onQueryResult(1, maybeTimedOutResult);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 2, 0, timedOut);

        listener.onPartialReduce(
            shards,
            new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
            InternalAggregationsTests.createTestInstance(),
            randomInt()
        );

        // Confirm that the cluster state is as expected
        var expectedStatus = timedOut ? SearchResponse.Cluster.Status.PARTIAL : SearchResponse.Cluster.Status.SUCCESSFUL;
        assertClusterMetadata(clusters.getCluster(clusterAlias), expectedStatus, 2, 2, 0, tookMillis, timedOut);
    }

    public void testOnPartialReduce_UpdatesStatusWhenFinalShard_PartialFailure() throws Exception {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 3);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register a failure for the first shard
        var exception = new IllegalArgumentException("test");
        onQueryFailureForShardIndex(listener, shards, 0, exception);

        // Register success for the second and third shards
        onQueryResultForShardIndex(listener, shards, 1);
        onQueryResultForShardIndex(listener, shards, 2);

        // Pass only successful shards to onPartialReduce()
        List<SearchShard> partialShards = shards.subList(1, 3);
        listener.onPartialReduce(
            partialShards,
            new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
            InternalAggregationsTests.createTestInstance(),
            randomInt()
        );

        // Confirm that the cluster state is modified as expected
        assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.PARTIAL, 3, 2, 1, tookMillis, false);
    }

    public void testOnFinalReduce_DoesNothingWhenStatusIsNotRunning() throws Exception {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var successfulShard = new SearchShard(clusterAlias, new ShardId("my-index", "uuid-a", 0));
        var failedShard = new SearchShard(clusterAlias, new ShardId("my-index", "uuid-a", 1));
        var shards = List.of(successfulShard, failedShard);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 0, 0, false);

        onQueryResultForShardIndex(listener, shards, 0);

        // Fail the final shard, which will set the state to something other than RUNNING
        var exception = new IllegalArgumentException("test");
        onQueryFailureForShardIndex(listener, shards, 1, exception);
        SearchResponse.Cluster initialClusterState = clusters.getCluster(clusterAlias);

        assertThat(initialClusterState.getStatus(), not(equalTo(SearchResponse.Cluster.Status.RUNNING)));

        // Only pass successful shards to onFinalReduce()
        listener.onFinalReduce(
            List.of(successfulShard),
            new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
            InternalAggregationsTests.createTestInstance(),
            randomInt()
        );

        // Confirm that the cluster state is not modified
        assertThat(clusters.getCluster(clusterAlias), sameInstance(initialClusterState));
    }

    public void testOnFinalReduce_UpdatesStatus_AllSuccessful() throws Exception {
        testOnFinalReduce_UpdatesStatus_AllSuccessful(false);
    }

    public void testOnFinalReduce_UpdatesStatus_AllSuccessful_TimedOut() throws Exception {
        testOnFinalReduce_UpdatesStatus_AllSuccessful(true);
    }

    private static void testOnFinalReduce_UpdatesStatus_AllSuccessful(boolean timedOut) throws Exception {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register success for the shards, optionally setting one as timed out, and check the state
        onQueryResultForShardIndex(listener, shards, 0);

        var maybeTimedOutResult = queryResultForShard(shards, 1);
        maybeTimedOutResult.searchTimedOut(timedOut);
        listener.onQueryResult(1, maybeTimedOutResult);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 2, 0, timedOut);

        listener.onFinalReduce(
            shards,
            new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
            InternalAggregationsTests.createTestInstance(),
            randomInt()
        );

        // Confirm that the cluster state is as expected
        var expectedStatus = timedOut ? SearchResponse.Cluster.Status.PARTIAL : SearchResponse.Cluster.Status.SUCCESSFUL;
        assertClusterMetadata(clusters.getCluster(clusterAlias), expectedStatus, 2, 2, 0, tookMillis, timedOut);
    }

    public void testOnFinalReduce_UpdatesStatus_PartialFailure() throws Exception {
        var clusterAlias = "project-a";
        var skipUnavailable = randomBoolean();
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", skipUnavailable, null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 3);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register a failure for the first shard
        var exception = new IllegalArgumentException("test");
        onQueryFailureForShardIndex(listener, shards, 0, exception);

        // Register success for the second and third shards
        onQueryResultForShardIndex(listener, shards, 1);
        onQueryResultForShardIndex(listener, shards, 2);

        // Pass only successful shards to onFinalReduce()
        List<SearchShard> finalShards = shards.subList(1, 3);
        listener.onFinalReduce(
            finalShards,
            new TotalHits(randomNonNegativeLong(), randomFrom(TotalHits.Relation.values())),
            InternalAggregationsTests.createTestInstance(),
            randomInt()
        );

        // Confirm that the cluster state is modified as expected
        assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.PARTIAL, 3, 2, 1, tookMillis, false);
    }

    public void testOnPhaseFailure_DoesNothingWhenStatusIsNotRunning() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Confirm the initial state
        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 0, 0, false);

        onQueryResultForShardIndex(listener, shards, 0);

        // Fail the final shard, which will set the state to something other than RUNNING
        var exception = new IllegalArgumentException("test");
        onQueryFailureForShardIndex(listener, shards, 1, exception);
        SearchResponse.Cluster initialClusterState = clusters.getCluster(clusterAlias);

        assertThat(initialClusterState.getStatus(), not(equalTo(SearchResponse.Cluster.Status.RUNNING)));

        listener.onPhaseFailure(new IllegalArgumentException("unused"));

        // Confirm that the cluster state is not modified
        assertThat(clusters.getCluster(clusterAlias), sameInstance(initialClusterState));
    }

    public void testOnPhaseFailure_UpdatesStatus_AllSuccessful() {
        testOnPhaseFailure_UpdatesStatus_AllSuccessful(false);
    }

    public void testOnPhaseFailure_UpdatesStatus_AllSuccessful_TimedOut() {
        testOnPhaseFailure_UpdatesStatus_AllSuccessful(true);
    }

    private static void testOnPhaseFailure_UpdatesStatus_AllSuccessful(boolean timedOut) {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register success for the shards, optionally setting one as timed out, and check the state
        onQueryResultForShardIndex(listener, shards, 0);

        var maybeTimedOutResult = queryResultForShard(shards, 1);
        maybeTimedOutResult.searchTimedOut(timedOut);
        listener.onQueryResult(1, maybeTimedOutResult);

        assertClusterMetadataRunning(clusters.getCluster(clusterAlias), 2, 2, 0, timedOut);

        listener.onPhaseFailure(new IllegalArgumentException("unused"));

        // Confirm that the cluster state is as expected
        var expectedStatus = timedOut ? SearchResponse.Cluster.Status.PARTIAL : SearchResponse.Cluster.Status.SUCCESSFUL;
        assertClusterMetadata(clusters.getCluster(clusterAlias), expectedStatus, 2, 2, 0, tookMillis, timedOut);
    }

    public void testOnPhaseFailure_UpdatesStatus_PartialFailure() {
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", randomBoolean(), null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 3);

        var tookMillis = TimeValue.timeValueMillis(1);
        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, tookMillis::nanos);
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register success for the first and second shards. The third shard should be treated as failed
        onQueryResultForShardIndex(listener, shards, 0);
        onQueryResultForShardIndex(listener, shards, 1);

        listener.onPhaseFailure(new IllegalArgumentException("unused"));

        // Confirm that the cluster state is modified as expected
        assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.PARTIAL, 3, 2, 1, 0, tookMillis, false);
    }

    public void testOnPhaseFailure_UpdatesStatus_NoSuccessfulShards() {
        boolean skipUnavailable = randomBoolean();
        var clusterAlias = "project-a";
        var clusterMap = Map.of(clusterAlias, new SearchResponse.Cluster(clusterAlias, "my-alias", skipUnavailable, null));
        var clusters = new SearchResponse.Clusters(clusterMap, false);
        var shards = getSearchShardListOneCluster(clusterAlias, 2);

        var timeProvider = new TransportSearchAction.SearchTimeProvider(0L, 0L, () -> TimeValue.timeValueMillis(1).nanos());
        var listener = new CCSSingleCoordinatorSearchProgressListener();
        listener.onListShards(shards, Map.of(), clusters, randomBoolean(), timeProvider);

        // Register failure for the first shard only, the second shard should be treated as failed
        onQueryFailureForShardIndex(listener, shards, 0, new IllegalArgumentException("test"));

        listener.onPhaseFailure(new IllegalArgumentException("unused"));

        // Confirm that the cluster state is updated
        if (skipUnavailable) {
            assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.SKIPPED, 2, 0, 2, 1, null, false);
        } else {
            assertClusterMetadata(clusters.getCluster(clusterAlias), SearchResponse.Cluster.Status.FAILED, 2, 0, 2, 1, null, false);
        }
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
        onQueryResultForShardIndex(listener, shards, 0);

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
        onQueryResultForShardIndex(listener, shards, 0);
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
        onQueryResultForShardIndex(listener, shards, 0);

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
        onQueryResultForShardIndex(listener, shards, 0);
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
        onQueryResultForShardIndex(listener, shards, 0);
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
        onQueryResultForShardIndex(listener, shards, 0);
        onQueryResultForShardIndex(listener, shards, 1);

        assertThat(clusters.getCluster(localCluster).getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(clusters.getCluster(localCluster).getTook().millis(), equalTo(0L));
        assertThat(clusters.getCluster(remoteCluster).getTook().millis(), equalTo(0L));

        nowNanos.set(TimeValue.timeValueMillis(12).nanos());
        // shard index 0 belongs to remoteCluster only
        listener.onFetchResult(0);

        assertThat(clusters.getCluster(localCluster).getTook().millis(), equalTo(0L));
        assertThat(clusters.getCluster(remoteCluster).getTook().millis(), equalTo(12L));
    }

    private static List<SearchShard> getSearchShardListOneCluster(String clusterAlias, int numShards) {
        return IntStream.range(0, numShards).mapToObj(i -> new SearchShard(clusterAlias, new ShardId("my-index", "uuid-a", i))).toList();
    }

    private static void onQueryResultForShardIndex(
        CCSSingleCoordinatorSearchProgressListener listener,
        List<SearchShard> shards,
        int shardIndex
    ) {
        listener.onQueryResult(shardIndex, queryResultForShard(shards, shardIndex));
    }

    private static QuerySearchResult queryResultForShard(List<SearchShard> searchShards, int shardIndex) {
        SearchShard shard = searchShards.get(shardIndex);
        ShardId shardId = shard.shardId();
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.setSearchShardTarget(
            new SearchShardTarget(
                "node-0",
                new ShardId(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id()),
                shard.clusterAlias()
            )
        );
        return querySearchResult;
    }

    private static SearchShardTarget onQueryFailureForShardIndex(
        CCSSingleCoordinatorSearchProgressListener listener,
        List<SearchShard> shards,
        int shardIndex,
        Exception exception
    ) {
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomUUID(),
            shards.get(shardIndex).shardId(),
            shards.get(shardIndex).clusterAlias()
        );
        listener.onQueryFailure(shardIndex, shardTarget, exception);
        return shardTarget;
    }

    private static void assertClusterMetadataRunning(
        SearchResponse.Cluster cluster,
        int totalShards,
        int successfulShards,
        int failedShards,
        boolean timedOut
    ) {
        assertClusterMetadata(cluster, SearchResponse.Cluster.Status.RUNNING, totalShards, successfulShards, failedShards, null, timedOut);
    }

    private static void assertClusterMetadata(
        SearchResponse.Cluster cluster,
        SearchResponse.Cluster.Status expectedStatus,
        int totalShards,
        int successfulShards,
        int failedShards,
        TimeValue took,
        boolean timedOut
    ) {
        assertClusterMetadata(cluster, expectedStatus, totalShards, successfulShards, failedShards, failedShards, took, timedOut);
    }

    private static void assertClusterMetadata(
        SearchResponse.Cluster cluster,
        SearchResponse.Cluster.Status expectedStatus,
        int totalShards,
        int successfulShards,
        int failedShards,
        int failuresSize,
        TimeValue took,
        boolean timedOut
    ) {
        assertThat(cluster.getStatus(), is(expectedStatus));
        assertThat(cluster.getTotalShards(), is(totalShards));
        assertThat(cluster.getSuccessfulShards(), is(successfulShards));
        assertThat(cluster.getFailedShards(), is(failedShards));
        assertThat(cluster.getFailures(), hasSize(failuresSize));
        if (took == null) {
            assertThat(cluster.getTook(), is(nullValue()));
        } else {
            assertThat(cluster.getTook(), is(took));
        }
        assertThat(cluster.isTimedOut(), is(timedOut));
    }
}
