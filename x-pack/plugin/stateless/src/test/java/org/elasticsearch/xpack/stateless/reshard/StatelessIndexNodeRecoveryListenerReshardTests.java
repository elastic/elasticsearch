/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.StatelessIndexNodeRecoveryListener;
import org.elasticsearch.xpack.stateless.recovery.metering.StatelessPrimaryRelocationMetricsCollector;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotsCommitService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatelessIndexNodeRecoveryListenerReshardTests extends ESTestCase {

    /// Normal path (no race): the state machine is already in the map when beforeIndexShardClosed fires.
    /// beforeIndexShardClosed removes and cancels it; afterIndexShardClosed must be a no-op and must not
    /// trip the isFirstCancellation assertion in StateMachine.cancel().
    public void testBeforeIndexShardClosedCleansUpWhenStateMachinePresentAndAfterIsNoOp() {
        var threadPool = mock(ThreadPool.class);
        var clusterService = mock(ClusterService.class);
        var reshardIndexService = mock(ReshardIndexService.class);
        when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);

        var splitTargetService = new SplitTargetService(Settings.EMPTY, new NoOpClient(threadPool), clusterService, reshardIndexService);
        var splitSourceService = mock(SplitSourceService.class);

        var listener = new StatelessIndexNodeRecoveryListener(
            threadPool,
            mock(StatelessCommitService.class),
            mock(ObjectStoreService.class),
            mock(TranslogReplicator.class),
            mock(SharedBlobCacheWarmingService.class),
            mock(HollowShardsService.class),
            splitTargetService,
            splitSourceService,
            mock(ProjectResolver.class),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            mock(StatelessSharedBlobCacheService.class),
            mock(SnapshotsCommitService.class),
            mock(StatelessPrimaryRelocationMetricsCollector.class)
        );

        var shardId = new ShardId("index", "uuid", 1);
        var split = new SplitTargetService.Split(shardId, DiscoveryNodeUtils.create("source"), DiscoveryNodeUtils.create("target"), 2, 2);

        var indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);

        // Step 1: state machine added before beforeIndexShardClosed fires (normal, non-race case).
        splitTargetService.initializeSplitInCloneState(indexShard, split);
        assertFalse(
            "state machine was added before beforeIndexShardClosed — should be non-empty",
            splitTargetService.getShardsWithOngoingSplits().isEmpty()
        );

        // Step 2: beforeIndexShardClosed fires — finds and removes the state machine.
        listener.beforeIndexShardClosed(shardId, indexShard, Settings.EMPTY);
        assertTrue(
            "beforeIndexShardClosed must clean up the state machine that was present",
            splitTargetService.getShardsWithOngoingSplits().isEmpty()
        );

        // Step 3: afterIndexShardClosed fires — map is already empty, must be a no-op.
        listener.afterIndexShardClosed(shardId, indexShard, Settings.EMPTY);
        assertTrue(
            "afterIndexShardClosed must remain a no-op when beforeIndexShardClosed already cleaned up",
            splitTargetService.getShardsWithOngoingSplits().isEmpty()
        );
    }

    /// Regression test for the race between beforeIndexShardClosed and startSplitTargetShardRecovery (issue #160169).
    ///
    /// beforeIndexShardClosed calls splitTargetService.cancelSplits, but if startSplitTargetShardRecovery runs after
    /// it — in the window between beforeIndexShardClosed and afterIndexShardClosed — the state machine is added to
    /// onGoingSplits with no subsequent cleanup. The fix adds a second cancelSplits call in afterIndexShardClosed.
    public void testAfterIndexShardClosedCleansUpSplitTargetStateMachineAddedAfterFirstCancel() {
        var threadPool = mock(ThreadPool.class);
        var clusterService = mock(ClusterService.class);
        var reshardIndexService = mock(ReshardIndexService.class);
        when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);

        var splitTargetService = new SplitTargetService(Settings.EMPTY, new NoOpClient(threadPool), clusterService, reshardIndexService);
        var splitSourceService = mock(SplitSourceService.class);

        var listener = new StatelessIndexNodeRecoveryListener(
            threadPool,
            mock(StatelessCommitService.class),
            mock(ObjectStoreService.class),
            mock(TranslogReplicator.class),
            mock(SharedBlobCacheWarmingService.class),
            mock(HollowShardsService.class),
            splitTargetService,
            splitSourceService,
            mock(ProjectResolver.class),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            mock(StatelessSharedBlobCacheService.class),
            mock(SnapshotsCommitService.class),
            mock(StatelessPrimaryRelocationMetricsCollector.class)
        );

        var shardId = new ShardId("index", "uuid", 1);
        var split = new SplitTargetService.Split(shardId, DiscoveryNodeUtils.create("source"), DiscoveryNodeUtils.create("target"), 2, 2);

        var indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);

        // Step 1: beforeIndexShardClosed fires — calls cancelSplits but map is empty, so it's a no-op.
        listener.beforeIndexShardClosed(shardId, indexShard, Settings.EMPTY);
        assertTrue(splitTargetService.getShardsWithOngoingSplits().isEmpty());

        // Step 2: startSplitTargetShardRecovery runs in the race window and adds a state machine.
        splitTargetService.initializeSplitInCloneState(indexShard, split);
        assertFalse(
            "state machine was added after beforeIndexShardClosed — should be non-empty before afterIndexShardClosed",
            splitTargetService.getShardsWithOngoingSplits().isEmpty()
        );

        // Step 3: afterIndexShardClosed fires — the fix ensures this also calls splitTargetService.cancelSplits.
        listener.afterIndexShardClosed(shardId, indexShard, Settings.EMPTY);
        assertTrue(
            "afterIndexShardClosed must clean up the split target state machine added in the race window",
            splitTargetService.getShardsWithOngoingSplits().isEmpty()
        );
    }
}
