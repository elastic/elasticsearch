/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.MockUtils;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCancelRecoveriesActionTests extends ESTestCase {

    private ClusterService clusterService;
    private IndicesService indicesService;
    private ThrottlingRecoveryService throttlingRecoveryService;
    private TransportCancelRecoveriesAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        final var transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        clusterService = mock(ClusterService.class);
        indicesService = mock(IndicesService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create("test-node"));
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        throttlingRecoveryService = new ThrottlingRecoveryService(
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            clusterService,
            new CompositeRecoverySchedulingListener()
        );
        when(indicesService.throttlingRecoveryService()).thenReturn(throttlingRecoveryService);

        action = new TransportCancelRecoveriesAction(
            transportService,
            new ActionFilters(Set.of()),
            clusterService,
            indicesService,
            mock(PeerRecoveryTargetService.class)
        );
    }

    public void testMissingIndexIsIgnored() {
        final var indexName = randomIndexName();
        final var shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        when(indicesService.indexServiceSafe(eq(shardId.getIndex()))).thenThrow(new IndexNotFoundException("index not found"));

        final var future = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), requestForShard(shardId, allocationId, 0L, true), future);
        assertNotNull(future.actionGet());
    }

    public void testShardNotRecoveringIsIgnored() {
        final var indexName = randomIndexName();
        final var shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var indexService = mock(IndexService.class);
        final var indexShard = mock(IndexShard.class);
        final var routing = ShardRouting.newUnassigned(
            shardId,
            true,
            mock(RecoverySource.class),
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            ShardRouting.Role.DEFAULT
        ).initialize(randomIdentifier(), allocationId, 0L).moveToStarted(0L);

        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(routing);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);

        final var future = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), requestForShard(shardId, allocationId, 0L, true), future);
        assertNotNull(future.actionGet());

        verify(indexShard, never()).requestRecoveryCancellation(any());
    }

    public void testCancelIfStartedFalseSkipsDirectCancellation() {
        final var indexName = randomIndexName();
        final var shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var indexService = mock(IndexService.class);
        final var indexShard = mock(IndexShard.class);
        final var routing = ShardRouting.newUnassigned(
            shardId,
            true,
            mock(RecoverySource.class),
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            ShardRouting.Role.DEFAULT
        ).initialize(randomIdentifier(), allocationId, 0L);

        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(routing);
        when(indexShard.state()).thenReturn(IndexShardState.RECOVERING);

        final var future = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), requestForShard(shardId, allocationId, 0L, false), future);
        assertNotNull(future.actionGet());

        verify(indexShard, never()).requestRecoveryCancellation(any());
    }

    public void testCancelIfStartedFalseStillCancelsQueuedRecovery() {
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        // Makes sure the cancellation gets recorded to throttlingRecoveryService
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenThrow(new IndexNotFoundException("not found"));

        final var future = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), requestForShard(shardId, allocationId, 0L, false), future);

        final var cancelled = new AtomicBoolean(false);
        final var recoveryState = mock(RecoveryState.class);
        when(recoveryState.getRecoverySource()).thenReturn(RecoverySource.ExistingStoreRecoverySource.INSTANCE);
        when(recoveryState.getShardId()).thenReturn(shardId);

        final var recoveryStarted = new AtomicBoolean(false);
        throttlingRecoveryService.enqueue(new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {}

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                cancelled.set(true);
            }

            @Override
            public void onRecoveryAborted() {}
        }, recoveryState, allocationId, new RecoveryStats(), listener -> recoveryStarted.set(true));

        assertTrue("recovery should be cancelled at queueing time", cancelled.get());
        assertFalse("recovery task should not have been invoked", recoveryStarted.get());
    }

    public void testExceptionInOneCancellationDoesNotBlockOthers() {
        final var indexName = randomIndexName();
        final var shardId1 = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId1 = UUIDs.randomBase64UUID();
        final var shardId2 = new ShardId("other", UUIDs.randomBase64UUID(), 0);
        final var allocationId2 = UUIDs.randomBase64UUID();

        when(indicesService.indexServiceSafe(shardId1.getIndex())).thenThrow(new IndexNotFoundException("index not found"));
        when(indicesService.indexServiceSafe(shardId2.getIndex())).thenThrow(new IndexNotFoundException("index not found"));

        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId1, allocationId1, true),
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId2, allocationId2, true)
            )
        );

        final var future = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), request, future);
        future.actionGet();

        verify(indicesService).indexServiceSafe(shardId1.getIndex());
        verify(indicesService).indexServiceSafe(shardId2.getIndex());
    }

    public void testActionIsDelayedUntilRequiredClusterStateVersion() {
        final var indexName = randomIndexName();
        final var shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var oldClusterState = ClusterState.EMPTY_STATE;
        when(clusterService.state()).thenReturn(oldClusterState);

        when(indicesService.indexServiceSafe(any())).thenThrow(new IndexNotFoundException("index not found"));

        final var future = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), requestForShard(shardId, allocationId, oldClusterState.version() + 1L, true), future);

        assertFalse("processing should be deferred until cluster state version is met", future.isDone());

        // ThrottlingRecoveryService also registers a ClusterStateListener so we use atLeast(1)
        final var listenerCaptor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService, atLeastOnce()).addListener(listenerCaptor.capture());

        final var advancedState = mock(ClusterState.class);
        when(advancedState.version()).thenReturn(oldClusterState.version() + 1L);
        final var event = mock(ClusterChangedEvent.class);
        when(event.state()).thenReturn(advancedState);
        listenerCaptor.getValue().clusterChanged(event);

        assertTrue("processing should have completed after cluster state advanced", future.isDone());
        verify(indicesService).indexServiceSafe(shardId.getIndex());
    }

    public void testReshardSplitCancellationIsNotSupported() {
        final var indexName = randomIndexName();
        final var shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var indexService = mock(IndexService.class);
        final var indexShard = mock(IndexShard.class);
        final var routing = ShardRouting.newUnassigned(
            shardId,
            true,
            new RecoverySource.ReshardSplitRecoverySource(shardId),
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            ShardRouting.Role.DEFAULT
        ).initialize(randomIdentifier(), allocationId, 0L);

        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        when(indexShard.routingEntry()).thenReturn(routing);
        when(indexShard.state()).thenReturn(IndexShardState.RECOVERING);

        try (var mockLog = MockLog.capture(TransportCancelRecoveriesAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warn for unsupported RESHARD_SPLIT cancellation",
                    TransportCancelRecoveriesAction.class.getCanonicalName(),
                    Level.WARN,
                    "*encountered error when direct cancelling shard*"
                )
            );

            final var future = new PlainActionFuture<CancelRecoveriesAction.Response>();
            action.execute(mock(Task.class), requestForShard(shardId, allocationId, 0L, true), future);
            assertNotNull(future.actionGet());

            mockLog.assertAllExpectationsMatched();
        }
        verify(indexShard, never()).requestRecoveryCancellation(any());
    }

    public void testCancellationStoredAndAppliedWhenShardNotYetCreated() {
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        when(indicesService.indexServiceSafe(shardId.getIndex())).thenThrow(new IndexNotFoundException("not found"));
        final var storeFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), requestForShard(shardId, allocationId, 0L, true), storeFuture);
        storeFuture.actionGet();

        // Simulate the shard being created and its recovery being enqueued
        final var cancelled = new AtomicBoolean(false);
        final var recoveryState = mock(RecoveryState.class);
        when(recoveryState.getRecoverySource()).thenReturn(RecoverySource.ExistingStoreRecoverySource.INSTANCE);
        when(recoveryState.getShardId()).thenReturn(shardId);

        final var taskInvoked = new AtomicBoolean(false);
        throttlingRecoveryService.enqueue(new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {}

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                cancelled.set(true);
            }

            @Override
            public void onRecoveryAborted() {

            }
        }, recoveryState, allocationId, new RecoveryStats(), ignored -> taskInvoked.set(true));

        assertTrue("recovery should have been cancelled at enqueue time", cancelled.get());
        assertFalse("recovery task should not have been invoked", taskInvoked.get());
    }

    private CancelRecoveriesAction.Request requestForShard(ShardId sid, String aid, long clusterStateVersion, boolean cancelIfStarted) {
        return new CancelRecoveriesAction.Request(
            clusterStateVersion,
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(sid, aid, cancelIfStarted))
        );
    }
}
