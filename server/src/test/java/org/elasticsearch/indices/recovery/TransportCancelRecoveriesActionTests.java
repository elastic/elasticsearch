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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
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
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportCancelRecoveriesActionTests extends ESTestCase {

    private IndicesService indicesService;
    private DeterministicTaskQueue taskQueue;
    private ThrottlingRecoveryService throttlingRecoveryService;
    private TransportCancelRecoveriesAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue();
        final var clusterService = mock(ClusterService.class);
        indicesService = mock(IndicesService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create("test-node"));
        final var clusterSettings = new ClusterSettings(
            Settings.builder().put(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey(), 1).build(),
            Set.of(ThrottlingRecoveryService.INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        throttlingRecoveryService = new ThrottlingRecoveryService(
            taskQueue.getThreadPool(),
            DefaultProjectResolver.INSTANCE,
            clusterService,
            RecoverySchedulingListener.NOOP
        );
        action = new TransportCancelRecoveriesAction(
            MockUtils.setupTransportServiceWithThreadpoolExecutor(),
            new ActionFilters(Set.of()),
            clusterService,
            indicesService,
            throttlingRecoveryService
        );
    }

    public void testActionResponseIncludesAllCancelledRecoveriesInQueue() throws Exception {
        final var shardId0 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId0 = UUIDs.randomBase64UUID();
        final var shardId1 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 1);
        final var allocationId1 = UUIDs.randomBase64UUID();
        final var shardId2 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 2);
        final var allocationId2 = UUIDs.randomBase64UUID();

        // running recovery
        final var recoveryState0 = newRecoveryState(shardId0);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState0,
            allocationId0,
            new RecoveryStats(),
            l -> taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> l.onRecoveryDone(recoveryState0, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            )
        );

        // queued recovery
        final var recoveryState1 = newRecoveryState(shardId1);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState1,
            allocationId1,
            new RecoveryStats(),
            ignored -> fail("recovery should be cancelled")
        );

        // queued recovery
        final var recoveryState2 = newRecoveryState(shardId2);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState2,
            allocationId2,
            new RecoveryStats(),
            ignored -> fail("recovery should be cancelled")
        );

        taskQueue.runAllRunnableTasks();
        assertThat(throttlingRecoveryService.currentQueueSize(), equalTo(2));

        final var indexShard = mockIndexShard(shardId0, allocationId0);
        final var indexService = mockIndexServiceForShard(indexShard);
        when(indicesService.indexServiceSafe(eq(shardId0.getIndex()))).thenReturn(indexService);

        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId0, allocationId0, true),
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId1, allocationId1, true),
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId2, allocationId2, true)
            )
        );
        action.execute(mock(Task.class), request, responseFuture);
        final var response = responseFuture.actionGet();
        assertThat(response.cancelledInQueue(), equalTo(Set.of(allocationId1, allocationId2)));
        assertThrows((RecoveryCancelledException.class), indexShard::ensureRecoveryNotCancelled);
    }

    public void testCancelIfStartedFalseCancelsQueuedRecoveryButSkipsDirectCancellation() throws Exception {
        final var shardId0 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId0 = UUIDs.randomBase64UUID();
        final var shardId1 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 1);
        final var allocationId1 = UUIDs.randomBase64UUID();

        // running recovery
        final var recoveryState0 = newRecoveryState(shardId0);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState0,
            allocationId0,
            new RecoveryStats(),
            l -> taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> l.onRecoveryDone(recoveryState0, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            )
        );

        // queued recovery
        final var recoveryState1 = newRecoveryState(shardId1);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState1,
            allocationId1,
            new RecoveryStats(),
            ignored -> fail("recovery should be cancelled")
        );
        taskQueue.runAllRunnableTasks();
        assertThat(throttlingRecoveryService.currentQueueSize(), equalTo(1));

        final var indexShard = mockIndexShard(shardId0, allocationId0);
        final var indexService = mockIndexServiceForShard(indexShard);
        when(indicesService.indexServiceSafe(eq(shardId0.getIndex()))).thenReturn(indexService);

        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId0, allocationId0, false),
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId1, allocationId1, false)
            )
        );
        action.execute(mock(Task.class), request, responseFuture);
        final var response = responseFuture.actionGet();
        assertThat(response.cancelledInQueue(), equalTo(Set.of(allocationId1)));
        indexShard.ensureRecoveryNotCancelled();
    }

    public void testActionIgnoresMissingIndex() throws Exception {
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        when(indicesService.indexServiceSafe(eq(shardId.getIndex()))).thenThrow(new IndexNotFoundException("index not found"));

        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        action.execute(mock(Task.class), request, responseFuture);
        final var response = responseFuture.actionGet();
        assertTrue(response.cancelledInQueue().isEmpty());
    }

    public void testActionIgnoresShardNotRecovering() throws Exception {
        final var indexName = randomIndexName();
        final var shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var indexShard = mockIndexShard(shardId, allocationId);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        final var indexService = mockIndexServiceForShard(indexShard);

        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);

        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        action.execute(mock(Task.class), request, responseFuture);
        final var response = responseFuture.actionGet();
        assertTrue(response.cancelledInQueue().isEmpty());

        indexShard.ensureRecoveryNotCancelled();
    }

    public void testCancellationIgnoredWhenAllocationIdDoesNotMatch() throws Exception {
        final var shardId0 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId0 = UUIDs.randomBase64UUID();
        final var shardId1 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 1);
        final var allocationId1 = UUIDs.randomBase64UUID();

        // running recovery
        final var recoveryState0 = newRecoveryState(shardId0);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState0,
            allocationId0,
            new RecoveryStats(),
            l -> taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> l.onRecoveryDone(recoveryState0, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            )
        );

        // queued recovery
        final var recoveryState1 = newRecoveryState(shardId1);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState1,
            allocationId1,
            new RecoveryStats(),
            ignored -> {}
        );

        taskQueue.runAllRunnableTasks();
        assertThat(throttlingRecoveryService.currentQueueSize(), equalTo(1));

        final var indexShard = mockIndexShard(shardId0, allocationId0);
        final var indexService = mockIndexServiceForShard(indexShard);
        when(indicesService.indexServiceSafe(shardId0.getIndex())).thenReturn(indexService);

        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId0, UUIDs.randomBase64UUID(), true),
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId1, UUIDs.randomBase64UUID(), false)
            )
        );
        action.execute(mock(Task.class), request, responseFuture);
        final var response = responseFuture.actionGet();
        assertTrue(response.cancelledInQueue().isEmpty());
        indexShard.ensureRecoveryNotCancelled();
        assertThat(throttlingRecoveryService.currentQueueSize(), equalTo(1));
        taskQueue.runAllTasks();
    }

    public void testExceptionInOneCancellationDoesNotAffectOthers() throws Exception {
        final var shardId0 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId0 = UUIDs.randomBase64UUID();
        final var shardId1 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 1);
        final var allocationId1 = UUIDs.randomBase64UUID();
        final var shardId2 = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 2);
        final var allocationId2 = UUIDs.randomBase64UUID();

        when(indicesService.indexServiceSafe(shardId1.getIndex())).thenThrow(new IndexNotFoundException("index not found"));
        final var indexShard0 = mockIndexShard(shardId0, allocationId0);
        final var indexService0 = mockIndexServiceForShard(indexShard0);

        when(indicesService.indexServiceSafe(shardId0.getIndex())).thenReturn(indexService0);

        // running recovery
        final var recoveryState0 = newRecoveryState(shardId0);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState0,
            allocationId0,
            new RecoveryStats(),
            l -> taskQueue.scheduleAt(
                taskQueue.getCurrentTimeMillis() + 100,
                () -> l.onRecoveryDone(recoveryState0, ShardLongFieldRange.EMPTY, ShardLongFieldRange.EMPTY)
            )
        );

        // queued recovery
        final var recoveryState2 = newRecoveryState(shardId2);
        throttlingRecoveryService.enqueue(
            ProjectId.DEFAULT,
            RecoveryListener.NOOP,
            recoveryState2,
            allocationId2,
            new RecoveryStats(),
            ignored -> fail("recovery should be cancelled")
        );

        taskQueue.runAllRunnableTasks();
        assertThat(throttlingRecoveryService.currentQueueSize(), equalTo(1));

        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId0, allocationId0, true),
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId1, allocationId1, true),
                new CancelRecoveriesAction.ShardRecoveryCancellation(shardId2, allocationId2, true)
            )
        );
        action.execute(mock(Task.class), request, responseFuture);
        final var response = responseFuture.actionGet();
        assertThat(response.cancelledInQueue(), equalTo(Set.of(allocationId2)));

        assertThrows((RecoveryCancelledException.class), indexShard0::ensureRecoveryNotCancelled);
    }

    @TestLogging(reason = "test asserts DEBUG log", value = "org.elasticsearch.indices.recovery.TransportCancelRecoveriesAction:DEBUG")
    public void testCancellationOfStartedPeerRecoveryIsNotSupported() throws Exception {
        final var indexName = randomIndexName();
        final var shardId = new ShardId(indexName, UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();
        final var indexService = mock(IndexService.class);
        final var indexShard = mock(IndexShard.class);
        final var routing = ShardRouting.newUnassigned(
            shardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
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
                    "debug log for unsupported PEER cancellation",
                    TransportCancelRecoveriesAction.class.getCanonicalName(),
                    Level.DEBUG,
                    "*cancellation flag cannot be set on*PEER recoveries do not currently support "
                        + "direct cancellation of started recoveries*"
                )
            );
            final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
            final var request = new CancelRecoveriesAction.Request(
                0L,
                List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
            );
            action.execute(mock(Task.class), request, responseFuture);
            final var response = responseFuture.actionGet();
            assertTrue(response.cancelledInQueue().isEmpty());
            mockLog.assertAllExpectationsMatched();
        }
        indexShard.ensureRecoveryNotCancelled();
    }

    @TestLogging(reason = "test asserts DEBUG log", value = "org.elasticsearch.indices.recovery.TransportCancelRecoveriesAction:DEBUG")
    public void testCancellationOfStartedReshardSplitRecoveriesIsNotSupported() throws Exception {
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
                    "debug log for unsupported RESHARD_SPLIT cancellation",
                    TransportCancelRecoveriesAction.class.getCanonicalName(),
                    Level.DEBUG,
                    "*cancellation flag cannot be set on*RESHARD_SPLIT recoveries do not currently support "
                        + "direct cancellation of started recoveries*"
                )
            );
            final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
            final var request = new CancelRecoveriesAction.Request(
                0L,
                List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
            );
            action.execute(mock(Task.class), request, responseFuture);
            final var response = responseFuture.actionGet();
            assertTrue(response.cancelledInQueue().isEmpty());
            mockLog.assertAllExpectationsMatched();
        }
        indexShard.ensureRecoveryNotCancelled();
    }

    public void testCancellationStoredAndAppliedWhenShardNotYetCreated() throws Exception {
        final var shardId = new ShardId(randomIndexName(), UUIDs.randomBase64UUID(), 0);
        final var allocationId = UUIDs.randomBase64UUID();

        when(indicesService.indexServiceSafe(shardId.getIndex())).thenThrow(new IndexNotFoundException("not found"));
        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        final var request = new CancelRecoveriesAction.Request(
            0L,
            List.of(new CancelRecoveriesAction.ShardRecoveryCancellation(shardId, allocationId, true))
        );
        action.execute(mock(Task.class), request, responseFuture);
        final var response = responseFuture.actionGet();
        assertTrue(response.cancelledInQueue().isEmpty());

        final var cancelled = new AtomicBoolean();
        throttlingRecoveryService.enqueue(ProjectId.DEFAULT, new RecoveryListener() {
            @Override
            public void onRecoveryDone(
                RecoveryState state,
                ShardLongFieldRange timestampMillisFieldRange,
                ShardLongFieldRange eventIngestedMillisFieldRange
            ) {
                fail("recovery should be cancelled");
            }

            @Override
            public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
                cancelled.set(true);
            }

            @Override
            public void onRecoveryAborted() {
                fail("recovery should be cancelled");
            }
        }, newRecoveryState(shardId), allocationId, new RecoveryStats(), ignored -> fail("recovery should be cancelled"));

        taskQueue.runAllTasks();
        assertTrue("expected recovery to be cancelled", cancelled.get());
    }

    public void testEmptyRequestReturnsEmptyResponse() {
        final var responseFuture = new PlainActionFuture<CancelRecoveriesAction.Response>();
        action.execute(mock(Task.class), new CancelRecoveriesAction.Request(0L, List.of()), responseFuture);
        final var response = responseFuture.actionGet();
        assertTrue(response.cancelledInQueue().isEmpty());
    }

    private static RecoveryState newRecoveryState(ShardId shardId) {
        return newRecoveryState(randomFrom(RecoverySource.Type.PEER, RecoverySource.Type.EMPTY_STORE), shardId);
    }

    private static RecoveryState newRecoveryState(RecoverySource.Type type, ShardId shardId) {
        final var routing = TestShardRouting.newShardRouting(
            shardId,
            "node",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        return new RecoveryState(routing, null, DiscoveryNodeUtils.create("target"));
    }

    private static IndexService mockIndexServiceForShard(IndexShard indexShard) {
        final var indexService = mock(IndexService.class);
        when(indexService.getShard(indexShard.shardId().id())).thenReturn(indexShard);
        return indexService;
    }

    private static IndexShard mockIndexShard(ShardId shardId, String allocationId) throws Exception {
        final var indexShard = mock(IndexShard.class);
        final var routing = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            ShardRouting.Role.DEFAULT
        ).initialize(randomIdentifier(), allocationId, 0L);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(routing);
        when(indexShard.state()).thenReturn(IndexShardState.RECOVERING);

        final var cancelled = new AtomicReference<RecoveryCancelledException>();
        doAnswer(invocation -> {
            cancelled.set(invocation.getArgument(0));
            return null;
        }).when(indexShard).requestRecoveryCancellation(any());
        doAnswer(ignored -> {
            final var exception = cancelled.get();
            if (exception != null) {
                throw exception;
            }
            return null;
        }).when(indexShard).ensureRecoveryNotCancelled();
        return indexShard;
    }
}
