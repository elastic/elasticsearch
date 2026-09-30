/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.FailureStrategy;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryListener;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.indices.cluster.IndicesClusterStateService.INDICES_RECOVERY_LOCAL_RETRY_SETTING;
import static org.elasticsearch.indices.recovery.FailureStrategy.FAIL_SEND;
import static org.elasticsearch.indices.recovery.FailureStrategy.RETRY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for local recovery-retry handoff ({@code retryingShards}): the data node owns recreate
 * while a handoff marker is present, and cluster-state application must not create in parallel.
 */
public class IndicesClusterStateServiceRetryHandoffTests extends AbstractIndicesClusterStateServiceTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ClusterApplierService clusterApplierService;
    private ShardStateAction shardStateAction;
    private RecordingIndicesService indicesService;
    private IndicesClusterStateService indicesClusterStateService;
    private ClusterState state;
    private final Queue<PendingApplierTask> pendingApplierTasks = new ArrayDeque<>();

    private record PendingApplierTask(String source, Consumer<ClusterState> consumer, ActionListener<Void> listener) {}

    @Before
    public void setUpService() {
        disableRandomFailures();
        threadPool = new TestThreadPool(getTestName());
        pendingApplierTasks.clear();
        shardStateAction = mock(ShardStateAction.class);
        clusterService = mock(ClusterService.class);
        clusterApplierService = mock(ClusterApplierService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        doAnswer(invocation -> {
            pendingApplierTasks.add(
                new PendingApplierTask(invocation.getArgument(0), invocation.getArgument(2), invocation.getArgument(3))
            );
            return null;
        }).when(clusterApplierService).runOnApplierThread(anyString(), any(Priority.class), any(), any());

        indicesClusterStateService = createService(true);
        indicesClusterStateService.start();
    }

    @After
    public void tearDownService() {
        if (indicesClusterStateService != null) {
            indicesClusterStateService.close();
        }
        assertThat(ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS), equalTo(true));
    }

    public void testRetryMarksHandoffWithoutNotifyingMaster() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        handleRecoveryFailureWithRetry(shardRouting);

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(
            indicesClusterStateService.retryingShards.get(shardRouting.shardId()),
            equalTo(new IndicesClusterStateService.RetryHandoff(shardRouting, 1))
        );
        assertTrue(indicesClusterStateService.failedShardsCache.isEmpty());
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
        assertThat(pendingApplierTasks.size(), equalTo(1));
        assertThat(pendingApplierTasks.peek().source(), equalTo("retry recovery " + shardRouting.shardId()));
    }

    public void testRetryApplierRecreatesShardAndClearsHandoff() {
        ShardRouting shardRouting = applyInitializingPrimary();
        int createsBefore = indicesService.createShardCalls.get();

        handleRecoveryFailureWithRetry(shardRouting);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        drainApplierTasks();

        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testConsecutiveRetriesIncrementLocalRetries() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(0));

        failRecoveryViaListener(RETRY);
        assertThat(
            indicesClusterStateService.retryingShards.get(shardRouting.shardId()),
            equalTo(new IndicesClusterStateService.RetryHandoff(shardRouting, 1))
        );
        drainApplierTasks();
        shardRouting = indicesService.getShardOrNull(shardRouting.shardId()).routingEntry();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));

        failRecoveryViaListener(RETRY);
        assertThat(
            indicesClusterStateService.retryingShards.get(shardRouting.shardId()),
            equalTo(new IndicesClusterStateService.RetryHandoff(shardRouting, 2))
        );
        drainApplierTasks();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(2));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testGiveUpResetsLocalRetriesToZero() {
        ShardRouting shardRouting = applyInitializingPrimary();
        failRecoveryViaListener(RETRY);
        drainApplierTasks();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));

        // New allocation id → handoff cleared; cluster-state create owns recreate with localRetries=0.
        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        assertFalse(newAllocation.isSameAllocation(shardRouting));
        applyState(stateWithShardRouting(newAllocation));

        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(0));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    public void testRetryDisabledBecomesFailSend() {
        indicesClusterStateService.close();
        indicesClusterStateService = createService(false);
        indicesClusterStateService.start();

        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(indicesClusterStateService.retryingShards.isEmpty());
        assertTrue(indicesClusterStateService.failedShardsCache.containsKey(shardRouting.shardId()));
        assertTrue(pendingApplierTasks.isEmpty());
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
    }

    public void testClusterStateSkipsCreateWhileHandoffPresent() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        int createsAfterFail = indicesService.createShardCalls.get();
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        // Intervening cluster-state apply must not recreate while handoff owns the shard.
        applyState(ClusterState.builder(state).version(state.version() + 1).build());
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterFail));
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        drainApplierTasks();
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
    }

    public void testGiveUpOnAllocationChangeThenClusterStateCreates() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        assertFalse(newAllocation.isSameAllocation(shardRouting));
        applyState(stateWithShardRouting(newAllocation));

        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
        // Retry applier task is still pending; when drained it must give up without another create.
        assertThat(pendingApplierTasks.size(), equalTo(1));
        int createsAfterCs = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    public void testUpdateRetryHandoffClearsWhenShardNotOnLocalNode() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        applyState(stateWithShardMovedOffLocalNode(shardRouting));
        assertTrue(indicesClusterStateService.retryingShards.isEmpty());
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testUpdateRetryHandoffClearsWhenAllocationIdChanges() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(newAllocation));
        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
    }

    public void testUpdateRetryHandoffClearsWhenNotInitializing() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        ShardRouting started = TestShardRouting.shardRoutingBuilder(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.STARTED
        ).withAllocationId(shardRouting.allocationId()).build();
        applyState(stateWithShardRouting(started));
        assertTrue(indicesClusterStateService.retryingShards.isEmpty());
    }

    public void testUpdateRetryHandoffClearsWhenFailedShardsCacheHit() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        // Concurrent FAIL_SEND fills failedShardsCache while the RETRY handoff is still present.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("concurrent fail-send"),
            0
        );
        assertTrue(indicesClusterStateService.failedShardsCache.containsKey(shardRouting.shardId()));

        applyState(ClusterState.builder(state).version(state.version() + 1).build());
        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testUpdateRetryHandoffClearsWhenIndexServiceMissing() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        indicesService.removeIndex(
            shardRouting.index(),
            IndexRemovalReason.NO_LONGER_ASSIGNED,
            "test",
            Runnable::run,
            ActionListener.noop()
        );
        applyState(ClusterState.builder(state).version(state.version() + 1).build());

        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
    }

    public void testUpdateRetryHandoffClearsWhenShardAlreadyExists() throws IOException {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        indicesService.indexService(shardRouting.index()).createShard(shardRouting);
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        // handoff marker still present
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        applyState(ClusterState.builder(state).version(state.version() + 1).build());
        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
    }

    public void testUpdateRetryingShardsClearsAllWhenLocalRoutingNodeMissing() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        // Empty routing table → RoutingNodes.node(local) is null → clear all handoffs.
        applyState(ClusterState.builder(state).routingTable(RoutingTable.builder().build()).metadata(Metadata.builder().build()).build());
        assertTrue(indicesClusterStateService.retryingShards.isEmpty());
    }

    public void testHandoffClearedWhenCreateGivesUpMissingPeerSource() {
        String indexName = randomIndexName();
        ClusterState base = ClusterStateCreationUtils.state(indexName, false, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        String localNodeId = base.nodes().getLocalNodeId();
        Index index = base.metadata().getProject().index(indexName).getIndex();
        ShardRouting primary = base.routingTable().index(index).shard(0).primaryShard();
        assertTrue(primary.active());
        assertFalse(primary.currentNodeId().equals(localNodeId));
        ShardRouting replica = TestShardRouting.newShardRouting(primary.shardId(), localNodeId, false, ShardRoutingState.INITIALIZING);
        assertThat(replica.recoverySource().getType(), equalTo(RecoverySource.Type.PEER));
        applyState(
            ClusterState.builder(base)
                .routingTable(
                    RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica).build()).build()
                )
                .build()
        );
        assertNotNull(indicesService.getShardOrNull(replica.shardId()));

        handleRecoveryFailureWithRetry(replica);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(replica.shardId()));

        // Remove node holding primary and make sure master is an existing node
        applyState(
            ClusterState.builder(state)
                .nodes(
                    DiscoveryNodes.builder(state.nodes())
                        .remove(primary.currentNodeId())
                        .masterNodeId(state.nodes().getLocalNodeId())
                        .build()
                )
                .build()
        );
        assertTrue(indicesClusterStateService.retryingShards.containsKey(replica.shardId()));

        int createsBefore = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertFalse(indicesClusterStateService.retryingShards.containsKey(replica.shardId()));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore));
        assertNull(indicesService.getShardOrNull(replica.shardId()));
    }

    public void testHandoffClearedWhenCreateFails() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        indicesService.failNextCreateShard = new IOException("simulated create failure");
        drainApplierTasks();

        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        // create failure path notifies master via failAndRemoveShard(sendShardFailure=true)
        verify(shardStateAction).localShardFailed(any(), anyString(), any(), any(), any());
        assertTrue(indicesClusterStateService.failedShardsCache.containsKey(shardRouting.shardId()));
    }

    public void testRetryUsesCurrentRoutingFromClusterState() {
        ShardRouting shardRouting = applyInitializingPrimary();
        ShardId shardId = shardRouting.shardId();
        String otherNodeId = null;
        for (DiscoveryNode node : state.nodes()) {
            if (node.getId().equals(state.nodes().getLocalNodeId()) == false) {
                otherNodeId = node.getId();
                break;
            }
        }
        assertNotNull(otherNodeId);

        ShardRouting withRelocating = TestShardRouting.shardRoutingBuilder(
            shardId,
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withAllocationId(shardRouting.allocationId()).withRelocatingNodeId(otherNodeId).build();
        applyState(stateWithShardRouting(withRelocating));
        assertNotNull(indicesService.getShardOrNull(shardId));

        handleRecoveryFailureWithRetry(withRelocating);
        assertThat(
            indicesClusterStateService.retryingShards.get(shardId),
            equalTo(new IndicesClusterStateService.RetryHandoff(withRelocating, 1))
        );

        ShardRouting clearedRelocating = TestShardRouting.shardRoutingBuilder(
            shardId,
            withRelocating.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withAllocationId(withRelocating.allocationId()).build();
        assertThat(clearedRelocating.relocatingNodeId(), nullValue());
        applyState(stateWithShardRouting(clearedRelocating));
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardId));

        drainApplierTasks();

        MockIndexShard recreated = indicesService.getShardOrNull(shardId);
        assertNotNull(recreated);
        assertThat(recreated.routingEntry().relocatingNodeId(), nullValue());
        assertThat(recreated.routingEntry().allocationId(), equalTo(clearedRelocating.allocationId()));
        assertFalse(indicesClusterStateService.retryingShards.containsKey(shardId));
    }

    public void testDoubleRetryHandoffAsserts() {
        assumeTrue("assertion-only guard", Assertions.ENABLED);
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(indicesClusterStateService.retryingShards.containsKey(shardRouting.shardId()));

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> indicesClusterStateService.handleRecoveryFailure(
                shardRouting,
                RETRY,
                primaryTerm(shardRouting),
                new Exception("again"),
                2
            )
        );
        assertThat(error.getMessage(), equalTo("retry handoff already present for " + shardRouting.shardId()));
    }

    // --- helpers ---

    private IndicesClusterStateService createService(boolean localRetryEnabled) {
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(INDICES_RECOVERY_LOCAL_RETRY_SETTING);
        Settings settings = Settings.builder().put(INDICES_RECOVERY_LOCAL_RETRY_SETTING.getKey(), localRetryEnabled).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        state = ClusterState.EMPTY_STATE;
        when(clusterService.state()).thenAnswer(invocation -> state);

        indicesService = new RecordingIndicesService();
        return new IndicesClusterStateService(
            settings,
            indicesService,
            clusterService,
            threadPool,
            null,
            shardStateAction,
            null,
            null,
            null,
            null,
            mock(PrimaryReplicaSyncer.class),
            null,
            null,
            null
        );
    }

    private ShardRouting applyInitializingPrimary() {
        applyState(ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING));
        ShardRouting shardRouting = state.getRoutingNodes().node(state.nodes().getLocalNodeId()).iterator().next();
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.initializing());
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        return shardRouting;
    }

    private void handleRecoveryFailureWithRetry(ShardRouting shardRouting) {
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("simulated recovery failure"),
            1
        );
    }

    /// Drives the production increment path: `ShardRecoveryListener` reads
    /// `state.getLocalRetries() + 1` rather than calling [IndicesClusterStateService#handleRecoveryFailure] directly.
    private void failRecoveryViaListener(FailureStrategy failureStrategy) {
        RecoveryListener listener = indicesService.lastRecoveryListener;
        assertNotNull("createShard must have captured a RecoveryListener", listener);
        MockIndexShard shard = indicesService.getShardOrNull(indicesService.lastCreatedShardId);
        assertNotNull(shard);
        RecoveryState recoveryState = shard.recoveryState();
        listener.onRecoveryFailure(
            recoveryState,
            new RecoveryFailedException(recoveryState, "simulated", new Exception("simulated")),
            failureStrategy
        );
    }

    private long primaryTerm(ShardRouting shardRouting) {
        return state.metadata().getProject().index(shardRouting.index()).primaryTerm(shardRouting.id());
    }

    private void applyState(ClusterState newState) {
        ClusterState previous = state;
        state = newState;
        when(clusterService.state()).thenReturn(state);
        indicesClusterStateService.applyClusterState(new ClusterChangedEvent("test", state, previous));
    }

    private void drainApplierTasks() {
        while (pendingApplierTasks.isEmpty() == false) {
            PendingApplierTask task = pendingApplierTasks.poll();
            task.consumer().accept(state);
            task.listener().onResponse(null);
        }
    }

    private ClusterState stateWithShardRouting(ShardRouting shardRouting) {
        Index index = shardRouting.index();
        IndexMetadata indexMetadata = state.metadata().getProject().index(index);
        assertNotNull("index metadata must exist for " + index, indexMetadata);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();
        return ClusterState.builder(state).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
    }

    private ClusterState stateWithShardMovedOffLocalNode(ShardRouting shardRouting) {
        Index index = shardRouting.index();
        IndexMetadata indexMetadata = state.metadata().getProject().index(index);
        String otherNodeId = null;
        for (DiscoveryNode node : state.nodes()) {
            if (node.getId().equals(state.nodes().getLocalNodeId()) == false) {
                otherNodeId = node.getId();
                break;
            }
        }
        assertNotNull(otherNodeId);
        ShardRouting elsewhere = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            otherNodeId,
            true,
            ShardRoutingState.INITIALIZING
        );
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(elsewhere).build();
        return ClusterState.builder(state)
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .metadata(Metadata.builder(state.metadata()).put(indexMetadata, false))
            .build();
    }

    private class RecordingIndicesService extends MockIndicesService {
        final AtomicInteger createShardCalls = new AtomicInteger();
        volatile Exception failNextCreateShard;
        volatile RecoveryListener lastRecoveryListener;
        volatile ShardId lastCreatedShardId;

        @Override
        public void createShard(
            ProjectId projectId,
            ShardRouting shardRouting,
            PeerRecoveryTargetService recoveryTargetService,
            RecoveryListener recoveryListener,
            RepositoriesService repositoriesService,
            Consumer<IndexShard.ShardFailure> onShardFailure,
            GlobalCheckpointSyncer globalCheckpointSyncer,
            RetentionLeaseSyncer retentionLeaseSyncer,
            DiscoveryNode targetNode,
            DiscoveryNode sourceNode,
            long clusterStateVersion,
            int localRetries
        ) throws IOException {
            createShardCalls.incrementAndGet();
            lastRecoveryListener = recoveryListener;
            lastCreatedShardId = shardRouting.shardId();
            Exception toFail = failNextCreateShard;
            if (toFail != null) {
                failNextCreateShard = null;
                if (toFail instanceof IOException ioe) {
                    throw ioe;
                }
                throw new IOException(toFail);
            }
            super.createShard(
                projectId,
                shardRouting,
                recoveryTargetService,
                recoveryListener,
                repositoriesService,
                onShardFailure,
                globalCheckpointSyncer,
                retentionLeaseSyncer,
                targetNode,
                sourceNode,
                clusterStateVersion,
                localRetries
            );
        }
    }
}
