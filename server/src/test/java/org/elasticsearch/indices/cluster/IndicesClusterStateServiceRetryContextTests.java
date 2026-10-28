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
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.recovery.FailureStrategy;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryListener;
import org.elasticsearch.indices.recovery.RecoveryMetricsCollector;
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
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING;
import static org.elasticsearch.indices.recovery.FailureStrategy.FAIL_SEND;
import static org.elasticsearch.indices.recovery.FailureStrategy.RETRY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/// Unit tests for pending-allocation-state markers: each allocation's state carries its retry count so either
/// the retry applier or cluster-state application may recreate the shard with the same count.
public class IndicesClusterStateServiceRetryContextTests extends AbstractIndicesClusterStateServiceTestCase {

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

    /// Validate the test setup with FAIL_SEND strategy
    public void testFailSendNotifyMaster() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(shard);
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("simulated recovery failure"),
            shard.recoveryState(),
            currentCreatingShard(shardRouting)
        );

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        assertTrue(isFailureReported(shardRouting));
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
        assertThat(pendingApplierTasks.size(), equalTo(0));
    }

    public void testRetryBecomesFailSendWhenShardPastRecovering() {
        ShardRouting shardRouting = applyInitializingPrimary();
        MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(shard);
        shard.setState(IndexShardState.POST_RECOVERY);

        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("simulated recovery failure"),
            shard.recoveryState(),
            currentCreatingShard(shardRouting)
        );

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertFalse(isAwaitingRecoveryRetry(shardRouting));
        assertTrue(isFailureReported(shardRouting));
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
        assertThat(pendingApplierTasks.size(), equalTo(0));
    }

    public void testRetrySetContextWithoutNotifyingMaster() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        handleRecoveryFailureWithRetry(shardRouting);

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(1));
        assertFalse(isFailureReported(shardRouting));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
        assertThat(pendingApplierTasks.size(), equalTo(1));
        assertThat(pendingApplierTasks.peek().source(), equalTo("retry recovery " + shardRouting.shardId()));
    }

    public void testRetryApplierRecreatesShardAndClearsContext() {
        ShardRouting shardRouting = applyInitializingPrimary();
        int createsBefore = indicesService.createShardCalls.get();

        handleRecoveryFailureWithRetry(shardRouting);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(isAwaitingRecoveryRetry(shardRouting));

        drainApplierTasks();

        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        // CreatingShard is held through recovery
        assertThat(
            indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class)
        );
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());

        completeRecoveryViaListener();
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
    }

    public void testConsecutiveRetriesIncrementLocalRetries() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(0));

        failRecoveryViaListener(RETRY);
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(1));
        drainApplierTasks();
        shardRouting = indicesService.getShardOrNull(shardRouting.shardId()).routingEntry();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));

        failRecoveryViaListener(RETRY);
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(2));
        drainApplierTasks();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(2));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testGiveUpResetsLocalRetriesToZero() {
        ShardRouting shardRouting = applyInitializingPrimary();
        failRecoveryViaListener(RETRY);
        drainApplierTasks();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));

        // New allocation id → context cleared; cluster-state create owns recreate with localRecoveryRetries=0.
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
        assertFalse(isAwaitingRecoveryRetry(shardRouting));
        assertTrue(isFailureReported(shardRouting));
        assertTrue(pendingApplierTasks.isEmpty());
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
    }

    public void testClusterStateCreateWhileContextAppliesLocalRetries() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        int createsAfterFail = indicesService.createShardCalls.get();
        assertTrue(isAwaitingRecoveryRetry(shardRouting));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        // Intervening cluster-state apply may recreate; context supplies localRecoveryRetries.
        applyState(ClusterState.builder(state).version(state.version() + 1).build());
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterFail + 1));
        // CreatingShard now spans recovery too: recovery for this CS-apply-created attempt is still pending.
        assertThat(
            indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class)
        );

        // Retry applier still pending; context already cleared so it must not create again.
        assertThat(pendingApplierTasks.size(), equalTo(1));
        int createsAfterCs = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        completeRecoveryViaListener();
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
    }

    public void testGiveUpOnAllocationChangeThenClusterStateCreates() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRecoveryRetry(shardRouting));

        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        assertFalse(newAllocation.isSameAllocation(shardRouting));
        applyState(stateWithShardRouting(newAllocation));

        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
        // Retry applier task is still pending; when drained it must give up without another create.
        assertThat(pendingApplierTasks.size(), equalTo(1));
        int createsAfterCs = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    public void testUpdateRetryContextClearsWhenShardNotOnLocalNode() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRecoveryRetry(shardRouting));

        applyState(stateWithShardMovedOffLocalNode(shardRouting));
        assertTrue(indicesClusterStateService.pendingAllocationStates.isEmpty());
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testUpdateRetryContextClearsWhenAllocationIdChanges() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRecoveryRetry(shardRouting));

        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(newAllocation));
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
    }

    public void testPendingAllocationStateClearedWhenIndexCloses() {
        ShardRouting shardRouting = applyInitializingPrimary();
        final var originalCreatingShard = currentCreatingShard(shardRouting);
        int createsBefore = indicesService.createShardCalls.get();

        // Closing the index doesn't change the shard's routing/allocation id, so without clearing
        // pendingAllocationStates explicitly here, the old ticket would survive and block the shard
        // from ever being recreated (see IndicesClusterStateService#removeIndicesAndShards).
        applyState(stateWithIndexClosed(shardRouting.index()));

        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        final var newTicket = indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting));
        assertThat(newTicket, instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class));
        assertNotSame(originalCreatingShard, newTicket);
    }

    public void testPendingAllocationStateClearedWhenPrimaryTermBumped() {
        ShardRouting shardRouting = applyInitializingPrimary();
        final var originalCreatingShard = currentCreatingShard(shardRouting);
        int createsBefore = indicesService.createShardCalls.get();

        // Batched cluster-state processing can take a primary straight from initializing(term1) to
        // initializing(term2) with the same routing/allocation id (see
        // IndicesClusterStateService#removeIndicesAndShards's "primary term bumped" branch). The old
        // ticket must not survive this, or the new chapter's createShard would find it still "owned".
        applyState(stateWithPrimaryTermBumped(shardRouting));

        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        final var newTicket = indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting));
        assertThat(newTicket, instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class));
        assertNotSame(originalCreatingShard, newTicket);
    }

    public void testAwaitingRecoveryRetryResetsLocalRetriesWhenPrimaryTermBumped() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(1));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        // The sweep in updatePendingAllocationStates must evict the retry ticket,
        // or the next attempt would inherit the stale (pre-bump) retry count.
        applyState(stateWithPrimaryTermBumped(shardRouting));

        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(0));
    }

    public void testCreatingShardTicketResetWhenPrimaryTermBumpedDuringLockWait() throws Exception {
        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();

        // Force the very first createShard attempt to hit shard-lock contention -> lock-wait retry loop
        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");
        applyState(initial);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertBusy(() -> assertFalse(pendingApplierTasks.isEmpty()));

        final var originalCreatingShard = currentCreatingShard(shardRouting);
        int createsBefore = indicesService.createShardCalls.get();

        // The sweep in updatePendingAllocationStates must evict the stale ticket,
        // or the eventual lock-wait wakeup could act on a superseded term.
        applyState(stateWithPrimaryTermBumped(shardRouting));

        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        final var newTicket = indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting));
        assertThat(newTicket, instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class));
        assertNotSame(originalCreatingShard, newTicket);

        // The orphaned lock-wait retry (still holding the old, now-evicted ticket) must give up quietly
        // rather than clobber the new attempt's ticket.
        final var lockRetry = pendingApplierTasks.remove();
        lockRetry.consumer().accept(state);
        lockRetry.listener().onResponse(null);
        assertSame(newTicket, indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)));
    }

    public void testPendingAllocationStateClearedWhenReplicaPromoted() {
        String indexName = randomIndexName();
        ClusterState base = ClusterStateCreationUtils.state(indexName, false, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        String localNodeId = base.nodes().getLocalNodeId();
        Index index = base.metadata().getProject().index(indexName).getIndex();
        ShardRouting oldPrimary = base.routingTable().index(index).shard(0).primaryShard();
        assertTrue(oldPrimary.active());
        assertFalse(oldPrimary.currentNodeId().equals(localNodeId));
        ShardRouting replica = TestShardRouting.newShardRouting(oldPrimary.shardId(), localNodeId, false, ShardRoutingState.INITIALIZING);
        applyState(
            ClusterState.builder(base)
                .routingTable(
                    RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(oldPrimary).addShard(replica).build()).build()
                )
                .build()
        );
        assertNotNull(indicesService.getShardOrNull(replica.shardId()));
        final var originalCreatingShard = currentCreatingShard(replica);
        int createsBefore = indicesService.createShardCalls.get();

        // Old primary failed; the former replica is promoted (same allocation id), bumping the primary
        // term (see IndicesClusterStateService#removeIndicesAndShards's "primary promotion" branch). The
        // old ticket must not survive this, or the new chapter's createShard would find it still "owned".
        ShardRouting promoted = TestShardRouting.shardRoutingBuilder(replica.shardId(), localNodeId, true, ShardRoutingState.INITIALIZING)
            .withAllocationId(replica.allocationId())
            .build();
        ShardRouting oldPrimaryGone = TestShardRouting.newShardRouting(oldPrimary.shardId(), null, false, ShardRoutingState.UNASSIGNED);
        IndexMetadata currentMetadata = state.metadata().getProject().index(index);
        IndexMetadata bumped = IndexMetadata.builder(currentMetadata)
            .primaryTerm(replica.id(), currentMetadata.primaryTerm(replica.id()) + 1)
            .build();
        applyState(
            ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).put(bumped, false))
                .routingTable(
                    RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(promoted).addShard(oldPrimaryGone).build()).build()
                )
                .build()
        );

        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        final var newTicket = indicesClusterStateService.pendingAllocationStates.get(allocationKey(replica));
        assertThat(newTicket, instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class));
        assertNotSame(originalCreatingShard, newTicket);
    }

    public void testPendingAllocationStatesClearedWhenStatePersistenceDisabled() {
        ShardRouting shardRouting = applyInitializingPrimary();
        currentCreatingShard(shardRouting); // sanity: a live ticket exists before the block is applied
        assertFalse(indicesClusterStateService.pendingAllocationStates.isEmpty());

        applyState(
            ClusterState.builder(state)
                .blocks(ClusterBlocks.builder().blocks(state.blocks()).addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build()
        );

        assertTrue(indicesClusterStateService.pendingAllocationStates.isEmpty());
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testUpdateRetryContextClearsWhenNotInitializing() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRecoveryRetry(shardRouting));

        ShardRouting started = TestShardRouting.shardRoutingBuilder(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.STARTED
        ).withAllocationId(shardRouting.allocationId()).build();
        applyState(stateWithShardRouting(started));
        assertFalse(isAwaitingRecoveryRetry(shardRouting));
        // Master thinks this allocation is active but we have no shard for it (it was removed by the
        // earlier recovery failure) -- createIndicesAndUpdateShards reports it as failed separately.
        assertTrue(isFailureReported(shardRouting));
    }

    // NOTE: a test formerly lived here ("...ClearsWhenFailedShardsCacheHit") simulating a FailureReported
    // write landing on top of an existing AwaitingRecoveryRetry via a second direct handleRecoveryFailure(FAIL_SEND)
    // call. That scenario is no longer constructible: handleRecoveryFailure now requires the caller's
    // ticket to match the live pendingAllocationStates entry, and a single CreatingShard attempt can only ever
    // produce one terminal signal (RecoveryListener#assertOnce), so two independent handleRecoveryFailure
    // calls for the same attempt can't happen. The only remaining production path that can still write
    // FailureReported over a live AwaitingRecoveryRetry is FailedShardHandler (a separate, ticket-independent
    // caller -- see the "leave FailedShardHandler as is" decision). testRetryDoesNotClobberFailureReported
    // covers the mirror-image ordering (FailureReported already present, then a stale RETRY arrives).

    public void testRetryContextKeptWhenCreateGivesUpMissingPeerSource() {
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
        assertTrue(isAwaitingRecoveryRetry(replica));

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
        // CS apply may attempt create; missing peer source → early return, keep marker for localRecoveryRetries.
        assertTrue(isAwaitingRecoveryRetry(replica));

        int createsBefore = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertTrue(isAwaitingRecoveryRetry(replica));
        assertThat(awaitingRecoveryRetryCount(replica), equalTo(1));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore));
        assertNull(indicesService.getShardOrNull(replica.shardId()));
    }

    public void testRetryContextClearedWhenCreateFails() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRecoveryRetry(shardRouting));

        indicesService.failNextCreateShard = new IOException("simulated create failure");
        drainApplierTasks();

        assertFalse(isAwaitingRecoveryRetry(shardRouting));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        // create failure path notifies master via failAndRemoveShard(sendShardFailure=true)
        verify(shardStateAction).localShardFailed(any(), anyString(), any(), any(), any());
        assertTrue(isFailureReported(shardRouting));
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

        final var withRelocating = TestShardRouting.shardRoutingBuilder(
            shardId,
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withAllocationId(AllocationId.newRelocation(shardRouting.allocationId())).withRelocatingNodeId(otherNodeId).build();
        applyState(stateWithShardRouting(withRelocating));
        assertNotNull(indicesService.getShardOrNull(shardId));

        handleRecoveryFailureWithRetry(withRelocating);
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(1));

        final var clearedRelocating = TestShardRouting.shardRoutingBuilder(
            shardId,
            withRelocating.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withAllocationId(AllocationId.finishRelocation(withRelocating.allocationId())).build();
        assertTrue(clearedRelocating.isSameAllocation(withRelocating));
        assertNotEquals(clearedRelocating.allocationId(), withRelocating.allocationId());
        assertThat(clearedRelocating.relocatingNodeId(), nullValue());
        // Publish routing change without CS create so the retry applier is what recreates,
        // proving the retry path passes currentRouting (relocating cleared) into createShard.
        state = stateWithShardRouting(clearedRelocating);
        when(clusterService.state()).thenReturn(state);
        assertTrue(isAwaitingRecoveryRetry(shardRouting));

        drainApplierTasks();

        MockIndexShard recreated = indicesService.getShardOrNull(shardId);
        assertNotNull(recreated);
        assertThat(recreated.routingEntry().relocatingNodeId(), nullValue());
        assertThat(recreated.routingEntry().allocationId(), equalTo(clearedRelocating.allocationId()));
        assertThat(recreated.recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(isAwaitingRecoveryRetry(shardRouting));
    }

    public void testRetryDoesNotRegressAnExistingFresherMarker() {
        final var shardRouting = applyInitializingPrimary();
        // Capture the original ticket before it's spent, so the later "stale" call below can present it.
        final var originalCreatingShard = currentCreatingShard(shardRouting);
        handleRecoveryFailureWithRetry(shardRouting);
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(1));

        // Seed a fresher marker, as if a newer attempt had already recorded more retries.
        indicesClusterStateService.pendingAllocationStates.put(
            allocationKey(shardRouting),
            new IndicesClusterStateService.PendingAllocationState.AwaitingRecoveryRetry(5, primaryTerm(shardRouting))
        );
        int tasksBefore = pendingApplierTasks.size();

        // A late failure from the original (now-superseded) attempt, presenting its own now-stale
        // ticket, must not regress the marker nor schedule a second retry task.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("stale failure"),
            recoveryState(shardRouting, 0),
            originalCreatingShard
        );
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(5));
        assertThat(pendingApplierTasks.size(), equalTo(tasksBefore));
    }

    public void testRetryDoesNotClobberFailureReported() {
        final var shardRouting = applyInitializingPrimary();
        final var creatingShard = currentCreatingShard(shardRouting);

        // A failure already reported to master for this exact allocation.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("already failed to master"),
            recoveryState(shardRouting, 0),
            creatingShard
        );
        assertTrue(isFailureReported(shardRouting));
        int tasksBefore = pendingApplierTasks.size();

        // A second call presenting the same (now-spent) ticket must be a no-op: FailureReported survives.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("stale failure"),
            recoveryState(shardRouting, 0),
            creatingShard
        );
        assertTrue(isFailureReported(shardRouting));
        assertFalse(isAwaitingRecoveryRetry(shardRouting));
        assertThat(pendingApplierTasks.size(), equalTo(tasksBefore));
    }

    public void testLateFailureFromPreviousAllocationPreservesReplacementRetry() {
        final var oldRouting = applyInitializingPrimary();
        final var oldListener = indicesService.lastRecoveryListener;
        final var oldRecoveryState = indicesService.getShardOrNull(oldRouting.shardId()).recoveryState();
        final var replacement = TestShardRouting.newShardRouting(
            oldRouting.shardId(),
            oldRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(replacement));
        failRecoveryViaListener(RETRY);

        oldListener.onRecoveryFailure(
            oldRecoveryState,
            new RecoveryFailedException(oldRecoveryState, "late failure", new IOException("old attempt")),
            RETRY
        );
        assertThat(awaitingRecoveryRetryCount(replacement), equalTo(1));

        drainApplierTasks();

        final var recreated = indicesService.getShardOrNull(replacement.shardId());
        assertNotNull(recreated);
        assertTrue(recreated.routingEntry().isSameAllocation(replacement));
        assertThat(recreated.recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(isAwaitingRecoveryRetry(replacement));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testLockWaitSurvivesUnrelatedClusterStateUpdate() throws Exception {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(1));

        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");
        final var recoveryRetry = pendingApplierTasks.remove();
        recoveryRetry.consumer().accept(state);
        recoveryRetry.listener().onResponse(null);
        assertBusy(() -> assertFalse(pendingApplierTasks.isEmpty()));

        // An unrelated cluster-state update (same allocation, just a different stateUUID) while the
        // lock-wait is pending must not cause the continuation to give up -- only a genuine
        // allocation change should. The claim survives and uses the fresh state once it wakes up.
        state = ClusterState.builder(state).stateUUID(UUIDs.randomBase64UUID()).build();
        when(clusterService.state()).thenReturn(state);

        final var lockRetry = pendingApplierTasks.remove();
        lockRetry.consumer().accept(state);
        lockRetry.listener().onResponse(null);

        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(isAwaitingRecoveryRetry(shardRouting));
        // CreatingShard is held through recovery, then released once recovery actually finishes.
        assertThat(
            indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class)
        );
        completeRecoveryViaListener();
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
    }

    public void testLockWaitGivesUpWhenAllocationGoesStale() throws Exception {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertThat(awaitingRecoveryRetryCount(shardRouting), equalTo(1));

        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");
        final var recoveryRetry = pendingApplierTasks.remove();
        recoveryRetry.consumer().accept(state);
        recoveryRetry.listener().onResponse(null);
        assertBusy(() -> assertFalse(pendingApplierTasks.isEmpty()));

        // A genuine allocation change while the lock-wait is pending must cause the continuation to
        // give up quietly and clean up its own claim, rather than attempt to create a shard for an
        // allocation that no longer exists.
        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(newAllocation));
        int createsAfterCs = indicesService.createShardCalls.get();

        final var lockRetry = pendingApplierTasks.remove();
        lockRetry.consumer().accept(state);
        lockRetry.listener().onResponse(null);

        assertFalse(isAwaitingRecoveryRetry(shardRouting));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    // --- helpers ---

    private IndicesClusterStateService createService(boolean localRetryEnabled) {
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(INDICES_RECOVERY_LOCAL_RETRY_SETTING);
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_LOCAL_RETRY_SETTING.getKey(), localRetryEnabled)
            .put(SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(1))
            .build();
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
            RetentionLeaseSyncer.EMPTY,
            null,
            RecoveryMetricsCollector.NOOP
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
        MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(shard);
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("simulated recovery failure"),
            shard.recoveryState(),
            currentCreatingShard(shardRouting)
        );
    }

    private static IndicesClusterStateService.ShardAllocationKey allocationKey(ShardRouting routing) {
        return IndicesClusterStateService.ShardAllocationKey.fromRouting(routing);
    }

    /// Fetches the currently-claimed [IndicesClusterStateService.PendingAllocationState.CreatingShard] ticket for
    /// `routing`'s allocation, for tests that call [IndicesClusterStateService#handleRecoveryFailure]
    /// directly rather than through the real [RecoveryListener].
    private IndicesClusterStateService.PendingAllocationState.CreatingShard currentCreatingShard(ShardRouting routing) {
        var current = indicesClusterStateService.pendingAllocationStates.get(allocationKey(routing));
        assertThat(current, instanceOf(IndicesClusterStateService.PendingAllocationState.CreatingShard.class));
        return (IndicesClusterStateService.PendingAllocationState.CreatingShard) current;
    }

    private boolean isAwaitingRecoveryRetry(ShardRouting routing) {
        return indicesClusterStateService.pendingAllocationStates.get(
            allocationKey(routing)
        ) instanceof IndicesClusterStateService.PendingAllocationState.AwaitingRecoveryRetry;
    }

    private Integer awaitingRecoveryRetryCount(ShardRouting routing) {
        var current = indicesClusterStateService.pendingAllocationStates.get(allocationKey(routing));
        return current instanceof IndicesClusterStateService.PendingAllocationState.AwaitingRecoveryRetry(var n, var ignoredTerm)
            ? n
            : null;
    }

    private boolean isFailureReported(ShardRouting routing) {
        return indicesClusterStateService.pendingAllocationStates.get(
            allocationKey(routing)
        ) instanceof IndicesClusterStateService.PendingAllocationState.FailureReported;
    }

    private RecoveryState recoveryState(ShardRouting shardRouting, int localRetries) {
        return new RecoveryState(shardRouting, state.nodes().getLocalNode(), null, localRetries);
    }

    /// Drives the production increment path via [RecoveryListener#onRecoveryFailure]
    /// rather than calling [IndicesClusterStateService#handleRecoveryFailure] directly.
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

    /// Completes the currently-captured [RecoveryListener] successfully via
    /// [RecoveryListener#onRecoveryDone], so tests can verify the creating-shard ticket is released
    /// once recovery actually finishes (it's deliberately held through recovery, not just construction
    /// -- see [RecordingIndicesService#autoCompleteRecovery]).
    private void completeRecoveryViaListener() {
        RecoveryListener listener = indicesService.lastRecoveryListener;
        assertNotNull("createShard must have captured a RecoveryListener", listener);
        MockIndexShard shard = indicesService.getShardOrNull(indicesService.lastCreatedShardId);
        assertNotNull(shard);
        listener.onRecoveryDone(shard.recoveryState(), ShardLongFieldRange.UNKNOWN, ShardLongFieldRange.UNKNOWN);
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

    private ClusterState stateWithIndexClosed(Index index) {
        IndexMetadata current = state.metadata().getProject().index(index);
        IndexMetadata closed = IndexMetadata.builder(current).state(IndexMetadata.State.CLOSE).build();
        return ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).put(closed, false)).build();
    }

    private ClusterState stateWithPrimaryTermBumped(ShardRouting shardRouting) {
        Index index = shardRouting.index();
        IndexMetadata current = state.metadata().getProject().index(index);
        long newTerm = current.primaryTerm(shardRouting.id()) + 1;
        IndexMetadata bumped = IndexMetadata.builder(current).primaryTerm(shardRouting.id(), newTerm).build();
        return ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).put(bumped, false)).build();
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
        Runnable afterShardCreated;

        @Override
        protected boolean autoCompleteRecovery() {
            // This test drives recovery completion/failure manually via lastRecoveryListener/failRecoveryViaListener.
            return false;
        }

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
            int localRecoveryRetries
        ) throws IOException {
            createShardCalls.incrementAndGet();
            lastRecoveryListener = recoveryListener;
            lastCreatedShardId = shardRouting.shardId();
            Exception toFail = failNextCreateShard;
            if (toFail != null) {
                failNextCreateShard = null;
                if (toFail instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
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
                localRecoveryRetries
            );
            final var callback = afterShardCreated;
            afterShardCreated = null;
            if (callback != null) {
                callback.run();
            }
        }
    }
}
