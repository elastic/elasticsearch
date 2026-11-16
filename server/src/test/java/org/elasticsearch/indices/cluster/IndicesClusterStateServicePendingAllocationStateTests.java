/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
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
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.indices.cluster.IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.SHARD_LOCK_RETRY_TIMEOUT_SETTING;
import static org.elasticsearch.indices.recovery.FailureStrategy.ABORT;
import static org.elasticsearch.indices.recovery.FailureStrategy.FAIL_SEND;
import static org.elasticsearch.indices.recovery.FailureStrategy.FAIL_SILENT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/// Unit tests for the unified pending-allocation-state bookkeeping ([IndicesClusterStateService.ShardAllocationKey],
/// [IndicesClusterStateService.PendingAllocationState]) and the ticket lifecycle built on top of it: claiming
/// ownership of a shard-creation attempt, holding it through the whole recovery (not just construction), the
/// cluster-state sweep that evicts stale tickets, and the shard-lock-retry continuation re-resolving routing and
/// source node on each iteration. No local recovery retry exists yet -- those tests belong here once it does.
public class IndicesClusterStateServicePendingAllocationStateTests extends AbstractIndicesClusterStateServiceTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ClusterService clusterService;
    private ClusterApplierService clusterApplierService;
    private ShardStateAction shardStateAction;
    private RecordingIndicesService indicesService;
    private IndicesClusterStateService indicesClusterStateService;
    private ClusterState state;

    @Before
    public void setUpService() {
        disableRandomFailures();
        deterministicTaskQueue = new DeterministicTaskQueue();
        shardStateAction = mock(ShardStateAction.class);
        clusterService = mock(ClusterService.class);
        clusterApplierService = mock(ClusterApplierService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        doAnswer(invocation -> {
            final Consumer<ClusterState> consumer = invocation.getArgument(2);
            final ActionListener<Void> listener = invocation.getArgument(3);
            consumer.accept(state);
            listener.onResponse(null);
            return null;
        }).when(clusterApplierService).runOnApplierThread(anyString(), any(Priority.class), any(), any());

        indicesClusterStateService = createService();
        indicesClusterStateService.start();
    }

    @After
    public void tearDownService() {
        if (indicesClusterStateService != null) {
            indicesClusterStateService.close();
        }
    }

    public void testFailSendNotifyMaster() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("simulated recovery failure"),
            currentInFlight(shardRouting)
        );

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(isFailureReported(shardRouting));
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
    }

    public void testAbortClearsInFlightTicketWithoutNotifyingMaster() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertNotNull(indicesService.lastRecoveryListener);

        // ABORT is the symptom of a shard closing; master is told through other means (or already knows),
        // so this must only release the ticket, never notify master.
        indicesService.failRecoveryViaListener(ABORT);

        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testAbortIgnoredWhenTicketSuperseded() {
        ShardRouting shardRouting = applyInitializingPrimary();
        RecoveryListener supersededListener = indicesService.lastRecoveryListener;
        assertNotNull(supersededListener);
        MockIndexShard supersededShard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(supersededShard);
        RecoveryState supersededRecoveryState = supersededShard.recoveryState();

        // A primary term bump for this same allocation (e.g. a batched cluster-state application
        // that goes straight from initializing(term1) to initializing(term2)) evicts the old ticket
        // via updatePendingAllocationTracker and claims a new one for the recreated allocation.
        applyState(stateWithPrimaryTermBumped(shardRouting));
        final var newTicket = currentInFlight(shardRouting);

        // The superseded attempt's own recovery listener aborts late. It must be a no-op: the new
        // ticket survives untouched, and master is never notified (ABORT's whole point).
        supersededListener.onRecoveryFailure(
            supersededRecoveryState,
            new RecoveryFailedException(supersededRecoveryState, "simulated", new Exception("simulated")),
            ABORT
        );

        assertSame(newTicket, indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testInFlightHeldThroughRecoveryReleasedOnCompletion() {
        ShardRouting shardRouting = applyInitializingPrimary();
        // Unlike shard construction alone, the InFlight ticket spans the whole recovery: it must still
        // be the live ticket here, well after createShard's own listener has already resolved.
        assertThat(
            indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.PendingAllocationState.InFlight.class)
        );

        indicesService.completeRecoveryViaListener();
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
        verify(shardStateAction).shardStarted(eq(shardRouting), anyLong(), anyString(), any(), any(), any());
    }

    public void testCreateShardFailureReportsToMaster() {
        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();
        indicesService.failNextCreateShard = new ElasticsearchException("simulated create failure");

        applyState(initial);

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(isFailureReported(shardRouting));
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
    }

    public void testPendingAllocationStateClearedWhenShardNotOnLocalNode() {
        ShardRouting shardRouting = applyInitializingPrimary();
        currentInFlight(shardRouting); // sanity: a live ticket exists before the move below

        applyState(stateWithShardMovedOffLocalNode(shardRouting));
        assertTrue(indicesClusterStateService.pendingAllocationStates.isEmpty());
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testPendingAllocationStateClearedWhenAllocationIdChanges() {
        ShardRouting shardRouting = applyInitializingPrimary();
        currentInFlight(shardRouting);

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
        Index index = shardRouting.index();
        currentInFlight(shardRouting);

        // Closing an index rebuilds its routing table with fresh, unassigned shards (see
        // RoutingTable.Builder#addAsFromOpenToClose), so the sweep in updatePendingAllocationTracker
        // evicts the stale ticket on its own via the currentRouting == null case.
        applyState(stateWithIndexClosed(index));
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        int createsBefore = indicesService.createShardCalls.get();

        // Once the index reopens and the allocator assigns a fresh allocation, the shard is created
        // normally -- nothing is left over from before the close to block it.
        ShardRouting reopenedRouting = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            state.nodes().getLocalNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        assertNotEquals(shardRouting.allocationId().getId(), reopenedRouting.allocationId().getId());
        applyState(stateWithIndexReopenedAndShardRouting(reopenedRouting));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        final var newTicket = indicesClusterStateService.pendingAllocationStates.get(allocationKey(reopenedRouting));
        assertThat(newTicket, instanceOf(IndicesClusterStateService.PendingAllocationState.InFlight.class));
    }

    public void testPendingAllocationStateClearedWhenRestoredOverOpenIndex() {
        ShardRouting shardRouting = applyInitializingPrimary();
        Index index = shardRouting.index();
        currentInFlight(shardRouting);

        // A restore over an already-open index (different history UUID, same index UUID) rebuilds the
        // routing table with fresh, unassigned shards the same way close/reopen does (see
        // RoutingTable.Builder#addAsRestore), so the sweep evicts the stale ticket the same way.
        applyState(stateWithIndexRestoredOverOpen(index));
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testPendingAllocationStateClearedWhenPrimaryTermBumped() {
        ShardRouting shardRouting = applyInitializingPrimary();
        final var originalInFlight = currentInFlight(shardRouting);
        int createsBefore = indicesService.createShardCalls.get();

        // Batched cluster-state processing can take a primary straight from initializing(term1) to
        // initializing(term2) with the same routing/allocation id (see
        // IndicesClusterStateService#removeIndicesAndShards's "primary term bumped" branch). The old
        // ticket must not survive this, or the new term's createShard would find it still "owned".
        applyState(stateWithPrimaryTermBumped(shardRouting));

        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        final var newTicket = indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting));
        assertThat(newTicket, instanceOf(IndicesClusterStateService.PendingAllocationState.InFlight.class));
        assertNotSame(originalInFlight, newTicket);
    }

    public void testFailureReportedEvictedWhenPrimaryTermBumped() {
        ShardRouting shardRouting = applyInitializingPrimary();
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("already failed to master"),
            currentInFlight(shardRouting)
        );
        assertTrue(isFailureReported(shardRouting));

        // Same routing/allocation id, only the primary term advances -- exactly what a batched
        // cluster-state application can produce without this node ever observing the shard as
        // unassigned in between. failureReport should be evicted.
        applyState(stateWithPrimaryTermBumped(shardRouting));

        assertFalse(isFailureReported(shardRouting));
    }

    public void testFailureReportedClearedWhenAllocationIdChanges() {
        ShardRouting shardRouting = applyInitializingPrimary();
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("already failed to master"),
            currentInFlight(shardRouting)
        );
        assertTrue(isFailureReported(shardRouting));

        // A fresh allocation for the same shardId (e.g. a close/reopen this node observes as a single
        // batched jump) must evict the old allocation's FailureReported via evictOrResendFailure's
        // allocation-id-mismatch branch, not just its primary-term-bump one.
        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(newAllocation));

        assertFalse(isFailureReported(shardRouting));
    }

    public void testFailureReportedResentWhileMasterHasNotProcessed() {
        ShardRouting shardRouting = applyInitializingPrimary();
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("already failed to master"),
            currentInFlight(shardRouting)
        );
        assertTrue(isFailureReported(shardRouting));
        verify(shardStateAction, times(1)).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());

        // Same allocation, same primary term -- nothing here tells this node master has actually
        // processed the failure, so evictOrResendFailure's resend branch should fire again rather
        // than silently going quiet.
        applyState(stateWithShardRouting(shardRouting));

        assertTrue(isFailureReported(shardRouting));
        verify(shardStateAction, times(2)).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
    }

    public void testInFlightTicketResetWhenPrimaryTermBumpedDuringLockWait() {
        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();

        // Force the very first createShard attempt to hit shard-lock contention -> lock-wait retry loop
        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");
        applyState(initial);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

        final var originalInFlight = currentInFlight(shardRouting);
        int createsBefore = indicesService.createShardCalls.get();

        // The sweep in updatePendingAllocationTracker must evict the stale ticket,
        // or the eventual lock-wait wakeup could act on a superseded term.
        applyState(stateWithPrimaryTermBumped(shardRouting));

        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        final var newTicket = indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting));
        assertThat(newTicket, instanceOf(IndicesClusterStateService.PendingAllocationState.InFlight.class));
        assertNotSame(originalInFlight, newTicket);

        // The orphaned lock-wait retry (still holding the old, now-evicted ticket) must give up quietly
        // rather than clobber the new attempt's ticket.
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertSame(newTicket, indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)));
    }

    public void testPendingAllocationStatesClearedWhenStatePersistenceDisabled() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertFalse(indicesClusterStateService.pendingAllocationStates.isEmpty());

        applyState(
            ClusterState.builder(state)
                .blocks(ClusterBlocks.builder().blocks(state.blocks()).addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build()
        );

        assertTrue(indicesClusterStateService.pendingAllocationStates.isEmpty());
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testFailureReportedSurvivesWhenStatePersistenceDisabled() {
        ShardRouting shardRouting = applyInitializingPrimary();
        final var inFlight = currentInFlight(shardRouting);

        // A failure already reported to master for this exact allocation.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("already failed to master"),
            inFlight
        );
        assertTrue(isFailureReported(shardRouting));

        applyState(
            ClusterState.builder(state)
                .blocks(ClusterBlocks.builder().blocks(state.blocks()).addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build()
        );

        // FailureReported tracks what master has been told, not a local shard, so it must survive even
        // though every index (and any InFlight ticket) was just torn down.
        assertTrue(isFailureReported(shardRouting));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testStaleTicketDoesNotClobberFailureReported() {
        final var shardRouting = applyInitializingPrimary();
        final var inFlight = currentInFlight(shardRouting);

        // A failure already reported to master for this exact allocation.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("already failed to master"),
            inFlight
        );
        assertTrue(isFailureReported(shardRouting));

        // A second call presenting the same (now-spent) ticket must be a no-op: FailureReported survives.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SILENT,
            primaryTerm(shardRouting),
            new Exception("stale failure"),
            inFlight
        );
        assertTrue(isFailureReported(shardRouting));
    }

    public void testUpdateShardDoesNotResendShardStartedWhileRecoveryInFlight() {
        ShardRouting shardRouting = applyInitializingPrimary();
        currentInFlight(shardRouting); // sanity: ticket is live before the state transition below
        MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(shard);

        // The shard reaches POST_RECOVERY locally (IndexShard#postRecovery's own synchronous
        // transition) before the recovery's own completion callback (onRecoveryDone) has run, so the
        // InFlight ticket is still held. A cluster state update for the same (still-initializing)
        // allocation must not have updateShard resend shardStarted on its behalf -- that would be the
        // first shardStarted ever sent for this attempt, racing ahead of recovery's own completion.
        shard.setState(IndexShardState.POST_RECOVERY);
        applyState(stateWithShardRouting(shardRouting));

        verify(shardStateAction, never()).shardStarted(any(), anyLong(), anyString(), any(), any(), any(), any());
        assertThat(
            indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.PendingAllocationState.InFlight.class)
        );
    }

    public void testUpdateShardResendsShardStartedOnceRecoveryNoLongerInFlight() {
        ShardRouting shardRouting = applyInitializingPrimary();
        MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(shard);

        // Recovery already finished and released its own ticket (independent of whether master
        // actually received the resulting shardStarted, e.g. a master failover lost it), but the
        // cluster state still shows this allocation as initializing. With no InFlight ticket left,
        // updateShard's resend is legitimate.
        indicesClusterStateService.pendingAllocationStates.remove(allocationKey(shardRouting));
        shard.setState(IndexShardState.POST_RECOVERY);
        applyState(stateWithShardRouting(shardRouting));

        verify(shardStateAction).shardStarted(eq(shardRouting), anyLong(), anyString(), any(), any(), any(), any());
    }

    public void testLockWaitSurvivesUnrelatedClusterStateUpdate() {
        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();
        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");

        applyState(initial);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

        // An unrelated cluster-state update (same allocation, just a different stateUUID) while the
        // lock-wait is pending must not cause the continuation to give up -- only a genuine
        // allocation change should. The claim survives and uses the fresh state once it wakes up.
        state = ClusterState.builder(state).stateUUID(UUIDs.randomBase64UUID()).build();
        when(clusterService.state()).thenReturn(state);

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        // InFlight is held through recovery, then released once recovery actually finishes.
        assertThat(
            indicesClusterStateService.pendingAllocationStates.get(allocationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.PendingAllocationState.InFlight.class)
        );
        indicesService.completeRecoveryViaListener();
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(shardRouting)));
    }

    public void testLockWaitGivesUpWhenAllocationGoesStale() {
        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();
        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");

        applyState(initial);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

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

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    public void testLockWaitGivesUpWhenSourceNodeGoesStale() {
        String indexName = randomIndexName();
        ClusterState base = ClusterStateCreationUtils.state(indexName, false, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        String localNodeId = base.nodes().getLocalNodeId();
        Index index = base.metadata().getProject().index(indexName).getIndex();
        ShardRouting primary = base.routingTable().index(index).shard(0).primaryShard();
        assertTrue(primary.active());
        assertFalse(primary.currentNodeId().equals(localNodeId));
        ShardRouting replica = TestShardRouting.newShardRouting(primary.shardId(), localNodeId, false, ShardRoutingState.INITIALIZING);

        indicesService.failNextCreateShard = new ShardLockObtainFailedException(replica.shardId(), "test lock held");
        applyState(
            ClusterState.builder(base)
                .routingTable(
                    RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica).build()).build()
                )
                .build()
        );
        assertNull(indicesService.getShardOrNull(replica.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

        // The primary relocates to a node this cluster state doesn't know about (routing changes,
        // DiscoveryNodes doesn't) -- the replica's own routing/allocation id is unchanged, so its
        // InFlight ticket survives the sweep, but the primary can no longer be resolved to an actual node.
        ShardRouting primaryOnUnknownNode = TestShardRouting.newShardRouting(
            primary.shardId(),
            "unknown-node",
            true,
            ShardRoutingState.STARTED
        );
        applyState(
            ClusterState.builder(state)
                .routingTable(
                    RoutingTable.builder()
                        .add(IndexRoutingTable.builder(index).addShard(primaryOnUnknownNode).addShard(replica).build())
                        .build()
                )
                .build()
        );
        assertTrue(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(replica)));
        int createsBefore = indicesService.createShardCalls.get();

        // Recomputing the source node from the current state (not the stale, captured one) must give up
        // quietly -- no source can be found anymore -- rather than recover from the gone primary.
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore));
        assertFalse(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(replica)));
    }

    public void testLockWaitUsesRelocatedSourceNode() {
        String indexName = randomIndexName();
        ClusterState base = ClusterStateCreationUtils.state(indexName, false, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        String localNodeId = base.nodes().getLocalNodeId();
        Index index = base.metadata().getProject().index(indexName).getIndex();
        ShardRouting primary = base.routingTable().index(index).shard(0).primaryShard();
        assertTrue(primary.active());
        assertFalse(primary.currentNodeId().equals(localNodeId));
        ShardRouting replica = TestShardRouting.newShardRouting(primary.shardId(), localNodeId, false, ShardRoutingState.INITIALIZING);

        indicesService.failNextCreateShard = new ShardLockObtainFailedException(replica.shardId(), "test lock held");
        applyState(
            ClusterState.builder(base)
                .routingTable(
                    RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica).build()).build()
                )
                .build()
        );
        assertNull(indicesService.getShardOrNull(replica.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

        // The primary relocates to a brand new node while the lock-wait is pending. The replica's own
        // routing/allocation id is untouched, so its InFlight ticket survives the sweep.
        DiscoveryNode newPrimaryNode = DiscoveryNodeUtils.builder("new-primary-node").name("new-primary-node").build();
        ShardRouting relocatedPrimary = TestShardRouting.newShardRouting(
            primary.shardId(),
            newPrimaryNode.getId(),
            true,
            ShardRoutingState.STARTED
        );
        applyState(
            ClusterState.builder(state)
                .nodes(DiscoveryNodes.builder(state.nodes()).add(newPrimaryNode))
                .routingTable(
                    RoutingTable.builder()
                        .add(IndexRoutingTable.builder(index).addShard(relocatedPrimary).addShard(replica).build())
                        .build()
                )
                .build()
        );
        assertTrue(indicesClusterStateService.pendingAllocationStates.containsKey(allocationKey(replica)));

        // The retry must recompute and use the relocated primary's new node, not the stale one captured
        // at the original attempt.
        deterministicTaskQueue.runAllTasksInTimeOrder();

        MockIndexShard shard = indicesService.getShardOrNull(replica.shardId());
        assertNotNull(shard);
        assertThat(shard.recoveryState().getSourceNode(), equalTo(newPrimaryNode));
    }

    public void testLockWaitGivesUpWhenNodeShuttingDown() {
        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();
        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");

        applyState(initial);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

        // The node starts shutting down while the shard-lock-retry continuation is pending.
        indicesClusterStateService.stop();
        int createsBeforeShutdownRetry = indicesService.createShardCalls.get();

        deterministicTaskQueue.runAllTasksInTimeOrder();

        // Must give up quietly rather than attempt to create a shard after the service has stopped;
        // doApplyClusterState (and hence the sweep) no longer runs once stopped, so pendingAllocationStates
        // can no longer be trusted to reflect currentState's routing/term.
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBeforeShutdownRetry));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testLockWaitGivesUpAfterTimeout() {
        // A zero timeout means the very first retry iteration already exceeds it
        indicesClusterStateService.close();
        indicesClusterStateService = createService(TimeValue.ZERO);
        indicesClusterStateService.start();

        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();
        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");

        applyState(initial);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

        // Runs the scheduled retry, advancing the virtual clock to its scheduled time in the process.
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertTrue(isFailureReported(shardRouting));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testLockWaitRetryBodyFailureReleasesTicketAndReportsToMaster() {
        ClusterState initial = ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING);
        ShardRouting shardRouting = initial.getRoutingNodes().node(initial.nodes().getLocalNodeId()).iterator().next();
        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");

        applyState(initial);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(deterministicTaskQueue.hasDeferredTasks());

        // Simulate an unexpected exception partway through the retry's own body (the kind of thing
        // findSourceNode could in principle throw). Without a catch around this body, this would leave
        // the InFlight ticket owned forever with nothing left to ever release or recreate it.
        indicesService.failNextIndexServiceLookup = new IllegalStateException("simulated");

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertTrue(isFailureReported(shardRouting));
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
    }

    // --- helpers ---

    private IndicesClusterStateService createService() {
        return createService(SHARD_LOCK_RETRY_TIMEOUT_SETTING.getDefault(Settings.EMPTY));
    }

    private IndicesClusterStateService createService(TimeValue shardLockRetryTimeout) {
        Settings settings = Settings.builder()
            .put(SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(1))
            .put(SHARD_LOCK_RETRY_TIMEOUT_SETTING.getKey(), shardLockRetryTimeout)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        state = ClusterState.EMPTY_STATE;
        when(clusterService.state()).thenAnswer(invocation -> state);

        indicesService = new RecordingIndicesService();
        return new IndicesClusterStateService(
            settings,
            indicesService,
            clusterService,
            deterministicTaskQueue.getThreadPool(),
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

    private static IndicesClusterStateService.ShardAllocationKey allocationKey(ShardRouting routing) {
        return IndicesClusterStateService.ShardAllocationKey.fromRouting(routing);
    }

    private IndicesClusterStateService.PendingAllocationState.InFlight currentInFlight(ShardRouting routing) {
        var current = indicesClusterStateService.pendingAllocationStates.get(allocationKey(routing));
        assertThat(current, instanceOf(IndicesClusterStateService.PendingAllocationState.InFlight.class));
        return (IndicesClusterStateService.PendingAllocationState.InFlight) current;
    }

    private boolean isFailureReported(ShardRouting routing) {
        return indicesClusterStateService.pendingAllocationStates.get(
            allocationKey(routing)
        ) instanceof IndicesClusterStateService.PendingAllocationState.FailureReported;
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

    private ClusterState stateWithShardRouting(ShardRouting shardRouting) {
        Index index = shardRouting.index();
        IndexMetadata indexMetadata = state.metadata().getProject().index(index);
        assertNotNull("index metadata must exist for " + index, indexMetadata);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();
        return ClusterState.builder(state).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
    }

    private ClusterState stateWithIndexClosed(Index index) {
        IndexMetadata current = state.metadata().getProject().index(index);
        IndexMetadata closed = IndexMetadata.builder(current)
            .state(IndexMetadata.State.CLOSE)
            .settings(
                Settings.builder().put(current.getSettings()).put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)
            )
            .settingsVersion(current.getSettingsVersion() + 1)
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, state.routingTable())
            .addAsFromOpenToClose(closed)
            .build();
        return ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).put(closed, false))
            .routingTable(routingTable)
            .build();
    }

    /// Simulates the index reopening and the allocator assigning `shardRouting` (a fresh allocation):
    /// reopening and allocation are two separate cluster-state updates in production, but only the
    /// combined end state matters for the tests using this helper.
    private ClusterState stateWithIndexReopenedAndShardRouting(ShardRouting shardRouting) {
        Index index = shardRouting.index();
        IndexMetadata reopened = IndexMetadata.builder(state.metadata().getProject().index(index)).state(IndexMetadata.State.OPEN).build();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();
        return ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).put(reopened, false))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }

    /// Simulates a restore over an already-open index: same index UUID, different history UUID,
    /// routing rebuilt via the real RoutingTable.Builder#addAsRestore production path.
    private ClusterState stateWithIndexRestoredOverOpen(Index index) {
        IndexMetadata current = state.metadata().getProject().index(index);
        IndexMetadata restored = IndexMetadata.builder(current)
            .settings(Settings.builder().put(current.getSettings()).put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID()))
            .settingsVersion(current.getSettingsVersion() + 1)
            .build();
        SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot("repo", new SnapshotId("snap", UUIDs.randomBase64UUID())),
            IndexVersion.current(),
            new IndexId(index.getName(), index.getUUID())
        );
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, state.routingTable())
            .addAsRestore(restored, recoverySource)
            .build();
        return ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).put(restored, false))
            .routingTable(routingTable)
            .build();
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
        volatile RuntimeException failNextCreateShard;
        volatile RuntimeException failNextIndexServiceLookup;
        volatile RecoveryListener lastRecoveryListener;
        volatile ShardId lastCreatedShardId;

        @Override
        protected boolean autoCompleteRecovery() {
            // This test drives recovery completion/failure manually via lastRecoveryListener/failRecoveryViaListener.
            return false;
        }

        @Override
        public MockIndexService indexService(Index index) {
            RuntimeException toFail = failNextIndexServiceLookup;
            if (toFail != null) {
                failNextIndexServiceLookup = null;
                throw toFail;
            }
            return super.indexService(index);
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
            RuntimeException toFail = failNextCreateShard;
            if (toFail != null) {
                failNextCreateShard = null;
                throw toFail;
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
        }

        /// Drives the production path via [RecoveryListener#onRecoveryFailure] rather than calling
        /// [IndicesClusterStateService#handleRecoveryFailure] directly.
        void failRecoveryViaListener(FailureStrategy failureStrategy) {
            assertNotNull("createShard must have captured a RecoveryListener", lastRecoveryListener);
            MockIndexShard shard = getShardOrNull(lastCreatedShardId);
            assertNotNull(shard);
            RecoveryState recoveryState = shard.recoveryState();
            lastRecoveryListener.onRecoveryFailure(
                recoveryState,
                new RecoveryFailedException(recoveryState, "simulated", new Exception("simulated")),
                failureStrategy
            );
        }

        /// Completes the currently-captured [RecoveryListener] successfully via
        /// [RecoveryListener#onRecoveryDone], so tests can verify the InFlight ticket is released
        /// once recovery actually finishes (it's deliberately held through recovery, not just construction
        /// -- see [#autoCompleteRecovery]).
        void completeRecoveryViaListener() {
            assertNotNull("createShard must have captured a RecoveryListener", lastRecoveryListener);
            MockIndexShard shard = getShardOrNull(lastCreatedShardId);
            assertNotNull(shard);
            lastRecoveryListener.onRecoveryDone(shard.recoveryState(), ShardLongFieldRange.UNKNOWN, ShardLongFieldRange.UNKNOWN);
        }
    }
}
