/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.indices.recovery.RecoveryListener.FailureStrategy.FAIL_SEND;
import static org.elasticsearch.indices.recovery.RecoveryListener.FailureStrategy.FAIL_SILENT;

/// Limit the number of concurrent recoveries. Slots are filled when dispatching a recovery task to the executor and
/// released when the recovery's [RecoveryListener] completes.
/// The max number of concurrent recovery slots is controlled by the [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING]
/// dynamic setting.
///
/// Dispatch is also subject to the node's recovery gates: while they block, no queued recovery is dispatched, and [#doFillSlots]
/// registers a listener with the [RecoveryGateMonitor] so dispatch resumes as soon as they allow recoveries again.
public final class ThrottlingRecoveryService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ThrottlingRecoveryService.class);

    /// Controls the max number of concurrent recoveries allowed on this data node. Excludes peer recoveries for which this
    /// node is the source, see [PeerRecoverySourceService#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING]. Includes both
    /// recoveries of unassigned shards and relocations. See also [#INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING] which
    /// imposes an additional throttle on relocations only.
    ///
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_recoveries",
        // Throttling handled by master allocation for now.
        Integer.MAX_VALUE,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /// Controls the max number of concurrent _relocation_ recoveries allowed on this data node. Excludes peer recoveries for which this
    /// node is the source, see [PeerRecoverySourceService#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING]. Includes both
    /// recoveries of unassigned shards and relocations.
    ///
    /// If this is set to a value less than [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING] then:
    /// - The total number of slots will be [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING] slots.
    /// - Recoveries from unassigned shards can use any of those slots.
    /// - Relocations can only use a subset of those slots given by [#INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING].
    /// - Therefore, there will be a number of slots given by [#INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING] which can be
    /// used by either type of recovery...
    /// - ...while there will be an additional number of slots given by the difference between the two settings that can only be used by
    /// recoveries from unassigned shards.
    ///
    /// If this is set to a value equal to or greater than [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING] then this setting has no
    /// effect.
    ///
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_relocation_recoveries",
        // Throttling handled by master allocation for now.
        Integer.MAX_VALUE,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Executor executor;
    private final ThreadContext threadContext;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;
    private final ClusterService clusterService;
    private final RecoverySchedulingListener schedulingListener;
    private final RecoveryGateMonitor recoveryGateMonitor;
    private final AtomicReference<BlockedState> blockedState = new AtomicReference<>();

    private final RecoveriesThrottle recoveriesThrottle = new RecoveriesThrottle();

    private static final Comparator<PendingRecovery> RECOVERY_ORDERING =
        // Order first by the recovery priority in the recovery state, then by using PriorityComparator on the index metadata:
        // (If there are multiple queue entries with the same recovery priority for the same index, execution order will be arbirary.)
        Comparator.<PendingRecovery, Integer>comparing(recovery -> recovery.recoveryState().getRecoveryPriority().ordinal())
            .thenComparing(PendingRecovery::indexMetadata, PriorityComparator.getIndexMetadataComparator());
    private final PriorityQueue<PendingRecovery> pendingRecoveries = new PriorityQueue<>(RECOVERY_ORDERING);

    /// Records allocation IDs that have been directly cancelled by the master, including those for recoveries that have
    /// already started (i.e. are not in [#pendingRecoveries]).
    /// Entries are pruned by [#clusterChanged] once the corresponding shard stops initializing or its allocationId changes.
    private final Map<String, ShardId> cancelledAllocationIds = new HashMap<>();

    public ThrottlingRecoveryService(
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        ClusterService clusterService,
        RecoverySchedulingListener schedulingListener,
        RecoveryGateMonitor recoveryGateMonitor
    ) {
        this.executor = threadPool.generic();
        this.threadContext = threadPool.getThreadContext();
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.schedulingListener = schedulingListener;
        this.clusterService = clusterService;
        this.recoveryGateMonitor = recoveryGateMonitor;
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(
                INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING,
                this::setMaxConcurrentRelocationRecoveries
            );
    }

    /// Enqueues a recovery task and/or dispatches it to the executor if there are any available slots.
    public void enqueue(
        ProjectId projectId,
        RecoveryListener recoveryListener,
        RecoveryState recoveryState,
        IndexMetadata indexMetadata,
        String allocationId,
        RecoveryStats stats,
        Consumer<RecoveryListener> task
    ) {
        final Supplier<ThreadContext.StoredContext> context = restorableContextForProject(projectId);
        final ShardId shardId = recoveryState.getShardId();
        final PendingRecovery pendingRecovery;
        final boolean serviceClosed;
        synchronized (this) {
            serviceClosed = isClosed();
            if (serviceClosed || cancelledAllocationIds.containsKey(allocationId)) {
                final ShardId cancelled = cancelledAllocationIds.get(allocationId);
                assert serviceClosed || cancelled.equals(shardId)
                    : "mismatch between cached cancellation [" + cancelled + "] and enqueue recovery: [" + recoveryState + "]";
                pendingRecovery = null;
            } else {
                pendingRecovery = new PendingRecovery(recoveryState, indexMetadata, allocationId, stats, task, recoveryListener, context);
                // Note that the PendingRecovery captures the IndexMetadata that was passed in when the recovery was enqueued, so it does
                // not respond to changes in index.priority and reorder the queue. If we wanted that, we would need to maintain a collection
                // of listeners (see IndexService.addMetadataListener) which are mapped to the queued entries, and remove and re-add them.
                pendingRecoveries.add(pendingRecovery);
                stats.targetRecoveryQueued(recoveryState.getRecoverySource().getType());
            }
        }
        if (pendingRecovery == null) {
            if (serviceClosed) {
                logger.debug("service is closed, aborting recovery: {}", recoveryState);
                RecoveryListener.wrapPreservingContext(recoveryListener, context).onRecoveryAborted();
            } else {
                logger.debug("recovery cancelled at enqueue time: {}", recoveryState);
                final RecoverySource.Type recoveryType = recoveryState.getRecoverySource().getType();
                // Get off the cluster applier thread. Generic executor has unbounded queue and thread shutdown happens
                // after service close so this runnable should never get rejected.
                executor.execute(() -> {
                    RecoveryListener.wrapPreservingContext(recoveryListener, context)
                        .onRecoveryFailure(
                            new RecoveryCancelledException(
                                recoveryState.getShardId(),
                                recoveryState.getSourceNode(),
                                recoveryState.getTargetNode()
                            ),
                            FAIL_SEND
                        );
                    schedulingListener.onRecoveryCancelledBeforeQueuingOnTarget(recoveryType);
                });
            }
            return;
        }
        logger.trace("enqueued recovery: {}", recoveryState);
        schedulingListener.onRecoveryQueuedOnTarget(recoveryState.getRecoverySource().getType(), pendingRecovery.priorityGroup());
        fillSlots();
    }

    /// Cancels recoveries matching the provided allocation ID batch.
    ///
    /// For each allocation ID, pre-emptively records the cancellation so that a future [#enqueue] call will reject it.
    /// Any matching entries already in the pending queue are immediately notified via `onRecoveryFailure`
    /// (with `sendShardFailure=false`, since the master is informed through the action response).
    ///
    /// Returns the set of allocation IDs that were found and removed from the pending queue.
    public Set<String> cancelRecoveries(Map<String, ShardId> cancellations) {
        final List<PendingRecovery> recoveriesToCancel = new ArrayList<>();
        synchronized (this) {
            if (isClosed()) {
                return Set.of();
            }
            // Record every cancellation, even for recoveries that have already started (i.e. are not in the pending queue).
            // Pruned by clusterChanged once the shard stops initializing or its allocation ID changes.
            cancelledAllocationIds.putAll(cancellations);
            final Iterator<PendingRecovery> it = pendingRecoveries.iterator();
            while (it.hasNext()) {
                final PendingRecovery candidate = it.next();
                if (cancellations.containsKey(candidate.allocationId())) {
                    assert cancellations.get(candidate.allocationId()).equals(candidate.recoveryState().getShardId());
                    it.remove();
                    recoveriesToCancel.add(candidate);
                    candidate.stats().targetQueuedRecoveryDiscarded(candidate.recoveryState().getRecoverySource().getType());
                }
            }
        }
        final Set<String> cancelledInQueue = new HashSet<>(recoveriesToCancel.size());
        for (PendingRecovery pendingRecovery : recoveriesToCancel) {
            final RecoveryState state = pendingRecovery.recoveryState();

            logger.trace("cancelling recovery in queue: {}", state);
            RecoveryListener.wrapPreservingContext(pendingRecovery.listener, pendingRecovery.context)
                .onRecoveryFailure(
                    new RecoveryCancelledException(state.getShardId(), state.getSourceNode(), state.getTargetNode()),
                    FAIL_SILENT
                );
            schedulingListener.onQueuedRecoveryCancelledOnTarget(state.getRecoverySource().getType(), pendingRecovery.priorityGroup());
            cancelledInQueue.add(pendingRecovery.allocationId());
        }
        return cancelledInQueue;
    }

    /// Prunes queued recoveries and remembered direct-cancellation requests that have been rendered stale by this
    /// cluster state update, i.e. the shard is no longer assigned to this node under the allocation ID it was
    /// queued or cancelled for (it was reallocated elsewhere, unassigned, or this node has left the cluster entirely).
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final RoutingNode localNode = event.state().getRoutingNodes().node(clusterService.localNode().getId());
        final List<PendingRecovery> staleRecoveries = new ArrayList<>();
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            if (localNode == null) {
                assert clusterService.localNode().canContainData() == false
                    && pendingRecoveries.isEmpty()
                    && cancelledAllocationIds.isEmpty()
                    : "this node received the cluster state update so it's either a data node and its RoutingNode "
                        + "entry must be non-null or it's not a data node and it should not have any recoveries";
                return;
            }
            cancelledAllocationIds.entrySet()
                .removeIf((cancellation) -> allocationIdIsOutdated(localNode, cancellation.getValue(), cancellation.getKey()));
            final Iterator<PendingRecovery> it = pendingRecoveries.iterator();
            while (it.hasNext()) {
                final PendingRecovery pending = it.next();
                final RecoveryState recoveryState = pending.recoveryState();
                if (allocationIdIsOutdated(localNode, recoveryState.getShardId(), pending.allocationId())) {
                    it.remove();
                    staleRecoveries.add(pending);
                    // Note that updating RecoveryStats is not strictly necessary here and just done out of completeness sake +
                    // easier testing. Indeed, a pending recovery never started, and if its allocation ID has changed or localNode
                    // became `null`, the old IndexShard object those stats belong to would have already been closed.
                    pending.stats().targetQueuedRecoveryDiscarded(pending.recoveryState().getRecoverySource().getType());
                }
            }
        }
        for (PendingRecovery stale : staleRecoveries) {
            final RecoveryState state = stale.recoveryState();
            // Get off the cluster applier thread. Generic executor has unbounded queue and thread shutdown happens
            // after service close so this runnable should never get rejected.
            logger.debug("cancelling stale queued recovery {}", state);
            executor.execute(() -> {
                stale.listener()
                    .onRecoveryFailure(
                        new RecoveryCancelledException(state.getShardId(), state.getSourceNode(), state.getTargetNode()),
                        FAIL_SILENT
                    );
                schedulingListener.onQueuedRecoveryDiscardedOnTarget(state.getRecoverySource().getType(), stale.priorityGroup());
            });
        }
    }

    /// Returns true if the given shard is not recorded under the provided `allocationId` on this node.
    private static boolean allocationIdIsOutdated(RoutingNode node, ShardId shardId, String allocationId) {
        assert node != null;
        final ShardRouting routing = node.getByShardId(shardId);
        return routing == null || routing.initializing() == false || routing.allocationId().getId().equals(allocationId) == false;
    }

    // visible for testing
    synchronized int currentQueueSize() {
        return pendingRecoveries.size();
    }

    @Override
    protected void doStop() {
        assert isClosed(); // state change happens-before this line: all recoveries are discarded here or rejected during enqueue, no leaks
        final List<PendingRecovery> recoveriesToAbort;
        synchronized (this) {
            recoveriesToAbort = new ArrayList<>(pendingRecoveries);
            pendingRecoveries.clear();
            cancelledAllocationIds.clear();
            for (PendingRecovery pending : recoveriesToAbort) {
                pending.stats().targetQueuedRecoveryDiscarded(pending.recoveryState().getRecoverySource().getType());
            }
        }
        for (PendingRecovery pending : recoveriesToAbort) {
            logger.trace("service closing, aborting recovery: {}", pending.recoveryState());
            RecoveryListener.wrapPreservingContext(pending.listener, pending.context).onRecoveryAborted();
            schedulingListener.onQueuedRecoveryDiscardedOnTarget(
                pending.recoveryState().getRecoverySource().getType(),
                pending.priorityGroup()
            );
        }
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {}

    /// Is the service closed, and therefore rejecting further recoveries? It closes in a single step (there's no separate `stop()` call
    /// first) so we count both [org.elasticsearch.common.component.Lifecycle.State#STOPPED] and
    /// [org.elasticsearch.common.component.Lifecycle.State#CLOSED] as "closed".
    private boolean isClosed() {
        assert lifecycle.initialized() == false : "service accessed before start";
        return lifecycle.stoppedOrClosed();
    }

    private boolean isBlocked() {
        return blockedState.get() != null;
    }

    /// Evaluates the recovery gates and drains the pending queue up to the max slot capacity, forking to the generic executor so
    /// dispatch is not run on the cluster state applier thread. Called on every enqueue, slot release and recovery gate callback.
    private void fillSlots() {
        if (isBlocked()) {
            return;
        }
        // generic thread pool is unbounded and does not reject
        executor.execute(this::doFillSlots);
    }

    private void doFillSlots() {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        if (isBlocked()) {
            return;
        }
        final RecoveryGate.Decision decision = recoveryGateMonitor.evaluate();
        if (decision.mayRun() == false) {
            onRecoveriesBlocked(decision);
            return;
        }
        final List<PendingRecovery> recoveriesToDispatch = new ArrayList<>();
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            // Pull pending recoveries from the priority queue up for as long as the RecoveriesThrottle will allow us to. Note that we just
            // peek at the item at the head of the queue here. This relies on the connection of these two facts:
            // 1. Recoveries from unassigned shards will always appear in the queue ahead of relocations;
            // 2. We will never throttle unassigned recoveries more tightly than relocations.
            // This means that the recoveriesThrottle.shouldStartNextPendingRecovery() will never return false for the item at the head of
            // the queue when it might have returned true for an item lower down the queue.
            while (pendingRecoveries.isEmpty() == false && recoveriesThrottle.shouldStartNextPendingRecovery(pendingRecoveries.peek())) {
                final PendingRecovery recovery = pendingRecoveries.poll();
                assert recovery != null;
                recoveriesToDispatch.add(recovery);
                recoveriesThrottle.incrementRunning(recovery);
                recovery.stats().targetRecoveryDequeuedAndStarted(recovery.recoveryState().getRecoverySource().getType());
            }
            // Assert on the postcondition described above:
            assert pendingRecoveries.stream().noneMatch(recoveriesThrottle::shouldStartNextPendingRecovery)
                : Strings.format(
                    """
                        The recovery at the head of the queue was throttled, but at least one recovery elsewhere in the queue would not have
                        been. This violates the expectation that we will never have a category of recovery which is prioritized more highly
                        but throttled more tightly than another. Highest priority recovery: %s. Non-throttled recoveries: %s.
                        """,
                    pendingRecoveries.peek(),
                    pendingRecoveries.stream().filter(recoveriesThrottle::shouldStartNextPendingRecovery).toList()
                );
        }
        for (PendingRecovery recovery : recoveriesToDispatch) {
            final RecoveryListener wrapped = wrapListenerForExecution(recovery.listener, recovery);
            try (var ignored = recovery.context.get()) {
                executor.execute(new RecoveryRunnable(recovery, wrapped));
            }
            logger.trace("dispatched recovery: {}", recovery.recoveryState());
            schedulingListener.onRecoveryDequeuedAndStartedOnTarget(
                recovery.recoveryState().getRecoverySource().getType(),
                recovery.priorityGroup()
            );
        }
    }

    /// Handles a blocked decision observed by [#fillSlots]: a transition into blocked state reports the block and registers a
    /// callback that resumes dispatch once the gates allow recoveries again.
    private void onRecoveriesBlocked(RecoveryGate.Decision decision) {
        assert decision.mayRun() == false;
        if (isClosed()) {
            return;
        }
        if (blockedState.compareAndSet(null, new BlockedState(decision.gateName(), threadPool.relativeTimeInMillis()))) {
            logger.info("recovery dispatch blocked by gate [{}]: {}", decision.gateName(), decision.reason());
            try {
                schedulingListener.onRecoveriesBlocked(decision.gateName());
            } finally {
                recoveryGateMonitor.addCallback(RecoveryGate.Outcome.RUN, this::onRecoveriesUnblocked);
            }
        } else {
            logger.debug("recovery dispatch still blocked by gate [{}]: {}", decision.gateName(), decision.reason());
        }
    }

    /// Fired by the [RecoveryGateMonitor] once the gates allow recoveries again: reports how long dispatch was held and resumes it.
    private void onRecoveriesUnblocked() {
        if (isClosed()) {
            return;
        }
        final BlockedState state = blockedState.get();
        assert state != null : "resume callback fired without a recorded block";
        try {
            final long blockedTimeMillis = threadPool.relativeTimeInMillis() - state.sinceMillis();
            logger.info(
                "resuming recoveries held for [{}] (initially blocked by gate [{}])",
                TimeValue.timeValueMillis(blockedTimeMillis),
                state.gateName()
            );
            schedulingListener.onRecoveriesUnblocked(blockedTimeMillis);
        } finally {
            blockedState.set(null);
            // no need to fork: the recovery gate monitor fires callbacks on the generic pool already
            doFillSlots();
        }
    }

    private RecoveryListener wrapListenerForExecution(RecoveryListener listener, PendingRecovery recovery) {
        final RecoverySource.Type recoveryType = recovery.recoveryState().getRecoverySource().getType();

        final RecoveryListener handleCancellation = RecoveryListener.runBeforeFailure(listener, e -> {
            if (ExceptionsHelper.unwrap(e, RecoveryCancelledException.class) != null) {
                schedulingListener.onStartedRecoveryCancelledOnTarget(recoveryType);
            }
        });

        final RecoveryListener releaseSlot = RecoveryListener.runAfter(handleCancellation, () -> releaseSlot(recovery));
        return RecoveryListener.wrapPreservingContext(releaseSlot, recovery.context);
    }

    private void releaseSlot(PendingRecovery recovery) {
        final RecoverySource source = recovery.recoveryState().getRecoverySource();
        synchronized (this) {
            recoveriesThrottle.decrementRunning(recovery);
            recovery.stats().targetRecoveryCompleted(source.getType());
        }
        logger.trace("recovery slot released: {}", recovery.recoveryState());
        schedulingListener.onRecoveryCompletedOnTarget(source.getType(), recovery.priorityGroup());
        fillSlots();
    }

    private void setMaxConcurrentRecoveries(int newMaxConcurrentRecoveries) {
        final int previousLimit;
        synchronized (this) {
            previousLimit = recoveriesThrottle.maxConcurrentRecoveries;
            recoveriesThrottle.maxConcurrentRecoveries = newMaxConcurrentRecoveries;
        }
        if (previousLimit < newMaxConcurrentRecoveries && lifecycle.started() /* calls before start can (must) be ignored */) {
            fillSlots();
        }
    }

    private void setMaxConcurrentRelocationRecoveries(int newMaxConcurrentRelocationRecoveries) {
        final int previousLimit;
        synchronized (this) {
            previousLimit = recoveriesThrottle.maxConcurrentRelocationRecoveries;
            recoveriesThrottle.maxConcurrentRelocationRecoveries = newMaxConcurrentRelocationRecoveries;
        }
        if (previousLimit < newMaxConcurrentRelocationRecoveries && lifecycle.started() /* calls before start can (must) be ignored */) {
            fillSlots();
        }
    }

    private Supplier<ThreadContext.StoredContext> restorableContextForProject(ProjectId projectId) {
        final var context = new AtomicReference<ThreadContext.StoredContext>();
        projectResolver.executeOnProject(projectId, () -> context.set(threadContext.newStoredContext()));
        return threadContext.wrapRestorable(context.get());
    }

    /// Metadata holder for a recovery that has been enqueued but not yet dispatched.
    /// The `listener` is the one passed in to [#enqueue] by indicesServices. Slot-release and other wrappers are added
    /// at dispatch time, such that aborting a queued-but-never-dispatched task does not decrement a slot that was never taken.
    private record PendingRecovery(
        RecoveryState recoveryState,
        IndexMetadata indexMetadata,
        String allocationId,
        RecoveryStats stats,
        Consumer<RecoveryListener> task,
        RecoveryListener listener,
        Supplier<ThreadContext.StoredContext> context
    ) {

        boolean isUnassigned() {
            return switch (recoveryState.getRecoveryPriority()) {
                case UNASSIGNED_NEW_PRIMARY, UNASSIGNED_UNEXPECTED, UNASSIGNED_EXPECTED -> true;
                case RELOCATION_CAN_REMAIN_NO, RELOCATION_CAN_REMAIN_NOT_PREFERRED, RELOCATE_REBALANCING -> false;
                case UNKNOWN -> {
                    assert false : "should never see RecoveryState with UNKNOWN priority in cluster state: " + recoveryState;
                    yield false; // fall back to false, as we treat this as the lowest priority, so it is ordered more like a relocation
                }
            };
        }

        RecoverySchedulingListener.PriorityGroup priorityGroup() {
            return isUnassigned()
                ? RecoverySchedulingListener.PriorityGroup.UNASSIGNED
                : RecoverySchedulingListener.PriorityGroup.RELOCATION;
        }
    }

    /// Helper class which manages throttling the number of running recoveries.
    private static class RecoveriesThrottle {

        /// The maximum number of concurrent recoveries, including recoveries from unassigned + relocations. See
        /// [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING].
        private int maxConcurrentRecoveries;
        /// The maximum number of concurrent relocation recoveries. See [#INDICES_RECOVERY_MAX_CONCURRENT_RELOCATION_RECOVERIES_SETTING].
        private int maxConcurrentRelocationRecoveries;
        /// The number of concurrent recoveries currently running, including recoveries from unassigned + relocations. Must not exceed
        /// [#maxConcurrentRecoveries].
        private int runningRecoveries = 0;
        /// The number of concurrent relocation recoveries currently running. Must not exceed [#maxConcurrentRelocationRecoveries].
        private int runningRelocationRecoveries = 0;

        void incrementRunning(PendingRecovery recoveryNowRunning) {
            runningRecoveries++;
            if (!recoveryNowRunning.isUnassigned()) {
                runningRelocationRecoveries++;
            }
        }

        void decrementRunning(PendingRecovery recoveryNowFinished) {
            runningRecoveries--;
            assert runningRecoveries >= 0 : "negative number of running unassigned recoveries " + runningRecoveries;
            if (!recoveryNowFinished.isUnassigned()) {
                runningRelocationRecoveries--;
                assert runningRelocationRecoveries >= 0 : "negative number of running relocation recoveries " + runningRelocationRecoveries;
            }
        }

        boolean shouldStartNextPendingRecovery(PendingRecovery nextPendingRecovery) {
            return runningRecoveries < maxConcurrentRecoveries
                && (nextPendingRecovery.isUnassigned() || (runningRelocationRecoveries < maxConcurrentRelocationRecoveries));
        }
    }

    /// Executable wrapper for a dispatched recovery. The provided recovery listener (from [PendingRecovery]) is wrapped
    /// with `assertOnce` (to ensure there is only one terminal callback).
    private static class RecoveryRunnable extends AbstractRunnable {
        private final RecoveryState recoveryState;
        private final Consumer<RecoveryListener> task;
        private final RecoveryListener listener;

        private RecoveryRunnable(PendingRecovery pending, RecoveryListener listener) {
            this.recoveryState = pending.recoveryState;
            this.task = pending.task;
            this.listener = RecoveryListener.assertOnce(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onRecoveryFailure(new RecoveryFailedException(recoveryState, null, e), FAIL_SEND);
        }

        @Override
        protected void doRun() {
            task.accept(listener);
        }
    }

    private record BlockedState(String gateName, long sinceMillis) {}
}
