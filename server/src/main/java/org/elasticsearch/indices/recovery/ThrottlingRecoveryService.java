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
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.IncomingRecoveryLimits;
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
import java.util.stream.Collectors;

import static org.elasticsearch.indices.recovery.FailureStrategy.ABORT;
import static org.elasticsearch.indices.recovery.FailureStrategy.FAIL_SEND;
import static org.elasticsearch.indices.recovery.FailureStrategy.FAIL_SILENT;

/// Limit the number of concurrent recoveries. Slots are filled when dispatching a recovery task to the executor and
/// released when the recovery's [RecoveryListener] completes. The max number of concurrent recovery slots is controlled
/// by the [DataNodeRecoveryThrottlingSettings#INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING] dynamic setting.
/// [DataNodeRecoveryThrottlingSettings#INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING] then
/// controls the max proportion of those concurrent recovery slots that may be used for non-critical recoveries (see
/// [PendingRecovery#isCritical()]), so that some slots are always kept free for critical ones.
///
/// Dispatch is also subject to the node's recovery gates: while they block, no queued recovery is dispatched, and [#doFillSlots]
/// registers a listener with the [RecoveryGateMonitor] so dispatch resumes as soon as they allow recoveries again.
public final class ThrottlingRecoveryService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ThrottlingRecoveryService.class);

    private final Executor executor;
    private final ThreadContext threadContext;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;
    private final ClusterService clusterService;
    private final RecoverySchedulingListener schedulingListener;
    private final RecoveryGateMonitor recoveryGateMonitor;
    private final ByteSizeValue maxHeap;
    private final AtomicReference<BlockedState> blockedState = new AtomicReference<>();

    private final RecoveriesThrottle recoveriesThrottle;

    private static final Comparator<PendingRecovery> RECOVERY_ORDERING =
        // Order first by the recovery priority in the recovery state, then by using PriorityComparator on the index metadata:
        // (If there are multiple queue entries with the same recovery priority for the same index, execution order will be arbitrary.)
        Comparator.<PendingRecovery, Integer>comparing(recovery -> recovery.recoveryState().getRecoveryPriority().ordinal())
            .thenComparing(PendingRecovery::indexMetadata, PriorityComparator.getIndexMetadataComparator());
    private final PriorityQueue<PendingRecovery> pendingRecoveries = new PriorityQueue<>(RECOVERY_ORDERING);

    // If non-null, this field caches the value for PendingRecovery.enqueuedTimeAbsoluteMillis() for any queued recovery, or Long.MAX_VALUE
    // if the queue is empty. If null, the value should be computed on demand and cached.
    // We use absoluteTimeInMillis rather than relativeTimeInMillis because the latter has an arbitrary zero point, so there's a chance
    // (though very small!) that it could wrap around while we're running, and then the minimum timestamp wouldn't be the earliest.
    // We deal with the (also very small) chance that we could observe time going backwards in queueLatencyMillis().
    private Long cachedEarliestEnqueuedTimeAbsoluteMillis;

    /// Records allocation IDs that have been directly cancelled by the master, including those for recoveries that have
    /// already started (i.e. are not in [#pendingRecoveries]).
    /// Entries are pruned by [#clusterChanged] once the corresponding shard stops initializing or its allocationId changes.
    private final Map<String, ShardId> cancelledAllocationIds = new HashMap<>();

    public ThrottlingRecoveryService(
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        ClusterService clusterService,
        RecoverySchedulingListener schedulingListener,
        RecoveryGateMonitor recoveryGateMonitor,
        ByteSizeValue maxHeap
    ) {
        this.executor = threadPool.generic();
        this.threadContext = threadPool.getThreadContext();
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
        this.schedulingListener = schedulingListener;
        this.clusterService = clusterService;
        this.recoveryGateMonitor = recoveryGateMonitor;
        this.maxHeap = maxHeap;
        this.recoveriesThrottle = new RecoveriesThrottle();
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
        DataNodeRecoveryThrottlingSettings.watchIncomingRecoveryLimits(
            clusterService.getClusterSettings(),
            clusterService.getSettings(),
            maxHeap,
            this::applyIncomingRecoveryLimits
        );
    }

    /// Enqueues a recovery task and/or dispatches it to the executor if there are any available slots.
    public void enqueue(
        ProjectId projectId,
        RecoveryListener recoveryListener,
        IndexShard indexShard,
        IndexMetadata indexMetadata,
        Consumer<RecoveryListener> task
    ) {
        final Supplier<ThreadContext.StoredContext> context = restorableContextForProject(projectId);
        final ShardId shardId = indexShard.shardId();
        final AllocationId routingAllocation = indexShard.routingEntry().allocationId();
        assert routingAllocation != null : "Initializing shard missing allocation " + indexShard.routingEntry();
        final String allocationId = routingAllocation.getId();
        final RecoverySource recoverySource = indexShard.routingEntry().recoverySource();
        assert recoverySource != null : "Initializing shard missing recovery source " + indexShard.routingEntry();
        final PendingRecovery pendingRecovery;
        final boolean serviceClosed;
        synchronized (this) {
            serviceClosed = isClosed();
            if (serviceClosed || cancelledAllocationIds.containsKey(allocationId)) {
                final ShardId cancelled = cancelledAllocationIds.get(allocationId);
                assert serviceClosed || cancelled.equals(shardId)
                    : "mismatch between cached cancellation [" + cancelled + "] and enqueue recovery: [" + indexShard.recoveryState() + "]";
                pendingRecovery = null;
            } else {
                pendingRecovery = new PendingRecovery(
                    indexShard,
                    indexMetadata,
                    task,
                    recoveryListener,
                    context,
                    threadPool.absoluteTimeInMillis()
                );
                // Note that the PendingRecovery captures the IndexMetadata that was passed in when the recovery was enqueued, so it does
                // not respond to changes in index.priority and reorder the queue. If we wanted that, we would need to maintain a collection
                // of listeners (see IndexService.addMetadataListener) which are mapped to the queued entries, and remove and re-add them.
                pendingRecoveries.add(pendingRecovery);
                onEnqueued(pendingRecovery);
                indexShard.recoveryStats().targetRecoveryQueued(recoverySource.getType());
            }
        }
        if (pendingRecovery == null) {
            if (serviceClosed) {
                logger.debug("service is closed, aborting recovery: {}", indexShard.recoveryState());
                RecoveryListener.wrapPreservingContext(recoveryListener, context)
                    .onRecoveryFailure(
                        indexShard.recoveryState(),
                        new RecoveryFailedException(indexShard.recoveryState(), "service is closed", null),
                        ABORT
                    );
            } else {
                logger.debug("recovery cancelled at enqueue time: {}", indexShard.recoveryState());
                final RecoverySource.Type recoveryType = recoverySource.getType();
                // Get off the cluster applier thread. Generic executor has unbounded queue and thread shutdown happens
                // after service close so this runnable should never get rejected.
                executor.execute(() -> {
                    RecoveryListener.wrapPreservingContext(recoveryListener, context)
                        .onRecoveryFailure(
                            indexShard.recoveryState(),
                            new RecoveryCancelledException(
                                indexShard.shardId(),
                                indexShard.recoveryState().getSourceNode(),
                                indexShard.recoveryState().getTargetNode()
                            ),
                            FAIL_SEND
                        );
                    schedulingListener.onRecoveryCancelledBeforeQueuingOnTarget(recoveryType);
                });
            }
            return;
        }
        logger.trace("enqueued recovery: {}", indexShard.recoveryState());
        schedulingListener.onRecoveryQueuedOnTarget(recoverySource.getType(), pendingRecovery.priorityGroup());
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
                    onDequeued(candidate);
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
                    state,
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
                    onDequeued(pending);
                    it.remove();
                    staleRecoveries.add(pending);
                    // Note that updating RecoveryStats is not strictly necessary here and just done out of completeness sake +
                    // easier testing. Indeed, a pending recovery never started, and if its allocation ID has changed or localNode
                    // became `null`, the old IndexShard object those stats belong to would have already been closed.
                    pending.stats().targetQueuedRecoveryDiscarded(recoveryState.getRecoverySource().getType());
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
                        state,
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

    /// Returns the allocation IDs of recoveries currently waiting in this node's queue.
    public synchronized Set<String> queuedAllocationIds() {
        return pendingRecoveries.stream().map(PendingRecovery::allocationId).collect(Collectors.toUnmodifiableSet());
    }

    /// Returns the current blocked state, or `null` if recovery dispatch is not blocked.
    public @Nullable BlockedState blockedState() {
        return blockedState.get();
    }

    /// Returns the longest time any recovery has been queued on this node, in milliseconds. If the queue is empty, returns zero.
    public long queueLatencyMillis() {
        return Math.max(threadPool.absoluteTimeInMillis() - earliestEnqueuedTimeAbsoluteMillis(), 0L);
    }

    private synchronized long earliestEnqueuedTimeAbsoluteMillis() {
        if (cachedEarliestEnqueuedTimeAbsoluteMillis == null) {
            cachedEarliestEnqueuedTimeAbsoluteMillis = pendingRecoveries.stream()
                .mapToLong(PendingRecovery::enqueuedTimeAbsoluteMillis)
                .min()
                .orElse(Long.MAX_VALUE); // MAX_VALUE ensures that queueLatencyMillis() returns zero
        }
        return cachedEarliestEnqueuedTimeAbsoluteMillis;
    }

    private void onEnqueued(PendingRecovery enqueued) {
        // If we have cached the earliest enqueued time, update it if necessary. (If it is not cached, it will be computed on demand.)
        if (cachedEarliestEnqueuedTimeAbsoluteMillis != null) {
            cachedEarliestEnqueuedTimeAbsoluteMillis = Math.min(
                cachedEarliestEnqueuedTimeAbsoluteMillis,
                enqueued.enqueuedTimeAbsoluteMillis()
            );
        }
    }

    private void onDequeued(PendingRecovery dequeued) {
        // If this recovery matches the cached earliest enqueued time, invalidate the cache (it will be computed the next time it's needed).
        if (cachedEarliestEnqueuedTimeAbsoluteMillis != null
            && dequeued.enqueuedTimeAbsoluteMillis() == cachedEarliestEnqueuedTimeAbsoluteMillis) {
            cachedEarliestEnqueuedTimeAbsoluteMillis = null;
        }
    }

    @Override
    protected void doStop() {
        assert isClosed(); // state change happens-before this line: all recoveries are discarded here or rejected during enqueue, no leaks
        final List<PendingRecovery> recoveriesToAbort;
        synchronized (this) {
            recoveriesToAbort = new ArrayList<>(pendingRecoveries);
            cachedEarliestEnqueuedTimeAbsoluteMillis = null;
            pendingRecoveries.clear();
            cancelledAllocationIds.clear();
            for (PendingRecovery pending : recoveriesToAbort) {
                pending.stats().targetQueuedRecoveryDiscarded(pending.recoveryState().getRecoverySource().getType());
            }
        }
        for (PendingRecovery pending : recoveriesToAbort) {
            logger.trace("service closing, aborting recovery: {}", pending.recoveryState());
            RecoveryListener.wrapPreservingContext(pending.listener, pending.context)
                .onRecoveryFailure(
                    pending.recoveryState(),
                    new RecoveryFailedException(pending.recoveryState(), "service closing", null),
                    ABORT
                );
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
            // Pull pending recoveries from the priority queue up for as long as the RecoveriesThrottle will allow us to.
            // Note that we just peek at the item at the head of the queue here. This relies on these two facts:
            // 1. Critical recoveries will always appear in the queue ahead of non-critical ones
            // 2. We will never throttle critical recoveries more tightly than non-critical ones.
            // This means that the recoveriesThrottle.shouldStartNextPendingRecovery() will never return false for the
            // item at the head of the queue when it might have returned true for an item lower down the queue.
            while (pendingRecoveries.isEmpty() == false && recoveriesThrottle.shouldStartNextPendingRecovery(pendingRecoveries.peek())) {
                final PendingRecovery recovery = pendingRecoveries.poll();
                assert recovery != null;
                onDequeued(recovery);
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
            final long blockedTimeMillis = threadPool.relativeTimeInMillis() - state.sinceRelativeMillis();
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
        final RecoveryListener handleCancellation = RecoveryListener.runBeforeFailure(listener, e -> {
            if (ExceptionsHelper.unwrap(e, RecoveryCancelledException.class) != null) {
                final RecoveryState recoveryState = recovery.recoveryState();
                schedulingListener.onStartedRecoveryCancelledOnTarget(
                    recoveryState.getRecoverySource().getType(),
                    recoveryState.getStage(),
                    recoveryState.getTimer().time()
                );
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

    private void applyIncomingRecoveryLimits(IncomingRecoveryLimits limits) {
        synchronized (this) {
            recoveriesThrottle.limits = limits;
        }
        if (lifecycle.started() /* calls before start can (must) be ignored */) {
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
        IndexShard indexShard,
        IndexMetadata indexMetadata,
        Consumer<RecoveryListener> task,
        RecoveryListener listener,
        Supplier<ThreadContext.StoredContext> context,
        long enqueuedTimeAbsoluteMillis
    ) {

        /// Returns the [RecoveryState] for the shard to be recovered.
        ///
        /// Note that, as long as [IndexShard#recoveryState()] returns a non-final field (and [RecoveryState#reset()] returns a new
        /// instance) repeated calls to this method might return different instances. Therefore, callers should not hang onto the value for
        /// a long time if they need a fresh value.
        RecoveryState recoveryState() {
            return indexShard.recoveryState();
        }

        RecoveryStats stats() {
            return indexShard.recoveryStats();
        }

        String allocationId() {
            AllocationId routingAllocation = indexShard.routingEntry().allocationId();
            assert routingAllocation != null : "Initializing shard missing allocation " + indexShard.routingEntry();
            return routingAllocation.getId();
        }

        /// Returns whether this is a critical recovery, i.e. one whose delay may cause additional unavailability. Relocations,
        /// recoveries of unassigned shards added for performance reasons, and snapshot restores (whose index is expected to be
        /// unavailable until they complete) are non-critical. Non-critical recoveries are additionally limited by
        /// [DataNodeRecoveryThrottlingSettings#INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING], so
        /// that the node can always immediately start work on at least one new critical recovery.
        ///
        /// All the critical priorities rank above all the non-critical ones in [ShardRouting.RecoveryPriority], which
        /// [#doFillSlots] relies on.
        boolean isCritical() {
            RecoveryState state = recoveryState();
            return switch (state.getRecoveryPriority()) {
                case UNASSIGNED_NEW_PRIMARY, UNASSIGNED_UNEXPECTED, UNASSIGNED_EXPECTED -> true;
                case RELOCATION_CAN_REMAIN_NO, UNASSIGNED_RESTORING, UNASSIGNED_PERFORMANCE_SCALING, RELOCATION_CAN_REMAIN_NOT_PREFERRED,
                    RELOCATE_REBALANCING -> false;
                case UNKNOWN -> {
                    assert false : "should never see RecoveryState with UNKNOWN priority in cluster state: " + state;
                    yield false; // fall back to false, as we treat this as the lowest priority, so it is ordered more like a relocation
                }
            };
        }

        RecoverySchedulingListener.PriorityGroup priorityGroup() {
            return isCritical() ? RecoverySchedulingListener.PriorityGroup.CRITICAL : RecoverySchedulingListener.PriorityGroup.NON_CRITICAL;
        }
    }

    /// Helper class which manages throttling the number of running recoveries.
    /// Not thread-safe. All access must be performed while holding the enclosing [ThrottlingRecoveryService] instance lock.
    private static class RecoveriesThrottle {

        /// The effective limits on running recoveries.
        private IncomingRecoveryLimits limits;

        /// The number of concurrent recoveries currently running, including critical and non-critical recoveries.
        private int runningRecoveries = 0;

        /// The number of concurrent non-critical recoveries currently running.
        private int runningNonCriticalRecoveries = 0;

        private void incrementRunning(PendingRecovery recoveryNowRunning) {
            runningRecoveries++;
            if (recoveryNowRunning.isCritical() == false) {
                runningNonCriticalRecoveries++;
            }
        }

        private void decrementRunning(PendingRecovery recoveryNowFinished) {
            runningRecoveries--;
            assert runningRecoveries >= 0 : "negative number of running unassigned recoveries " + runningRecoveries;
            if (recoveryNowFinished.isCritical() == false) {
                runningNonCriticalRecoveries--;
                assert runningNonCriticalRecoveries >= 0
                    : "negative number of running non-critical recoveries " + runningNonCriticalRecoveries;
            }
        }

        private boolean shouldStartNextPendingRecovery(PendingRecovery nextPendingRecovery) {
            assert limits != null : "recovery limits are set when the service starts";
            return runningRecoveries < limits.maxConcurrentRecoveries()
                && (nextPendingRecovery.isCritical() || (runningNonCriticalRecoveries < limits.maxConcurrentNonCriticalRecoveries()));
        }
    }

    /// Executable wrapper for a dispatched recovery. The provided recovery listener (from [PendingRecovery]) is wrapped
    /// with `assertOnce` (to ensure there is only one terminal callback).
    private static class RecoveryRunnable extends AbstractRunnable {
        private final IndexShard indexShard;
        private final Consumer<RecoveryListener> task;
        private final RecoveryListener listener;

        private RecoveryRunnable(PendingRecovery pending, RecoveryListener listener) {
            this.indexShard = pending.indexShard();
            this.task = pending.task;
            this.listener = RecoveryListener.assertOnce(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onRecoveryFailure(
                indexShard.recoveryState(),
                new RecoveryFailedException(indexShard.recoveryState(), null, e),
                FAIL_SEND
            );
        }

        @Override
        protected void doRun() {
            task.accept(listener);
        }
    }

    /// The recovery gate blocking dispatch
    ///
    /// @param gateName the name of the blocking gate
    /// @param sinceRelativeMillis the value of [ThreadPool#relativeTimeInMillis()] when blocking started
    public record BlockedState(String gateName, long sinceRelativeMillis) {}
}
