/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/// Limit the number of concurrent recoveries. Slots are filled when dispatching a recovery task to the executor and
/// released when the recovery's [RecoveryListener] completes.
/// The max number of concurrent recovery slots is controlled by the [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING]
/// dynamic setting.
public final class ThrottlingRecoveryService implements ClusterStateListener, Closeable {

    private static final Logger logger = LogManager.getLogger(ThrottlingRecoveryService.class);

    /// Controls the max number of concurrent recoveries allowed on this data node (excludes peer recoveries for which this
    /// node is the source, see [PeerRecoverySourceService#INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING]).
    ///
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_recoveries",
        // Throttling handled by master allocation for now.
        Integer.MAX_VALUE,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Executor executor;
    private final ThreadContext threadContext;
    private final ProjectResolver projectResolver;
    private final ClusterService clusterService;
    private final RecoverySchedulingListener schedulingListener;

    private int maxConcurrentRecoveries;
    private int runningRecoveries = 0;
    private final Deque<PendingRecovery> pendingRecoveries = new ArrayDeque<>();

    /// Records allocation IDs that have been directly cancelled by the master, including those for recoveries that have
    /// already started (i.e. are not in [#pendingRecoveries]).
    /// Entries are pruned by [#clusterChanged] once the corresponding shard stops initializing or its allocationId changes.
    private final Map<String, ShardId> cancelledAllocationIds = new HashMap<>();

    private boolean closed;

    public ThrottlingRecoveryService(
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        ClusterService clusterService,
        RecoverySchedulingListener schedulingListener
    ) {
        this.executor = threadPool.generic();
        this.threadContext = threadPool.getThreadContext();
        this.projectResolver = projectResolver;
        this.schedulingListener = schedulingListener;
        this.clusterService = clusterService;
        clusterService.addListener(this);
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    /// Enqueues a recovery task and/or dispatches it to the executor if there are any available slots.
    public void enqueue(
        ProjectId projectId,
        RecoveryListener recoveryListener,
        RecoveryState recoveryState,
        String allocationId,
        RecoveryStats stats,
        Consumer<RecoveryListener> task
    ) {
        final Supplier<ThreadContext.StoredContext> context = restorableContextForProject(projectId);
        final PendingRecovery pendingRecovery;
        final boolean serviceClosed;
        synchronized (this) {
            serviceClosed = closed;
            if (serviceClosed || cancelledAllocationIds.remove(allocationId) != null) {
                pendingRecovery = null;
            } else {
                pendingRecovery = new PendingRecovery(recoveryState, allocationId, stats, task, recoveryListener, context);
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
                // Get off the cluster applier thread. Generic executor has unbounded queue and thread shutdown happens
                // after service close so this runnable should never get rejected.
                executor.execute(
                    () -> RecoveryListener.wrapPreservingContext(recoveryListener, context)
                        .onRecoveryFailure(
                            new RecoveryCancelledException(
                                recoveryState.getShardId(),
                                recoveryState.getSourceNode(),
                                recoveryState.getTargetNode()
                            ),
                            true
                        )
                );
            }
            return;
        }
        logger.trace("enqueued recovery: {}", recoveryState);
        schedulingListener.onRecoveryQueued(recoveryState.getRecoverySource().getType(), RecoveryRole.TARGET);
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
            if (closed) {
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
                .onRecoveryFailure(new RecoveryCancelledException(state.getShardId(), state.getSourceNode(), state.getTargetNode()), false);
            schedulingListener.onQueuedRecoveryDiscarded(state.getRecoverySource().getType(), RecoveryRole.TARGET);
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
            if (closed) {
                return;
            }
            // This node has left the cluster's data nodes entirely (e.g. it's shutting down)
            if (localNode == null) {
                cancelledAllocationIds.clear();
                staleRecoveries.addAll(pendingRecoveries);
                pendingRecoveries.clear();
            } else {
                cancelledAllocationIds.entrySet()
                    .removeIf((cancellation) -> allocationIdIsOutdated(localNode, cancellation.getValue(), cancellation.getKey()));
                final Iterator<PendingRecovery> it = pendingRecoveries.iterator();
                while (it.hasNext()) {
                    final PendingRecovery pending = it.next();
                    final RecoveryState recoveryState = pending.recoveryState();
                    if (allocationIdIsOutdated(localNode, recoveryState.getShardId(), pending.allocationId())) {
                        it.remove();
                        staleRecoveries.add(pending);
                    }
                }
            }
        }
        for (PendingRecovery stale : staleRecoveries) {
            final RecoveryState state = stale.recoveryState();
            // Note that updating RecoveryStats is not strictly necessary here and just done out of completeness sake +
            // easier testing. Indeed, a pending recovery never started, and if its allocation ID has changed or localNode
            // became `null`, the old IndexShard objects those stats belong would have already been closed.
            stale.stats().targetQueuedRecoveryDiscarded(stale.recoveryState().getRecoverySource().getType());

            // Get off the cluster applier thread. Generic executor has unbounded queue and thread shutdown happens
            // after service close so this runnable should never get rejected.
            logger.debug("cancelling stale queued recovery {}", state);
            executor.execute(() -> {
                stale.listener()
                    .onRecoveryFailure(
                        new RecoveryCancelledException(state.getShardId(), state.getSourceNode(), state.getTargetNode()),
                        false
                    );
                schedulingListener.onQueuedRecoveryDiscarded(state.getRecoverySource().getType(), RecoveryRole.TARGET);
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
    public void close() {
        final List<PendingRecovery> recoveriesToAbort;
        synchronized (this) {
            // idempotent
            if (closed) {
                return;
            }
            closed = true;
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
            schedulingListener.onQueuedRecoveryDiscarded(pending.recoveryState().getRecoverySource().getType(), RecoveryRole.TARGET);
        }
        clusterService.removeListener(this);
    }

    // visible for testing
    synchronized boolean isClosed() {
        return closed;
    }

    /// Drains the pending queue up to the max slot capacity
    private void fillSlots() {
        final List<PendingRecovery> recoveriesToDispatch = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            while (pendingRecoveries.isEmpty() == false && runningRecoveries < maxConcurrentRecoveries) {
                final PendingRecovery recovery = pendingRecoveries.poll();
                recoveriesToDispatch.add(recovery);
                runningRecoveries++;
                recovery.stats().targetRecoveryDequeuedAndStarted(recovery.recoveryState().getRecoverySource().getType());
            }
        }
        for (PendingRecovery recovery : recoveriesToDispatch) {
            final RecoveryListener wrapped = RecoveryListener.wrapPreservingContext(
                RecoveryListener.runAfter(recovery.listener, () -> releaseSlot(recovery)),
                recovery.context
            );
            try (var ignored = recovery.context.get()) {
                executor.execute(new RecoveryRunnable(recovery, wrapped));
            }
            logger.trace("dispatched recovery: {}", recovery.recoveryState());
            schedulingListener.onRecoveryDequeuedAndStarted(recovery.recoveryState().getRecoverySource().getType(), RecoveryRole.TARGET);
        }
    }

    private void releaseSlot(PendingRecovery recovery) {
        final RecoverySource source = recovery.recoveryState().getRecoverySource();
        final int currentRunning;
        synchronized (this) {
            runningRecoveries--;
            currentRunning = runningRecoveries;
            assert currentRunning >= 0 : "negative number of running recoveries " + currentRunning;
            recovery.stats().targetRecoveryCompleted(source.getType());
        }
        logger.trace("recovery slot released: {}", recovery.recoveryState());
        schedulingListener.onRecoveryCompleted(source.getType(), RecoveryRole.TARGET);
        fillSlots();
    }

    private void setMaxConcurrentRecoveries(int newMaxConcurrentRecoveries) {
        final int previousLimit;
        synchronized (this) {
            previousLimit = this.maxConcurrentRecoveries;
            this.maxConcurrentRecoveries = newMaxConcurrentRecoveries;
        }
        if (previousLimit < newMaxConcurrentRecoveries) {
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
        String allocationId,
        RecoveryStats stats,
        Consumer<RecoveryListener> task,
        RecoveryListener listener,
        Supplier<ThreadContext.StoredContext> context
    ) {}

    /// Executable wrapper for a dispatched recovery. The provided recovery listener (from [PendingRecovery]) is wrapped
    /// with `runAfter` (to release a recovery slot on completion) and `assertOnce` (to ensure there is only one terminal callback).
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
            listener.onRecoveryFailure(new RecoveryFailedException(recoveryState, null, e), true);
        }

        @Override
        protected void doRun() {
            task.accept(listener);
        }
    }
}
