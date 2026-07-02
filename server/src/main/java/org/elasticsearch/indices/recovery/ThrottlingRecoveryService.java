/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/// Limit the number of concurrent recoveries. Slots are filled when dispatching a recovery task to the executor and
/// released when the recovery's [RecoveryListener] completes.
/// The max number of concurrent recovery slots is controlled by the [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING]
/// dynamic setting.
public final class ThrottlingRecoveryService implements Closeable {

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
    private final CompositeRecoverySchedulingListener schedulingListeners;

    private int maxConcurrentRecoveries;
    private int runningRecoveries = 0;
    private final Deque<PendingRecovery> pendingRecoveries = new ArrayDeque<>();

    private boolean closed;

    public ThrottlingRecoveryService(
        ThreadPool threadPool,
        ProjectResolver projectResolver,
        ClusterService clusterService,
        CompositeRecoverySchedulingListener schedulingListeners
    ) {
        this.executor = threadPool.generic();
        this.threadContext = threadPool.getThreadContext();
        this.projectResolver = projectResolver;
        this.schedulingListeners = schedulingListeners;
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    /// Enqueues a recovery task and/or dispatches it to the executor if there are any available slots.
    public void enqueue(
        ProjectId projectId,
        RecoveryListener recoveryListener,
        RecoveryState recoveryState,
        RecoveryStats stats,
        Consumer<RecoveryListener> task
    ) {
        final Supplier<ThreadContext.StoredContext> context = restorableContextForProject(projectId);
        final PendingRecovery pendingRecovery;
        synchronized (this) {
            if (closed == false) {
                pendingRecovery = new PendingRecovery(recoveryState, stats, task, recoveryListener, context);
                pendingRecoveries.add(pendingRecovery);
                stats.targetRecoveryQueued(recoveryState.getRecoverySource().getType());
            } else {
                pendingRecovery = null;
            }
        }
        if (pendingRecovery == null) {
            logger.debug("service is closed, aborting recovery: {}", recoveryState);
            RecoveryListener.wrapPreservingContext(recoveryListener, context).onRecoveryAborted();
            return;
        }
        logger.trace("enqueued recovery: {}", recoveryState);
        schedulingListeners.onRecoveryQueued(recoveryState.getRecoverySource().getType(), RecoveryRole.TARGET);
        fillSlots();
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
            for (PendingRecovery pending : recoveriesToAbort) {
                pending.stats().targetQueuedRecoveryDiscarded(pending.recoveryState().getRecoverySource().getType());
            }
        }
        for (PendingRecovery pending : recoveriesToAbort) {
            logger.trace("service closing, aborting recovery: {}", pending.recoveryState());
            RecoveryListener.wrapPreservingContext(pending.listener, pending.context).onRecoveryAborted();
            schedulingListeners.onQueuedRecoveryDiscarded(pending.recoveryState().getRecoverySource().getType(), RecoveryRole.TARGET);
        }
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
            schedulingListeners.onRecoveryDequeuedAndStarted(recovery.recoveryState().getRecoverySource().getType(), RecoveryRole.TARGET);
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
        schedulingListeners.onRecoveryCompleted(source.getType(), RecoveryRole.TARGET);
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
