/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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
    // Dynamically updated through setting updates, controls max number of running "slots"
    private volatile int maxConcurrentRecoveries;
    // Recoveries that have been dispatched to the executor and not yet reached closeAndMaybeDispatch
    private final AtomicInteger runningRecoveries = new AtomicInteger(0);
    // Queue of recoveries waiting to be dispatched
    private final Queue<PendingRecovery> pendingRecoveries = new ConcurrentLinkedQueue<>();

    // visible for testing
    final AtomicBoolean closed = new AtomicBoolean();

    public ThrottlingRecoveryService(Executor executor, ClusterService clusterService) {
        this.executor = executor;
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    /// Enqueues a recovery task and/or dispatches it to the executor if there are any available slots.
    public void enqueue(RecoveryListener recoveryListener, RecoveryState recoveryState, Consumer<RecoveryListener> task) {
        if (closed.get()) {
            logger.debug(
                "service is closed, aborting recovery: recoverySource: [{}], shardId: [{}]",
                recoveryState.getRecoverySource(),
                recoveryState.getShardId()
            );
            recoveryListener.onRecoveryAborted();
            return;
        }
        logger.trace(
            "enqueue recovery: recoverySource: [{}], shardId: [{}]",
            recoveryState.getRecoverySource(),
            recoveryState.getShardId()
        );
        pendingRecoveries.add(new PendingRecovery(recoveryState, task, recoveryListener));
        fillSlots();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            drainQueue();
        }
    }

    /// Aborts all pending (not-yet-dispatched) recoveries by calling [RecoveryListener#onRecoveryAborted] on each.
    private void drainQueue() {
        assert closed.get();
        PendingRecovery pending;
        while ((pending = pendingRecoveries.poll()) != null) {
            pending.listener.onRecoveryAborted();
        }
    }

    /// Drains the pending queue up to the max slot capacity
    void fillSlots() {
        while (closed.get() == false && pendingRecoveries.isEmpty() == false) {
            final int current = runningRecoveries.get();
            if (current >= maxConcurrentRecoveries) {
                break;
            }
            if (runningRecoveries.compareAndSet(current, current + 1)) {
                PendingRecovery next = pendingRecoveries.poll();
                if (next != null) {
                    logger.trace(
                        "dispatch recovery: recoverySource: [{}], shardId: [{}]",
                        next.recoveryState.getRecoverySource(),
                        next.recoveryState.getShardId()
                    );
                    executor.execute(new RecoveryRunnable(next, () -> {
                        logger.trace(
                            "close recovery: recoverySource: [{}], shardId: [{}]",
                            next.recoveryState.getRecoverySource(),
                            next.recoveryState.getShardId()
                        );
                        int currentRunning = runningRecoveries.decrementAndGet();
                        assert currentRunning >= 0 : "negative number of running recoveries " + currentRunning;
                        fillSlots();
                    }));
                } else {
                    runningRecoveries.decrementAndGet();
                }
            }
        }
        if (closed.get()) {
            drainQueue();
        }
    }

    private void setMaxConcurrentRecoveries(Integer newMaxConcurrentRecoveries) {
        int oldMax = this.maxConcurrentRecoveries;
        this.maxConcurrentRecoveries = newMaxConcurrentRecoveries;
        if (oldMax < newMaxConcurrentRecoveries) {
            fillSlots();
        }
    }

    // visible for testing
    int currentQueueSize() {
        return pendingRecoveries.size();
    }

    private record PendingRecovery(RecoveryState recoveryState, Consumer<RecoveryListener> task, RecoveryListener listener) {}

    private static class RecoveryRunnable extends AbstractRunnable {
        private final RecoveryState recoveryState;
        private final Consumer<RecoveryListener> task;
        private final RecoveryListener listener;

        private RecoveryRunnable(PendingRecovery pending, Runnable runAfter) {
            this.recoveryState = pending.recoveryState;
            this.task = pending.task;
            this.listener = RecoveryListener.assertOnce(RecoveryListener.runAfter(pending.listener, runAfter));
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
