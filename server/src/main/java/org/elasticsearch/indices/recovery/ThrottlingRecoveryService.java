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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;
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

    private int maxConcurrentRecoveries;
    private int runningRecoveries = 0;
    private final Deque<PendingRecovery> pendingRecoveries = new ArrayDeque<>();

    private boolean closed;

    public ThrottlingRecoveryService(Executor executor, ClusterService clusterService) {
        this.executor = executor;
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    /// Enqueues a recovery task and/or dispatches it to the executor if there are any available slots.
    public void enqueue(RecoveryListener recoveryListener, RecoveryState recoveryState, Consumer<RecoveryListener> task) {
        final PendingRecovery pendingRecovery;
        synchronized (this) {
            if (closed == false) {
                pendingRecovery = new PendingRecovery(recoveryState, task, recoveryListener);
                pendingRecoveries.add(pendingRecovery);
            } else {
                pendingRecovery = null;
            }
        }
        if (pendingRecovery == null) {
            logger.debug("service is closed, aborting recovery: {}", recoveryState);
            recoveryListener.onRecoveryAborted();
            return;
        }
        logger.trace("enqueued recovery: {}", recoveryState);
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
        }
        for (PendingRecovery pending : recoveriesToAbort) {
            final RecoveryState state = pending.recoveryState;
            logger.trace("service closing, aborting recovery: {}", state);
            pending.listener.onRecoveryAborted();
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
                recoveriesToDispatch.add(pendingRecoveries.poll());
                runningRecoveries++;
            }
        }
        for (PendingRecovery recovery : recoveriesToDispatch) {
            final RecoveryState state = recovery.recoveryState;
            executor.execute(new RecoveryRunnable(recovery, () -> releaseSlot(state)));
            logger.trace("dispatched recovery: {}", state);
        }
    }

    private void releaseSlot(RecoveryState state) {
        final int currentRunning;
        synchronized (this) {
            runningRecoveries--;
            currentRunning = runningRecoveries;
        }
        assert currentRunning >= 0 : "negative number of running recoveries " + currentRunning;
        logger.trace("recovery slot released: {}", state);
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

    /// Metadata holder for a recovery that has been enqueued but not yet dispatched.
    /// The `listener` is the one passed in to [#enqueue] by indicesServices. Slot-release and other wrappers are added
    /// at dispatch time, such that aborting a queued-but-never-dispatched task does not decrement a slot that was never taken
    private record PendingRecovery(RecoveryState recoveryState, Consumer<RecoveryListener> task, RecoveryListener listener) {}

    /// Executable wrapper for a dispatched recovery. The provided recovery listener (from [PendingRecovery]) is wrapped
    /// with `runAfter` (to release a recovery slot on completion) and `assertOnce` (to ensure there is only one terminal callback).
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
