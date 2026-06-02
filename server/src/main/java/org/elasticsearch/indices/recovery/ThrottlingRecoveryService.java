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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Task queue for recoveries running on target node.
 * <p>
 * Limit the number of concurrent recoveries. Slots are filled when dispatching a recovery task
 * to the executor and released when {@link RecoveryListener} terminates
 * ({@link RecoveryListener#onRecoveryDone} / {@link RecoveryListener#onRecoveryFailure}),
 * through a callback to {@link #closeAndFillSlots(RecoveryTask)}.
 * <p>
 * Max number of concurrent recovery slots are controlled by setting {@link #INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING}
 * which can be dynamically updated. If limit is increased, pending tasks will be dispatched up to the new limit.
 * If limit decreases, no tasks will be canceled, and we will let running tasks finish.
 */
public final class ThrottlingRecoveryService {
    /**
     * Controls the number of concurrent recoveries allowed on target node.
     * Target node is the node that owns the shard after recovery is finished.
     * This setting applies to all types of recoveries, not only peer recovery.
     */
    private static final int DEFAULT_INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES = 10;
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_recoveries",
        DEFAULT_INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Executor executor;
    // Dynamically updated through setting updates, controls max number of running "slots"
    private volatile int maxConcurrentRecoveries;
    // Recoveries that has been dispatched to executor and not yet reached closeAndMaybeDispatch
    private final AtomicInteger runningRecoveries = new AtomicInteger(0);
    // Queue of recoveries waiting to be dispatched
    private final Queue<RecoveryTask> pendingRecoveries = new ConcurrentLinkedQueue<>();
    private final Logger logger = LogManager.getLogger(ThrottlingRecoveryService.class);

    public ThrottlingRecoveryService(Executor executor, ClusterService clusterService) {
        this.executor = executor;
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    /**
     * Enqueue recovery task and dispatch to async Executor if there are available slots.
     * Exceptions from task are propagated to {@link RecoveryListener#onRecoveryFailure(RecoveryFailedException, boolean)}
     * on provided listener.
     */
    public void enqueue(RecoveryListener recoveryListener, RecoveryState recoveryState, Consumer<RecoveryListener> task) {
        logger.trace(
            "enqueue recovery: recoverySource: [{}], shardId: [{}]",
            recoveryState.getRecoverySource(),
            recoveryState.getShardId()
        );
        pendingRecoveries.add(asRecoveryTask(recoveryListener, recoveryState, task));
        fillSlots();
    }

    /**
     * Add termination hook to recoveryListener that dispatches next pending recovery task.
     * Add exception handling to task by passing exceptions to listener.
     * Return a Runnable, ready to be dispatched to Executor or put on the pending queue.
     */
    private RecoveryTask asRecoveryTask(RecoveryListener recoveryListener, RecoveryState recoveryState, Consumer<RecoveryListener> task) {
        return new RecoveryTask(recoveryListener, recoveryState, task);
    }

    /**
     * Drain the pending queue up to max slot capacity (maxConcurrentRecoveries)
     */
    private void fillSlots() {
        int current;
        while ((current = runningRecoveries.get()) < maxConcurrentRecoveries && !pendingRecoveries.isEmpty()) {
            if (runningRecoveries.compareAndSet(current, current + 1)) {
                RecoveryTask nextTask = pendingRecoveries.poll();
                if (nextTask != null) {
                    logger.trace(
                        "dispatch recovery: recoverySource: [{}], shardId: [{}]",
                        nextTask.recoveryState.getRecoverySource(),
                        nextTask.recoveryState.getShardId()
                    );
                    executor.execute(nextTask);
                } else {
                    runningRecoveries.decrementAndGet();
                }
            }
        }
    }

    private void closeAndFillSlots(RecoveryTask recoveryTask) {
        logger.trace(
            "close recovery: recoverySource: [{}], shardId: [{}]",
            recoveryTask.recoveryState.getRecoverySource(),
            recoveryTask.recoveryState.getShardId()
        );
        int current = runningRecoveries.decrementAndGet();
        assert current >= 0 : "negative number of running recoveries " + current;
        fillSlots();
    }

    private void setMaxConcurrentRecoveries(Integer newMaxConcurrentRecoveries) {
        int oldMax = this.maxConcurrentRecoveries;
        this.maxConcurrentRecoveries = newMaxConcurrentRecoveries;
        if (oldMax < newMaxConcurrentRecoveries) {
            fillSlots();
        }
    }

    private class RecoveryTask extends AbstractRunnable {
        private final RecoveryListener listener;
        private final RecoveryState recoveryState;
        private final Consumer<RecoveryListener> task;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private RecoveryTask(RecoveryListener recoveryListener, RecoveryState recoveryState, Consumer<RecoveryListener> task) {
            this.recoveryState = recoveryState;
            this.task = task;
            this.listener = RecoveryListener.runAfter(recoveryListener, () -> {
                boolean firstRelease = released.compareAndSet(false, true);
                assert firstRelease : "already released";
                // noinspection ConstantValue
                if (firstRelease) {
                    closeAndFillSlots(this);
                }
            });
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
