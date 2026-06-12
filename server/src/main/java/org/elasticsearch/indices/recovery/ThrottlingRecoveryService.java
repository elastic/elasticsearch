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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/// Limit the number of concurrent recoveries. Slots are filled when dispatching a recovery task to the executor and
 /// released when the recovery's [RecoveryListener] completes.
 /// The max number of concurrent recovery slots is controlled by the [#INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING]
 /// dynamic setting.
public final class ThrottlingRecoveryService {

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
    // Recoveries that has been dispatched to executor and not yet reached closeAndMaybeDispatch
    private final AtomicInteger runningRecoveries = new AtomicInteger(0);
    // Queue of recoveries waiting to be dispatched
    private final Queue<RecoveryTask> pendingRecoveries = new ConcurrentLinkedQueue<>();
    private static final Logger logger = LogManager.getLogger(ThrottlingRecoveryService.class);

    public ThrottlingRecoveryService(Executor executor, ClusterService clusterService) {
        this.executor = executor;
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    /// Enqueues a recovery task and/or dispatches it to the executor if there are any available slots.
    public void enqueue(RecoveryListener recoveryListener, RecoveryState recoveryState, Consumer<RecoveryListener> task) {
        logger.trace(
            "enqueue recovery: recoverySource: [{}], shardId: [{}]",
            recoveryState.getRecoverySource(),
            recoveryState.getShardId()
        );
        pendingRecoveries.add(new RecoveryTask(recoveryState, task, RecoveryListener.runAfter(recoveryListener, () -> {
            logger.trace(
                "close recovery: recoverySource: [{}], shardId: [{}]",
                recoveryState.getRecoverySource(),
                recoveryState.getShardId()
            );
            int current = runningRecoveries.decrementAndGet();
            assert current >= 0 : "negative number of running recoveries " + current;
            fillSlots();
        })));
        fillSlots();
    }

    /// Drains the pending queue up to the max slot capacity
    private void fillSlots() {
        int current;
        while (((current = runningRecoveries.get()) < maxConcurrentRecoveries || maxConcurrentRecoveries == Integer.MAX_VALUE)
            && !pendingRecoveries.isEmpty()) {
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

    private void setMaxConcurrentRecoveries(Integer newMaxConcurrentRecoveries) {
        int oldMax = this.maxConcurrentRecoveries;
        this.maxConcurrentRecoveries = newMaxConcurrentRecoveries;
        if (oldMax < newMaxConcurrentRecoveries) {
            fillSlots();
        }
    }

    private static class RecoveryTask extends AbstractRunnable {
        private final RecoveryState recoveryState;
        private final Consumer<RecoveryListener> task;
        private final RecoveryListener listener;

        private RecoveryTask(RecoveryState recoveryState, Consumer<RecoveryListener> task, RecoveryListener recoveryListener) {
            this.recoveryState = recoveryState;
            this.task = task;
            this.listener = RecoveryListener.assertOnce(recoveryListener);
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
