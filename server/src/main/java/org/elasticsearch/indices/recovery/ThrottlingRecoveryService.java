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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Task queue for recoveries running on target node.
 * <p>
 * Limit the number of concurrent recoveries. Slots are filled when dispatching a recovery task
 * to the executor and released when {@link RecoveryListener} terminates
 * ({@link RecoveryListener#onRecoveryDone} / {@link RecoveryListener#onRecoveryFailure}),
 * through a callback to {@link #closeAndFillSlots()}.
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

    public ThrottlingRecoveryService(Executor executor, ClusterService clusterService) {
        this.executor = executor;

        if (clusterService.getClusterSettings().isDynamicSetting(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.getKey())) {
            // setting only registered in some tests today
            clusterService.getClusterSettings()
                .initializeAndWatch(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
        } else {
            maxConcurrentRecoveries = INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING.get(clusterService.getSettings());
        }
    }

    public void enqueue(RecoveryListener recoveryListener, Consumer<RecoveryListener> task) {
        RecoveryTask recoveryTask = new RecoveryTask(RecoveryListener.runAfter(recoveryListener, this::closeAndFillSlots), task);
        pendingRecoveries.add(recoveryTask);
        fillSlots();
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
                    dispatch(nextTask);
                } else {
                    runningRecoveries.decrementAndGet();
                }
            }
        }
    }

    private void closeAndFillSlots() {
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

    private void dispatch(RecoveryTask recoveryTask) {
        executor.execute(() -> recoveryTask.task.accept(recoveryTask.recoveryListener));
    }

    private record RecoveryTask(RecoveryListener recoveryListener, Consumer<RecoveryListener> task) {}
}
