/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Schedules inbound shard recoveries on the target node with bounded concurrency.
 * <p>
 * Capacity slots are tied to {@link RecoveryListener} terminal callbacks.
 * <p>
 * Limit the number of concurrent inbound recoveries (target side). Slots are released when {@link RecoveryListener}
 * terminates ({@link RecoveryListener#onRecoveryDone} / {@link RecoveryListener#onRecoveryFailure}).
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

    private final ThreadPool threadPool;
    private volatile int maxConcurrentRecoveries;

    private final Queue<RecoveryTask> pending = new ArrayDeque<>();

    /** In-flight tasks: dispatched to {@link ThreadPool#generic()} and not yet completed ({@link #closeAndMaybeDispatch()} not run). */
    private int running;

    public ThrottlingRecoveryService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        this.threadPool = threadPool;
        clusterSettings.initializeAndWatch(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    public void enqueue(RecoveryListener recoveryListener, Consumer<RecoveryListener> task) {
        RecoveryTask recoveryTask = new RecoveryTask(RecoveryListener.runAfter(recoveryListener, this::closeAndMaybeDispatch), task);
        synchronized (this) {
            if (running < maxConcurrentRecoveries) {
                dispatch(recoveryTask);
            } else {
                pending.add(recoveryTask);
            }
        }
    }

    private void closeAndMaybeDispatch() {
        synchronized (this) {
            running--;
            assert running >= 0 : "negative number of running recoveries " + running;
            maybeDispatch();
        }
    }

    private void setMaxConcurrentRecoveries(Integer newMaxConcurrentRecoveries) {
        synchronized (this) {
            int oldMax = this.maxConcurrentRecoveries;
            this.maxConcurrentRecoveries = newMaxConcurrentRecoveries;
            if (oldMax < newMaxConcurrentRecoveries) {
                maybeDispatch();
            }
        }
    }

    /**
     * Caller must hold {@code this} lock
     * @param recoveryTask
     */
    private void dispatch(RecoveryTask recoveryTask) {
        running++;
        threadPool.generic().execute(() -> recoveryTask.task.accept(recoveryTask.recoveryListener));
    }

    /**
     * Dispatch next pending task(s) in queue up to max number of concurrent tasks has been reached or queue is empty.
     * Caller must hold {@code this} lock
     */
    private void maybeDispatch() {
        while (running < maxConcurrentRecoveries && pending.isEmpty() == false) {
            assert running < maxConcurrentRecoveries : running + " vs " + maxConcurrentRecoveries;
            RecoveryTask next = pending.poll();
            assert next != null;
            dispatch(next);
        }
    }

    private record RecoveryTask(RecoveryListener recoveryListener, Consumer<RecoveryListener> task) {}
}
