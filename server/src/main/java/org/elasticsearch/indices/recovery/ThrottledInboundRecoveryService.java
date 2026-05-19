/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
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
public final class ThrottledInboundRecoveryService {
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

    /** Test-only value: skip concurrency limiting (still forks to {@link ThreadPool#generic()} when unlimited). */
    public static final int UNLIMITED = -1;

    public static final int DEFAULT = ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES;

    private final ThreadPool threadPool;
    private volatile int maxConcurrentRecoveries;

    private final Queue<RecoveryTask> pending = new ArrayDeque<>();
    private int running;

    public ThrottledInboundRecoveryService(ThreadPool threadPool, ClusterSettings clusterSettings) {
        this.threadPool = threadPool;
        clusterSettings.initializeAndWatch(INDICES_RECOVERY_MAX_CONCURRENT_RECOVERIES_SETTING, this::setMaxConcurrentRecoveries);
    }

    public void enqueue(RecoveryListener recoveryListener, Consumer<RecoveryListener> task) {
        RecoveryTask recoveryTask = new RecoveryTask(RecoveryListener.runAfter(recoveryListener, this::scheduleNext), task);
        synchronized (this) {
            if (running < maxConcurrentRecoveries || maxConcurrentRecoveries == UNLIMITED) {
                running++;
                dispatch(recoveryTask);
            } else {
                pending.add(recoveryTask);
            }
        }
    }

    private void dispatch(RecoveryTask recoveryTask) {
        threadPool.generic().execute(() -> recoveryTask.task.accept(recoveryTask.recoveryListener));
    }

    private void scheduleNext() {
        RecoveryTask next;
        synchronized (this) {
            next = pending.poll();
            if (next == null) {
                running--;
            }
        }
        if (next != null) {
            dispatch(next);
        }
    }

    private void setMaxConcurrentRecoveries(Integer maxConcurrentRecoveries) {
        // todo: If maxConcurrentRecoveries is increased we need to schedule pending tasks up to the new limit
        //  If maxConcurrentRecoveries decrease we should not cancel or interrupt running tasks,
        //  but running tasks should not automatically schedule pending without looking at the newly updated limit.
        //  Is it safe to do the scheduling on the thread that updates the setting?
        //  Under heavy contention we might block, but everything that happens under synchronized should be quick.
        this.maxConcurrentRecoveries = maxConcurrentRecoveries;
    }

    private record RecoveryTask(RecoveryListener recoveryListener, Consumer<RecoveryListener> task) {}
}
