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

    /** Test-only value: skip concurrency limiting (still forks to {@link ThreadPool#generic()} when unlimited). */
    public static final int UNLIMITED = -1;

    public static final int DEFAULT = ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES;

    private final ThreadPool threadPool;
    private final int maxConcurrentRecoveries;

    private final Queue<RecoveryTask> pending = new ArrayDeque<>();
    private int running;
    private final Object mutex = new Object();

    /** Fixed limit for tests (no dynamic updates). */
    public ThrottledInboundRecoveryService(ThreadPool threadPool, int maxConcurrentRecoveries) {
        this.threadPool = threadPool;
        this.maxConcurrentRecoveries = maxConcurrentRecoveries;
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

    private record RecoveryTask(RecoveryListener recoveryListener, Consumer<RecoveryListener> task) {}
}
