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

public class RecoveryThrottlingQueue {

    private final ThreadPool threadPool;
    // todo: Introduce separate setting to control maxConcurrentRecoveries.
    // Temporary defaults and unlimited for testing
    public static final int DEFAULT = ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES;
    public static final int UNLIMITED = -1;
    private final int maxConcurrentRecoveries;
    // 'pending' and 'running' both protected by synchronized(this)
    private final Queue<Runnable> pending = new ArrayDeque<>();
    private int running;

    public RecoveryThrottlingQueue(ThreadPool threadPool, int maxConcurrentRecoveries) {
        this.threadPool = threadPool;
        this.maxConcurrentRecoveries = maxConcurrentRecoveries;
    }

    public void enqueue(Runnable recoveryTask) {
        synchronized (this) {
            if (running < maxConcurrentRecoveries || maxConcurrentRecoveries == UNLIMITED) {
                running++;
                dispatch(recoveryTask);
            } else {
                pending.add(recoveryTask);
            }
        }
    }

    private void dispatch(Runnable recoveryTask) {
        threadPool.generic().execute(() -> {
            try {
                recoveryTask.run();
            } finally {
                scheduleNext();
            }
        });
    }

    private void scheduleNext() {
        Runnable next;
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
}
