/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ConcurrentLinkedQueue;

public class RecoveryThrottlingQueue {

    private final ThreadPool threadPool;
    private final int maxConcurrentRecoveries;
    private final ConcurrentLinkedQueue<RecoveryTask> pending = new ConcurrentLinkedQueue<>();
    private final Object mutex = new Object();
    // Protected by mutex
    private int running;

    public RecoveryThrottlingQueue(ThreadPool threadPool, int maxConcurrentRecoveries) {
        this.threadPool = threadPool;
        this.maxConcurrentRecoveries = maxConcurrentRecoveries;
    }

    public void enqueue(RecoveryTask recoveryTask) {
        synchronized (mutex) {
            if (running < maxConcurrentRecoveries) {
                running++;
                dispatch(recoveryTask);
            } else {
                pending.add(recoveryTask);
            }
        }
    }

    private void dispatch(RecoveryTask recoveryTask) {
        threadPool.generic().execute(() -> {
            try {
                recoveryTask.run();
            } finally {
                scheduleNext();
            }
        });
    }

    private void scheduleNext() {
        RecoveryTask next;
        synchronized (mutex) {
            next = pending.poll();
            if (next == null) {
                running--;
            }
        }
        if (next != null) {
            dispatch(next);
        }
    }

    @FunctionalInterface
    public interface RecoveryTask extends Runnable {}
}
