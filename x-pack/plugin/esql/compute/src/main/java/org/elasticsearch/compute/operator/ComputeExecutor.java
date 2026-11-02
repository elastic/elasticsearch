/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executor;

/**
 * Runs compute work, {@link Driver}s above all, on a thread pool while owning the order it runs in. A pool is FIFO,
 * so a driver woken because its exchange has pages or room again would wait behind every task submitted since it
 * went to sleep. Here such work goes to a separate queue that is served first, FIFO within each queue; every
 * {@link #NORMAL_EVERY}th turn goes to the normal queue instead so that new drivers, and the workers of in-flight
 * parallel operators, still make progress while resumed work saturates the pool. For every task one empty placeholder
 * is submitted to the pool after the task is queued; a placeholder runs whichever task is due, so the pool still sees
 * the real backlog and its bounded queue remains the admission limit. Both queues are read and written under one lock
 * so that a placeholder, or the rejection fallback, sees them as a whole: scanned one at a time, the contention is
 * expected to be small here.
 */
public final class ComputeExecutor implements Executor {
    static final int NORMAL_EVERY = 20;
    private final Executor executor;
    private final ThreadContext threadContext;

    // guarded under `lock`
    private final Object lock = new Object();
    private final Deque<AbstractRunnable> resumed = new ArrayDeque<>();
    private final Deque<AbstractRunnable> normal = new ArrayDeque<>();
    private long nextPoll;

    private final Runnable placeholder = new Runnable() {
        @Override
        public void run() {
            final AbstractRunnable task = poll();
            assert task != null : "task must be enqueued before";
            if (task != null) {
                task.run();
            }
        }

        @Override
        public String toString() {
            return "ComputeExecutor.Placeholder";
        }
    };

    public ComputeExecutor(Executor executor, ThreadContext threadContext) {
        this.executor = executor;
        this.threadContext = threadContext;
    }

    @Override
    public void execute(Runnable task) {
        if (task instanceof AbstractRunnable runnable) {
            submit(runnable, normal);
        } else {
            executor.execute(task);
        }
    }

    /**
     * Runs {@code task} ahead of normal work, for work that has already waited its turn, such as a driver resuming
     * after its exchange was blocked. If the pool is full the task is rejected via {@link AbstractRunnable#onRejection}.
     */
    public void executeResumed(AbstractRunnable task) {
        submit(task, resumed);
    }

    private void submit(AbstractRunnable task, Deque<AbstractRunnable> queue) {
        if (task.isForceExecution()) {
            // driver tasks should not use force-execution, so delegates here
            executor.execute(task);
            return;
        }
        final AbstractRunnable preserving = (AbstractRunnable) threadContext.preserveContext(task);
        synchronized (lock) {
            queue.addLast(preserving);
        }
        try {
            executor.execute(placeholder);
        } catch (EsRejectedExecutionException e) {
            // Normally our task is still queued, so remove it and reject it. But another placeholder may have already taken it.
            // In that case, reject another queued task instead, so queued tasks stay matched with pending placeholders. Prefer a
            // normal task, as resumed tasks are more critical.
            AbstractRunnable rejected;
            synchronized (lock) {
                rejected = queue.removeLastOccurrence(preserving) ? preserving : normal.pollLast();
                if (rejected == null) {
                    rejected = resumed.pollLast();
                }
            }
            assert rejected != null : "rejected placeholder without a task in " + this;
            if (rejected != null) {
                try {
                    rejected.onRejection(e);
                } finally {
                    rejected.onAfter();
                }
            }
        }
    }

    private AbstractRunnable poll() {
        synchronized (lock) {
            final boolean normalTurn = ++nextPoll % NORMAL_EVERY == 0;
            final AbstractRunnable task = (normalTurn ? normal : resumed).pollFirst();
            return task != null ? task : (normalTurn ? resumed : normal).pollFirst();
        }
    }

    @Override
    public String toString() {
        return "ComputeExecutor[executor=" + executor + "]";
    }
}
