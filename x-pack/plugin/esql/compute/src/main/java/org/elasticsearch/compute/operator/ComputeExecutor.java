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

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs compute work, {@link Driver}s above all, on a thread pool while owning the order it runs in. A pool is FIFO,
 * so a driver woken because its exchange has pages or room again would wait behind every task submitted since it
 * went to sleep. Here such work goes to a separate queue that is served first, FIFO within each queue; every
 * {@link #NORMAL_EVERY}th turn goes to the normal queue instead so that new drivers, and the workers of in-flight
 * parallel operators, still make progress while resumed work saturates the pool. For every task one empty placeholder
 * is submitted to the pool after the task is queued; a placeholder runs whichever task is due, so the pool still sees
 * the real backlog and its bounded queue remains the admission limit.
 */
public final class ComputeExecutor implements Executor {
    static final int NORMAL_EVERY = 20;

    private final Executor executor;
    private final ThreadContext threadContext;
    private final Deque<AbstractRunnable> resumed = new ConcurrentLinkedDeque<>();
    private final Deque<AbstractRunnable> normal = new ConcurrentLinkedDeque<>();
    private final AtomicLong polls = new AtomicLong();

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
        queue.addLast(preserving);
        try {
            executor.execute(placeholder);
        } catch (EsRejectedExecutionException e) {
            AbstractRunnable rejected = queue.removeLastOccurrence(preserving) ? preserving : normal.pollLast();
            if (rejected == null) {
                rejected = resumed.pollLast();
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
        final boolean normalTurn = polls.incrementAndGet() % NORMAL_EVERY == 0;
        final AbstractRunnable task = (normalTurn ? normal : resumed).pollFirst();
        return task != null ? task : (normalTurn ? resumed : normal).pollFirst();
    }

    @Override
    public String toString() {
        return "ComputeExecutor[executor=" + executor + "]";
    }
}
