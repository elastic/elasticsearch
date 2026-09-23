/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Driver be put to sleep while its sink is full or its source is empty or be rescheduled after running several iterations.
 * This scheduler tracks the delayed and scheduled tasks, allowing a sleeping driver to be woken up without waiting for its
 * sink or source. This enables fast cancellation or early finishing without discarding the current result.
 * <p>
 * Cancellation and early finishing are triggered from arbitrary threads, including transport workers handling a task ban.
 * Running the driver there would close its operators, which can release Lucene readers and block the transport worker, so a
 * woken driver is always resumed on its own executor. Once completing, that resumption is force-queued so a full queue does
 * not fail the driver with a rejection instead of letting it finish.
 */
final class DriverScheduler {
    private final AtomicReference<Runnable> delayedTask = new AtomicReference<>();
    private final AtomicReference<AbstractRunnable> scheduledTask = new AtomicReference<>();
    private final AtomicBoolean completing = new AtomicBoolean();

    void addOrRunDelayedTask(Runnable task) {
        delayedTask.set(task);
        if (completing.get()) {
            final Runnable toRun = delayedTask.getAndSet(null);
            if (toRun != null) {
                assert task == toRun;
                toRun.run();
            }
        }
    }

    void scheduleOrRunTask(Executor executor, AbstractRunnable task) {
        final AbstractRunnable existing = scheduledTask.getAndSet(task);
        assert existing == null : existing;
        final boolean forceExecution = completing.get();
        executor.execute(new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof EsRejectedExecutionException : new AssertionError(e);
                if (scheduledTask.getAndUpdate(t -> t == task ? null : t) == task) {
                    if (forceExecution) {
                        // Only a shut-down executor rejects a forced task. Let the driver finish here rather than fail it.
                        // This runs on the calling thread, but a node stops its transport before its thread pools, so this
                        // is never a transport worker. Failing the driver instead would close its operators on this thread too.
                        task.run();
                    } else {
                        task.onFailure(e);
                    }
                }
            }

            @Override
            protected void doRun() {
                AbstractRunnable toRun = scheduledTask.getAndSet(null);
                if (toRun == task) {
                    task.run();
                }
            }
        });
    }

    /**
     * Wakes up a sleeping driver so it can observe cancellation or early finishing. The delayed task only reschedules the
     * driver on its executor, so this never runs the driver on the calling thread. An already scheduled task is left to
     * the executor.
     */
    void runPendingTasks() {
        completing.set(true);
        final Runnable task = delayedTask.getAndSet(null);
        if (task != null) {
            task.run();
        }
    }
}
