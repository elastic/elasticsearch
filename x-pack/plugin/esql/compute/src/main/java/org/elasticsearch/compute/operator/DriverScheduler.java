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
 * Running the driver there would close its operators, which can release Lucene readers and block the transport worker. The
 * driver's own executor is no good either: it is bounded and shared with long-running drivers, so a cancelled driver would
 * wait in its queue while holding shard references and memory. Once completing, the driver is therefore resumed on a separate
 * completion executor, taking over a task that is still waiting in the driver's queue.
 */
final class DriverScheduler {
    private final AtomicReference<Runnable> delayedTask = new AtomicReference<>();
    private final AtomicReference<AbstractRunnable> scheduledTask = new AtomicReference<>();
    private final AtomicBoolean completing = new AtomicBoolean();
    private volatile Executor completionExecutor;

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

    void scheduleOrRunTask(Executor executor, Executor completionExecutor, AbstractRunnable task) {
        this.completionExecutor = completionExecutor;
        final AbstractRunnable existing = scheduledTask.getAndSet(task);
        assert existing == null : existing;
        if (completing.get()) {
            // Whoever clears the slot owns the task; runPendingTasks may be taking it over concurrently.
            if (scheduledTask.getAndSet(null) == task) {
                runOnCompletionExecutor(task);
            }
            return;
        }
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                assert e instanceof EsRejectedExecutionException : new AssertionError(e);
                if (scheduledTask.getAndUpdate(t -> t == task ? null : t) == task) {
                    // Failing the task closes the driver's operators, and the rejecting thread may be a transport worker.
                    failOnCompletionExecutor(task, e);
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
     * Wakes up a sleeping driver so it can observe cancellation or early finishing, and takes over a task that is still waiting
     * in the driver's executor. The sleeping driver's blocked future is completed on the calling thread, but the driver itself
     * resumes on the completion executor, unless that executor is shut down. The entry left in the driver's executor becomes
     * a no-op.
     */
    void runPendingTasks() {
        completing.set(true);
        final AbstractRunnable scheduled = scheduledTask.getAndSet(null);
        if (scheduled != null) {
            runOnCompletionExecutor(scheduled);
        }
        final Runnable task = delayedTask.getAndSet(null);
        if (task != null) {
            task.run();
        }
    }

    private void runOnCompletionExecutor(AbstractRunnable task) {
        completionExecutor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                assert e instanceof EsRejectedExecutionException : new AssertionError(e);
                // Only a shut-down executor rejects the task. Let the driver finish here rather than leave it unclosed.
                // A node stops its transport before its thread pools, so the calling thread is not a transport worker.
                task.run();
            }

            @Override
            protected void doRun() {
                task.run();
            }
        });
    }

    private void failOnCompletionExecutor(AbstractRunnable task, Exception rejection) {
        completionExecutor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                // Only a shut-down executor rejects the task. Fail the driver here rather than leave it unclosed.
                rejection.addSuppressed(e);
                task.onFailure(rejection);
            }

            @Override
            protected void doRun() {
                task.onFailure(rejection);
            }
        });
    }
}
