/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.QueueTimeAware;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

/**
 * A Driver be put to sleep while its sink is full or its source is empty or be rescheduled after running several iterations.
 * This scheduler tracks the delayed and scheduled tasks, allowing them to run without waking up the driver or waiting for
 * the thread pool to pick up the task. This enables fast cancellation or early finishing without discarding the current result.
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

    /**
     * Runs {@code task} on {@code executor}, or on the calling thread once the driver is completing.
     * @param queueTimeListener told how long the submitted runnable waited in the queue, unless {@link #runPendingTasks()} already
     *                          ran or replaced the task in the meantime
     */
    void scheduleOrRunTask(Executor executor, AbstractRunnable task, LongConsumer queueTimeListener) {
        final AbstractRunnable existing = scheduledTask.getAndSet(task);
        assert existing == null : existing;
        final Executor executorToUse = completing.get() ? EsExecutors.DIRECT_EXECUTOR_SERVICE : executor;
        class QueueTimeReportingRunnable extends AbstractRunnable implements QueueTimeAware {
            @Override
            public void onQueueTime(long queueTimeMicros) {
                if (scheduledTask.get() == task) {
                    queueTimeListener.accept(queueTimeMicros);
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof EsRejectedExecutionException : new AssertionError(e);
                if (scheduledTask.getAndUpdate(t -> t == task ? null : t) == task) {
                    task.onFailure(e);
                }
            }

            @Override
            protected void doRun() {
                AbstractRunnable toRun = scheduledTask.getAndSet(null);
                if (toRun == task) {
                    task.run();
                }
            }
        }
        executorToUse.execute(new QueueTimeReportingRunnable());
    }

    void runPendingTasks() {
        completing.set(true);
        for (var taskHolder : List.of(scheduledTask, delayedTask)) {
            final Runnable task = taskHolder.getAndSet(null);
            if (task != null) {
                task.run();
            }
        }
    }
}
