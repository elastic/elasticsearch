/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Driver be put to sleep while its sink is full or its source is empty or be rescheduled after running several iterations.
 * This scheduler tracks the delayed and scheduled tasks, allowing them to run without waking up the driver or waiting for
 * the thread pool to pick up the task. This enables fast cancellation or early finishing without discarding the current result.
 */
final class DriverScheduler {
    private final AtomicReference<Runnable> delayedTask = new AtomicReference<>();
    private final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
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

    void scheduleOrRunTask(Executor executor, Runnable task) {
        final Runnable existing = scheduledTask.getAndSet(task);
        assert existing == null : existing;
        final Executor executorToUse = completing.get() ? EsExecutors.DIRECT_EXECUTOR_SERVICE : executor;
        executorToUse.execute(() -> {
            final Runnable next = scheduledTask.getAndSet(null);
            if (next != null) {
                assert next == task;
                next.run();
            }
        });
    }

    void runPendingTasks() {
        completing.set(true);
        for (var taskHolder : List.of(delayedTask, scheduledTask)) {
            final Runnable task = taskHolder.getAndSet(null);
            if (task != null) {
                task.run();
            }
        }
    }
}
