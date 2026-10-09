/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.InstrumentedThrottledTaskRunner.TimedTask;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * {@link PrioritizedThrottledAsyncTaskRunner} performs the enqueued tasks in the order dictated by the
 * natural ordering of the tasks, limiting the max number of concurrently running tasks. Each new task
 * that is dequeued to be run, is forked off to the given executor.
 */
public class PrioritizedThrottledAsyncTaskRunner<T extends ActionListener<Releasable> & Comparable<T>> {
    private final Consumer<T> enqueuer;
    private final IntSupplier runningTasks;
    private final IntSupplier queuedTasks;
    private final IntSupplier maxRunningTasksGetter;
    private final IntConsumer maxRunningTasksSetter;

    public PrioritizedThrottledAsyncTaskRunner(final String name, final int maxRunningTasks, final Executor executor) {
        final var runner = new AbstractThrottledTaskRunner<T>(name, maxRunningTasks, executor, new PriorityBlockingQueue<>());
        this.enqueuer = runner::enqueueTask;
        this.runningTasks = runner::runningTasks;
        this.queuedTasks = runner::queuedTasks;
        this.maxRunningTasksGetter = runner::getMaxRunningTasks;
        this.maxRunningTasksSetter = runner::setMaxRunningTasks;
    }

    /**
     * Creates an instrumented runner, see {@link InstrumentedThrottledTaskRunner}.
     */
    public PrioritizedThrottledAsyncTaskRunner(
        final String name,
        final int maxRunningTasks,
        final Executor executor,
        final MeterRegistry meterRegistry,
        final LongSupplier relativeTimeNanosProvider
    ) {
        final var runner = new InstrumentedThrottledTaskRunner<T>(
            name,
            maxRunningTasks,
            executor,
            new PriorityBlockingQueue<>(11, Comparator.comparing(TimedTask::task)),
            meterRegistry,
            relativeTimeNanosProvider
        );
        this.enqueuer = runner::enqueueTask;
        this.runningTasks = runner::runningTasks;
        this.queuedTasks = runner::queuedTasks;
        this.maxRunningTasksGetter = runner::getMaxRunningTasks;
        this.maxRunningTasksSetter = runner::setMaxRunningTasks;
    }

    /**
     * Submits a task for execution. If there are fewer than {@code maxRunningTasks} tasks currently running then this task is immediately
     * submitted to the executor. Otherwise this task is enqueued and will be submitted to the executor in turn on completion of some other
     * task.
     */
    public void enqueueTask(final T task) {
        enqueuer.accept(task);
    }

    public int getMaxRunningTasks() {
        return maxRunningTasksGetter.getAsInt();
    }

    /**
     * Changes the max number of concurrently running tasks, see {@link AbstractThrottledTaskRunner#setMaxRunningTasks}.
     */
    public void setMaxRunningTasks(int maxRunningTasks) {
        maxRunningTasksSetter.accept(maxRunningTasks);
    }

    // Used by tests and for monitoring
    public int runningTasks() {
        return runningTasks.getAsInt();
    }

    // Used by tests and for monitoring
    public int queueSize() {
        return queuedTasks.getAsInt();
    }
}
