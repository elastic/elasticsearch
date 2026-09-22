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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/// Wraps an [AbstractThrottledTaskRunner] and publishes metrics about it under `es.throttled_task_runner.<name>.tasks.*`: the number
/// of queued tasks, the number of running tasks, and a histogram of how long each task waited in the queue.
///
/// Queue latency captures the waiting time till the task gets a free slot in the underlying runner, before it gets executed to the executor.
public class InstrumentedThrottledTaskRunner<T extends ActionListener<Releasable>> {
    static final String THROTTLED_TASK_RUNNER_METRIC_PREFIX = "es.throttled_task_runner.";
    static final String THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE = ".tasks.queue.size";
    static final String THROTTLED_TASK_RUNNER_METRIC_NAME_RUNNING = ".tasks.running.current";
    static final String THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE_TIME = ".tasks.queue_latency.histogram";

    private final AbstractThrottledTaskRunner<TimedTask<T>> runner;
    private final LongSupplier relativeTimeNanosProvider;
    private final LongHistogram queueLatencyMillisHistogram;

    /// Creates an instrumented runner over a FIFO queue.
    /// @param name: name of the task runner that must also be a valid metric name (see MetricValidator.validateMetricName)
    public InstrumentedThrottledTaskRunner(
        final String name,
        final int maxRunningTasks,
        final Executor executor,
        final MeterRegistry meterRegistry,
        final LongSupplier relativeTimeNanosProvider
    ) {
        this(name, maxRunningTasks, executor, ConcurrentCollections.newBlockingQueue(), meterRegistry, relativeTimeNanosProvider);
    }

    /// @param name: name of the task runner that must also be a valid metric name (see MetricValidator.validateMetricName)
    InstrumentedThrottledTaskRunner(
        final String name,
        final int maxRunningTasks,
        final Executor executor,
        final Queue<TimedTask<T>> taskQueue,
        final MeterRegistry meterRegistry,
        final LongSupplier relativeTimeNanosProvider
    ) {
        this.relativeTimeNanosProvider = relativeTimeNanosProvider;
        this.runner = new AbstractThrottledTaskRunner<>(name, maxRunningTasks, executor, taskQueue) {
            @Override
            protected void onDequeue(TimedTask<T> task) {
                queueLatencyMillisHistogram.record(
                    TimeUnit.NANOSECONDS.toMillis(relativeTimeNanosProvider.getAsLong() - task.queueStartNanos())
                );
            }
        };

        final var prefix = THROTTLED_TASK_RUNNER_METRIC_PREFIX + name;
        this.queueLatencyMillisHistogram = meterRegistry.registerLongHistogram(
            prefix + THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE_TIME,
            "time tasks spent in the queue for throttled task runner " + name,
            "milliseconds"
        );
        meterRegistry.registerLongAsyncGauge(
            prefix + THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE,
            "number of tasks waiting in the queue for throttled task runner " + name,
            "count",
            () -> new LongWithAttributes(runner.queuedTasks())
        );
        meterRegistry.registerLongAsyncGauge(
            prefix + THROTTLED_TASK_RUNNER_METRIC_NAME_RUNNING,
            "number of tasks currently running (i.e., submitted to the underlying executor) for throttled task runner " + name,
            "count",
            () -> new LongWithAttributes(runner.runningTasks())
        );
    }

    public void enqueueTask(final T task) {
        runner.enqueueTask(new TimedTask<>(task, relativeTimeNanosProvider.getAsLong()));
    }

    public void runSyncTasksEagerly(Executor executor) {
        runner.runSyncTasksEagerly(executor);
    }

    // exposed for testing
    int runningTasks() {
        return runner.runningTasks();
    }

    // exposed for testing
    int queuedTasks() {
        return runner.queuedTasks();
    }

    // we wrap each task in a TimedTask that includes its enqueued time, so we can compute the latency from the `runner'`s queue itself.
    record TimedTask<T extends ActionListener<Releasable>>(T task, long queueStartNanos) implements ActionListener<Releasable> {
        @Override
        public void onResponse(Releasable releasable) {
            task.onResponse(releasable);
        }

        @Override
        public void onFailure(Exception e) {
            task.onFailure(e);
        }

        @Override
        public String toString() {
            return task.toString();
        }
    }
}
