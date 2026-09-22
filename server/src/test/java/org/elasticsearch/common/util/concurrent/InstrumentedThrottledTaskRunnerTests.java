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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestEsExecutors;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.util.concurrent.InstrumentedThrottledTaskRunner.THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE;
import static org.elasticsearch.common.util.concurrent.InstrumentedThrottledTaskRunner.THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE_TIME;
import static org.elasticsearch.common.util.concurrent.InstrumentedThrottledTaskRunner.THROTTLED_TASK_RUNNER_METRIC_NAME_RUNNING;
import static org.elasticsearch.common.util.concurrent.InstrumentedThrottledTaskRunner.THROTTLED_TASK_RUNNER_METRIC_PREFIX;
import static org.hamcrest.Matchers.equalTo;

public class InstrumentedThrottledTaskRunnerTests extends ESTestCase {

    private static final ThreadFactory threadFactory = TestEsExecutors.testOnlyDaemonThreadFactory("test");
    private static final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    private ExecutorService executor;
    private int maxThreads;

    @Before
    public void createExecutor() throws Exception {
        maxThreads = between(1, 10);
        executor = EsExecutors.newScaling("test", maxThreads, maxThreads, 0, TimeUnit.MILLISECONDS, false, threadFactory, threadContext);
    }

    @After
    public void terminateExecutor() throws Exception {
        terminate(executor);
    }

    public void testInstrumentedThrottledTaskRunnerRecordsQueuingAndRunningCounts() throws Exception {
        final String runnerName = "some_throttler";
        final String queueSizeMetric = THROTTLED_TASK_RUNNER_METRIC_PREFIX + runnerName + THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE;
        final String runningMetric = THROTTLED_TASK_RUNNER_METRIC_PREFIX + runnerName + THROTTLED_TASK_RUNNER_METRIC_NAME_RUNNING;

        final var taskRunning = new CountDownLatch(1);
        final var taskCanFinish = new CountDownLatch(1);

        final var registry = new RecordingMeterRegistry();
        final var taskRunner = new InstrumentedThrottledTaskRunner<ActionListener<Releasable>>(runnerName, 1, executor, registry, () -> 0L);

        // enqueue a single task and hold it there so we know it's running
        taskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    taskRunning.countDown();
                    safeAwait(taskCanFinish);
                }
            }
        });
        safeAwait(taskRunning);

        // enqueue a second task that will get queued because of maxRunningTasks=1
        taskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                releasable.close();
            }
        });

        // collect metrics
        registry.getRecorder().collect();
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, queueSizeMetric),
            RecordingMeterRegistry.measures(1L)
        );
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, runningMetric),
            RecordingMeterRegistry.measures(1L)
        );

        // let the first task finish and hence the queued one will also get a slot and finish
        taskCanFinish.countDown();
        assertNoRunningTasks(taskRunner);

        // reset and re-collect metrics and both queue- and running-size should be 0 now since we had no queued or running tasks right
        // before
        registry.getRecorder().resetCalls();
        registry.getRecorder().collect();
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, queueSizeMetric),
            RecordingMeterRegistry.measures(0L)
        );
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, runningMetric),
            RecordingMeterRegistry.measures(0L)
        );
    }

    public void testInstrumentedThrottledTaskRunnerRecordsQueueLatency() throws Exception {
        final String runnerName = "some_throttler";
        final String queueLatencyMetric = THROTTLED_TASK_RUNNER_METRIC_PREFIX + runnerName + THROTTLED_TASK_RUNNER_METRIC_NAME_QUEUE_TIME;

        final var taskRunning = new CountDownLatch(1);
        final var taskCanFinish = new CountDownLatch(1);

        final var registry = new RecordingMeterRegistry();
        // we enqueue at 0ns and start running at 5_000_000ns, so we have 5ms latency
        final long[] clockValues = { 0L, 5_000_000L };
        final var clockIndex = new AtomicInteger();
        final var taskRunner = new InstrumentedThrottledTaskRunner<ActionListener<Releasable>>(
            runnerName,
            1,
            executor,
            registry,
            () -> clockValues[clockIndex.getAndIncrement()]
        );

        // enqueue a single task and hold it there so we know it's running
        taskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    taskRunning.countDown();
                    safeAwait(taskCanFinish);
                }
            }
        });
        safeAwait(taskRunning);

        // let the task finish
        taskCanFinish.countDown();
        assertNoRunningTasks(taskRunner);

        registry.getRecorder().collect();
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, queueLatencyMetric),
            RecordingMeterRegistry.measures(5L)
        );
    }

    private void assertNoRunningTasks(InstrumentedThrottledTaskRunner<?> taskRunner) {
        final var barrier = new CyclicBarrier(maxThreads + 1);
        for (int i = 0; i < maxThreads; i++) {
            executor.execute(() -> safeAwait(barrier));
        }
        safeAwait(barrier);
        assertThat(taskRunner.runningTasks(), equalTo(0));
    }
}
