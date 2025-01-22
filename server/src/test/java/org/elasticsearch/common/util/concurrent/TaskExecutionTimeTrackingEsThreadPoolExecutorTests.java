/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DEFAULT_EWMA_ALPHA;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for the automatic queue resizing of the {@code QueueResizingEsThreadPoolExecutorTests}
 * based on the time taken for each event.
 */
public class TaskExecutionTimeTrackingEsThreadPoolExecutorTests extends ESTestCase {

    public void testExecutionEWMACalculation() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);

        TaskExecutionTimeTrackingEsThreadPoolExecutor executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            new EsExecutors.QualifiedName("test-threadpool"),
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            settableWrapper(TimeUnit.NANOSECONDS.toNanos(100)),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            new TaskTrackingConfig(randomBoolean(), DEFAULT_EWMA_ALPHA),
            MeterRegistry.NOOP
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        assertThat(executor.getTotalTaskExecutionTime(), equalTo(0L));
        // Using the settableWrapper each task would take 100ns
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(30L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(100L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(51L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(200L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(65L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(300L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(75L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(400L));
        });
        executeTask(executor, 1);
        assertBusy(() -> {
            assertThat((long) executor.getTaskExecutionEWMA(), equalTo(83L));
            assertThat(executor.getTotalTaskExecutionTime(), equalTo(500L));
        });
        assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /** Use a runnable wrapper that simulates a task with unknown failures. */
    public void testExceptionThrowingTask() throws Exception {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        TaskExecutionTimeTrackingEsThreadPoolExecutor executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            new EsExecutors.QualifiedName("test-threadpool"),
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            exceptionalWrapper(),
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            new TaskTrackingConfig(randomBoolean(), DEFAULT_EWMA_ALPHA),
            MeterRegistry.NOOP
        );
        executor.prestartAllCoreThreads();
        logger.info("--> executor: {}", executor);

        // Using the exceptionalWrapper each task's execution time is -1 to simulate unknown failures/rejections.
        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        int taskCount = randomIntBetween(1, 100);
        executeTask(executor, taskCount);
        assertBusy(() -> assertThat(executor.getCompletedTaskCount(), equalTo((long) taskCount)));
        assertThat((long) executor.getTaskExecutionEWMA(), equalTo(0L));
        assertThat(executor.getTotalTaskExecutionTime(), equalTo(0L));
        assertThat(executor.getActiveCount(), equalTo(0));
        assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testGetOngoingTasks() throws Exception {
        var testStartTimeNanos = System.nanoTime();
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        var executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            new EsExecutors.QualifiedName("test-threadpool"),
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            TimedRunnable::new,
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            context,
            new TaskTrackingConfig(true, DEFAULT_EWMA_ALPHA),
            MeterRegistry.NOOP
        );
        var taskRunningLatch = new CountDownLatch(1);
        var exitTaskLatch = new CountDownLatch(1);
        assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0));
        Runnable runnable = () -> {
            taskRunningLatch.countDown();
            safeAwait(exitTaskLatch);
        };
        executor.execute(runnable);
        safeAwait(taskRunningLatch);
        var ongoingTasks = executor.getOngoingTasks();
        assertThat(ongoingTasks.toString(), ongoingTasks.size(), equalTo(1));
        assertThat(ongoingTasks.values().iterator().next(), greaterThanOrEqualTo(testStartTimeNanos));
        exitTaskLatch.countDown();
        assertBusy(() -> assertThat(executor.getOngoingTasks().toString(), executor.getOngoingTasks().size(), equalTo(0)));
        assertThat(executor.getTotalTaskExecutionTime(), greaterThan(0L));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void testQueueLatencyMetrics() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final var threadPoolName = randomIdentifier();
        var executor = new TaskExecutionTimeTrackingEsThreadPoolExecutor(
            new EsExecutors.QualifiedName(threadPoolName),
            1,
            1,
            1000,
            TimeUnit.MILLISECONDS,
            ConcurrentCollections.newBlockingQueue(),
            TimedRunnable::new,
            EsExecutors.daemonThreadFactory("queuetest"),
            new EsAbortPolicy(),
            new ThreadContext(Settings.EMPTY),
            new TaskTrackingConfig(true, DEFAULT_EWMA_ALPHA),
            meterRegistry
        );

        try {
            final var barrier = new CyclicBarrier(2);
            executor.execute(() -> {
                safeAwait(barrier);
                safeAwait(barrier);
            });
            safeAwait(barrier);

            Future<?> submit = executor.submit(() -> {
                // Do nothing
            });
            final long delayTimeMs = randomLongBetween(1, 30);
            safeSleep(delayTimeMs);
            safeAwait(barrier);
            safeGet(submit);

            List<Measurement> measurements = meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.DOUBLE_HISTOGRAM,
                    ThreadPool.THREAD_POOL_METRIC_PREFIX + threadPoolName
                        + TaskExecutionTimeTrackingEsThreadPoolExecutor.THREAD_POOL_METRIC_NAME_QUEUE_TIME
                );
            assertEquals(2, measurements.size());
            assertThat(measurements.get(1).getDouble(), greaterThanOrEqualTo(TimeValue.timeValueMillis(delayTimeMs).secondsFrac()));
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * The returned function outputs a WrappedRunnabled that simulates the case
     * where {@link TimedRunnable#getTotalExecutionNanos()} always returns {@code timeTakenNanos}.
     */
    private Function<Runnable, WrappedRunnable> settableWrapper(long timeTakenNanos) {
        return (runnable) -> new SettableTimedRunnable(timeTakenNanos, false);
    }

    /**
     * The returned function outputs a WrappedRunnabled that simulates the case
     * where {@link TimedRunnable#getTotalExecutionNanos()} returns -1 because
     * the job failed or was rejected before it finished.
     */
    private Function<Runnable, WrappedRunnable> exceptionalWrapper() {
        return (runnable) -> new SettableTimedRunnable(TimeUnit.NANOSECONDS.toNanos(-1), true);
    }

    /** Execute a blank task {@code times} times for the executor */
    private void executeTask(TaskExecutionTimeTrackingEsThreadPoolExecutor executor, int times) {
        logger.info("--> executing a task [{}] times", times);
        for (int i = 0; i < times; i++) {
            executor.execute(() -> {});
        }
    }

    public class SettableTimedRunnable extends TimedRunnable {
        private final long timeTaken;
        private final boolean testFailedOrRejected;

        public SettableTimedRunnable(long timeTaken, boolean failedOrRejected) {
            super(() -> {});
            this.timeTaken = timeTaken;
            this.testFailedOrRejected = failedOrRejected;
        }

        @Override
        public long getTotalExecutionNanos() {
            return timeTaken;
        }

        @Override
        public boolean getFailedOrRejected() {
            return testFailedOrRejected;
        }
    }
}
