/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.internal.BuiltInExecutorBuilders;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DEFAULT;
import static org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig.DO_NOT_TRACK;
import static org.elasticsearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.LATE_TIME_INTERVAL_WARN_THRESHOLD_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.assertCurrentMethodIsNotCalledRecursively;
import static org.elasticsearch.threadpool.ThreadPool.getMaxSnapshotThreadPoolSize;
import static org.elasticsearch.threadpool.ThreadPool.halfAllocatedProcessorsMaxFive;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class ThreadPoolTests extends ESTestCase {

    public void testBoundedByBelowMin() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(Integer.MIN_VALUE, min - 1);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(min));
    }

    public void testBoundedByAboveMax() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(max + 1, Integer.MAX_VALUE);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(max));
    }

    public void testBoundedByBetweenMinAndMax() {
        int min = randomIntBetween(0, 32);
        int max = randomIntBetween(min + 1, 64);
        int value = randomIntBetween(min, max);
        assertThat(ThreadPool.boundedBy(value, min, max), equalTo(value));
    }

    public void testOneEighthAllocatedProcessors() {
        assertThat(ThreadPool.oneEighthAllocatedProcessors(1), equalTo(1));
        assertThat(ThreadPool.oneEighthAllocatedProcessors(4), equalTo(1));
        assertThat(ThreadPool.oneEighthAllocatedProcessors(8), equalTo(1));
        assertThat(ThreadPool.oneEighthAllocatedProcessors(32), equalTo(4));
    }

    public void testAbsoluteTime() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            long currentTime = System.currentTimeMillis();
            long gotTime = threadPool.absoluteTimeInMillis();
            long delta = Math.abs(gotTime - currentTime);
            // the delta can be large, we just care it is the same order of magnitude
            assertTrue("thread pool cached absolute time " + gotTime + " is too far from real current time " + currentTime, delta < 10000);
        } finally {
            terminate(threadPool);
        }
    }

    public void testEstimatedTimeIntervalSettingAcceptsOnlyZeroAndPositiveTime() {
        final Settings settings = Settings.builder().put("thread_pool.estimated_time_interval", -1).build();
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> ESTIMATED_TIME_INTERVAL_SETTING.get(settings)).getMessage(),
            equalTo("failed to parse value [-1] for setting [thread_pool.estimated_time_interval], must be >= [0ms]")
        );
    }

    public void testLateTimeIntervalWarningSettingAcceptsOnlyZeroAndPositiveTime() {
        final Settings settings = Settings.builder().put("thread_pool.estimated_time_interval.warn_threshold", -1).build();
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> LATE_TIME_INTERVAL_WARN_THRESHOLD_SETTING.get(settings)).getMessage(),
            equalTo("failed to parse value [-1] for setting [thread_pool.estimated_time_interval.warn_threshold], must be >= [0ms]")
        );
    }

    public void testLateTimeIntervalWarningMuchLongerThanEstimatedTimeIntervalByDefault() {
        assertThat(
            LATE_TIME_INTERVAL_WARN_THRESHOLD_SETTING.get(Settings.EMPTY).getMillis(),
            greaterThan(ESTIMATED_TIME_INTERVAL_SETTING.get(Settings.EMPTY).getMillis() + 4000)
        );
    }

    public void testTimerThreadWarningLogging() throws Exception {
        try (var mockLog = MockLog.capture(ThreadPool.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warning for absolute clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [*] on absolute clock which is above the warn threshold of [100ms]"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warning for relative clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [*] on relative clock which is above the warn threshold of [100ms]"
                )
            );

            final ThreadPool.CachedTimeThread thread = new ThreadPool.CachedTimeThread("[timer]", 200, 100);
            thread.start();

            mockLog.awaitAllExpectationsMatched();

            thread.interrupt();
            thread.join();
        }
    }

    public void testTimeChangeChecker() throws Exception {
        try (var mockLog = MockLog.capture(ThreadPool.class)) {
            long absoluteMillis = randomLong(); // overflow should still be handled correctly
            long relativeNanos = randomLong(); // overflow should still be handled correctly

            final ThreadPool.TimeChangeChecker timeChangeChecker = new ThreadPool.TimeChangeChecker(100, absoluteMillis, relativeNanos);

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warning for absolute clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [2s/2000ms] on absolute clock which is above the warn threshold of [100ms]"
                )
            );

            absoluteMillis += TimeValue.timeValueSeconds(2).millis();
            timeChangeChecker.check(absoluteMillis, relativeNanos);
            mockLog.assertAllExpectationsMatched();

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warning for relative clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [3s/3000000000ns] on relative clock which is above the warn threshold of [100ms]"
                )
            );

            relativeNanos += TimeValue.timeValueSeconds(3).nanos();
            timeChangeChecker.check(absoluteMillis, relativeNanos);
            mockLog.assertAllExpectationsMatched();

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warning for absolute clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "absolute clock went backwards by [1ms/1ms] while timer thread was sleeping"
                )
            );

            absoluteMillis -= 1;
            timeChangeChecker.check(absoluteMillis, relativeNanos);
            mockLog.assertAllExpectationsMatched();

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warning for relative clock",
                    ThreadPool.class.getName(),
                    Level.ERROR,
                    "relative clock went backwards by [1nanos/1ns] while timer thread was sleeping"
                )
            );

            relativeNanos -= 1;
            try {
                timeChangeChecker.check(absoluteMillis, relativeNanos);
            } catch (AssertionError e) {
                // yeah really shouldn't happen but at least we should log the right warning
            }
            mockLog.assertAllExpectationsMatched();

        }
    }

    int factorial(int n) {
        assertCurrentMethodIsNotCalledRecursively();
        if (n <= 1) {
            return 1;
        } else {
            return n * factorial(n - 1);
        }
    }

    int factorialForked(int n, ExecutorService executor) {
        assertCurrentMethodIsNotCalledRecursively();
        if (n <= 1) {
            return 1;
        }
        return n * FutureUtils.get(executor.submit(() -> factorialForked(n - 1, executor)));
    }

    public void testAssertCurrentMethodIsNotCalledRecursively() {
        expectThrows(AssertionError.class, () -> factorial(between(2, 10)));
        assertThat(factorial(1), equalTo(1)); // is not called recursively
        assertThat(
            expectThrows(AssertionError.class, () -> factorial(between(2, 10))).getMessage(),
            equalTo("org.elasticsearch.threadpool.ThreadPoolTests#factorial is called recursively")
        );
        TestThreadPool threadPool = new TestThreadPool("test");
        assertThat(factorialForked(1, threadPool.generic()), equalTo(1));
        assertThat(factorialForked(10, threadPool.generic()), equalTo(3628800));
        assertThat(
            expectThrows(AssertionError.class, () -> factorialForked(between(2, 10), EsExecutors.DIRECT_EXECUTOR_SERVICE)).getMessage(),
            equalTo("org.elasticsearch.threadpool.ThreadPoolTests#factorialForked is called recursively")
        );
        terminate(threadPool);
    }

    public void testInheritContextOnSchedule() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch executed = new CountDownLatch(1);

        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            threadPool.getThreadContext().putHeader("foo", "bar");
            final Integer one = Integer.valueOf(1);
            threadPool.getThreadContext().putTransient("foo", one);
            threadPool.schedule(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    fail();
                }
                assertEquals(threadPool.getThreadContext().getHeader("foo"), "bar");
                assertSame(threadPool.getThreadContext().getTransient("foo"), one);
                assertNull(threadPool.getThreadContext().getHeader("bar"));
                assertNull(threadPool.getThreadContext().getTransient("bar"));
                executed.countDown();
            }, TimeValue.timeValueMillis(randomInt(100)), randomFrom(EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.generic()));
            threadPool.getThreadContext().putTransient("bar", "boom");
            threadPool.getThreadContext().putHeader("bar", "boom");
            latch.countDown();
            executed.await();
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }

    public void testSchedulerWarnLogging() throws Exception {
        final ThreadPool threadPool = new TestThreadPool(
            "test",
            Settings.builder().put(ThreadPool.SLOW_SCHEDULER_TASK_WARN_THRESHOLD_SETTING.getKey(), "10ms").build()
        );
        try (var mockLog = MockLog.capture(ThreadPool.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warning for slow task",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "execution of [slow-test-task] took [*ms] which is above the warn threshold of [10ms]"
                )
            );
            final Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    final long start = threadPool.relativeTimeInMillis();
                    try {
                        assertBusy(() -> assertThat(threadPool.relativeTimeInMillis() - start, greaterThan(10L)));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }

                @Override
                public String toString() {
                    return "slow-test-task";
                }
            };
            threadPool.schedule(runnable, TimeValue.timeValueMillis(randomLongBetween(0, 300)), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            mockLog.awaitAllExpectationsMatched();
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    public void testForceMergeThreadPoolSize() {
        final int allocatedProcessors = randomIntBetween(1, EsExecutors.allocatedProcessors(Settings.EMPTY));
        final ThreadPool threadPool = new TestThreadPool(
            "test",
            Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), allocatedProcessors).build()
        );
        try {
            final int expectedSize = Math.max(1, allocatedProcessors / 8);
            ThreadPool.Info info = threadPool.info(ThreadPool.Names.FORCE_MERGE);
            assertThat(info.getThreadPoolType(), equalTo(ThreadPool.ThreadPoolType.FIXED));
            assertThat(info.getMin(), equalTo(expectedSize));
            assertThat(info.getMax(), equalTo(expectedSize));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    public void testSearchCoordinationThreadPoolSize() {
        final int expectedSize = randomIntBetween(1, EsExecutors.allocatedProcessors(Settings.EMPTY) / 2);
        final int allocatedProcessors = Math.min(
            EsExecutors.allocatedProcessors(Settings.EMPTY),
            expectedSize * 2 - (randomIntBetween(0, 1))
        );
        final ThreadPool threadPool = new TestThreadPool(
            "test",
            Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), allocatedProcessors).build()
        );
        try {
            ThreadPool.Info info = threadPool.info(ThreadPool.Names.SEARCH_COORDINATION);
            assertThat(info.getThreadPoolType(), equalTo(ThreadPool.ThreadPoolType.FIXED));
            assertThat(info.getMin(), equalTo(expectedSize));
            assertThat(info.getMax(), equalTo(expectedSize));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    public void testGetMaxSnapshotCores() {
        int allocatedProcessors = randomIntBetween(1, 16);
        assertThat(
            getMaxSnapshotThreadPoolSize(allocatedProcessors, ByteSizeValue.ofMb(400)),
            equalTo(halfAllocatedProcessorsMaxFive(allocatedProcessors))
        );
        allocatedProcessors = randomIntBetween(1, 16);
        assertThat(
            getMaxSnapshotThreadPoolSize(allocatedProcessors, ByteSizeValue.ofMb(749)),
            equalTo(halfAllocatedProcessorsMaxFive(allocatedProcessors))
        );
        allocatedProcessors = randomIntBetween(1, 16);
        assertThat(getMaxSnapshotThreadPoolSize(allocatedProcessors, ByteSizeValue.ofMb(750)), equalTo(10));
        allocatedProcessors = randomIntBetween(1, 16);
        assertThat(getMaxSnapshotThreadPoolSize(allocatedProcessors, ByteSizeValue.ofGb(4)), equalTo(10));
    }

    public void testWriteThreadPoolUsesTaskExecutionTimeTrackingEsThreadPoolExecutor() {
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        try {
            assertThat(threadPool.executor(ThreadPool.Names.WRITE), instanceOf(TaskExecutionTimeTrackingEsThreadPoolExecutor.class));
            assertThat(threadPool.executor(ThreadPool.Names.SYSTEM_WRITE), instanceOf(TaskExecutionTimeTrackingEsThreadPoolExecutor.class));
            assertThat(
                threadPool.executor(ThreadPool.Names.SYSTEM_CRITICAL_WRITE),
                instanceOf(TaskExecutionTimeTrackingEsThreadPoolExecutor.class)
            );
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    public void testScheduledOneShotRejection() {
        final var name = "fixed-bounded";
        final var threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, name, between(1, 5), between(1, 5), randomFrom(DEFAULT, DO_NOT_TRACK))
        );

        final var future = new PlainActionFuture<Void>();
        final var latch = new CountDownLatch(1);
        try {
            blockExecution(threadPool.executor(name), latch);
            threadPool.schedule(
                ActionRunnable.run(future, () -> fail("should not execute")),
                TimeValue.timeValueMillis(between(1, 100)),
                threadPool.executor(name)
            );

            expectThrows(EsRejectedExecutionException.class, () -> FutureUtils.get(future, 10, TimeUnit.SECONDS));
        } finally {
            latch.countDown();
            assertTrue(terminate(threadPool));
        }
    }

    public void testScheduledOneShotForceExecution() {
        final var name = "fixed-bounded";
        final var threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, name, between(1, 5), between(1, 5), randomFrom(DEFAULT, DO_NOT_TRACK))
        );

        final var future = new PlainActionFuture<Void>();
        final var latch = new CountDownLatch(1);
        try {
            blockExecution(threadPool.executor(name), latch);
            threadPool.schedule(
                forceExecution(ActionRunnable.run(future, () -> {})),
                TimeValue.timeValueMillis(between(1, 100)),
                threadPool.executor(name)
            );

            Thread.yield();
            assertFalse(future.isDone());

            latch.countDown();
            FutureUtils.get(future, 10, TimeUnit.SECONDS); // shouldn't throw
        } finally {
            latch.countDown();
            assertTrue(terminate(threadPool));
        }
    }

    public void testScheduledFixedDelayRejection() {
        final var name = "fixed-bounded";
        final var threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, name, between(1, 5), between(1, 5), randomFrom(DEFAULT, DO_NOT_TRACK))
        );

        final var future = new PlainActionFuture<Void>();
        final var latch = new CountDownLatch(1);
        try {
            blockExecution(threadPool.executor(name), latch);
            threadPool.scheduleWithFixedDelay(
                ActionRunnable.wrap(future, ignored -> fail("should not execute")),
                TimeValue.timeValueMillis(between(1, 100)),
                threadPool.executor(name)
            );

            expectThrows(EsRejectedExecutionException.class, () -> FutureUtils.get(future, 10, TimeUnit.SECONDS));
        } finally {
            latch.countDown();
            assertTrue(terminate(threadPool));
        }
    }

    public void testScheduledFixedDelayForceExecution() {
        final var name = "fixed-bounded";
        final var threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, name, between(1, 5), between(1, 5), randomFrom(DEFAULT, DO_NOT_TRACK))
        );

        final var future = new PlainActionFuture<Void>();
        final var latch = new CountDownLatch(1);
        try {
            blockExecution(threadPool.executor(name), latch);

            threadPool.scheduleWithFixedDelay(
                forceExecution(ActionRunnable.run(future, Thread::yield)),
                TimeValue.timeValueMillis(between(1, 100)),
                threadPool.executor(name)
            );

            assertFalse(future.isDone());

            latch.countDown();
            FutureUtils.get(future, 10, TimeUnit.SECONDS); // shouldn't throw
        } finally {
            latch.countDown();
            assertTrue(terminate(threadPool));
        }
    }

    public void testDetailedUtilizationMetric() throws Exception {
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final BuiltInExecutorBuilders builtInExecutorBuilders = new DefaultBuiltInExecutorBuilders();

        final ThreadPool threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(),
            meterRegistry,
            builtInExecutorBuilders
        );
        try {
            // write thread pool is tracked
            final String threadPoolName = ThreadPool.Names.WRITE;
            final MetricAsserter metricAsserter = new MetricAsserter(meterRegistry, threadPoolName);
            final ThreadPool.Info threadPoolInfo = threadPool.info(threadPoolName);
            final TaskExecutionTimeTrackingEsThreadPoolExecutor executor = asInstanceOf(
                TaskExecutionTimeTrackingEsThreadPoolExecutor.class,
                threadPool.executor(threadPoolName)
            );

            final long beforePreviousCollectNanos = System.nanoTime();
            meterRegistry.getRecorder().collect();
            final long afterPreviousCollectNanos = System.nanoTime();
            metricAsserter.assertLatestMetricValueMatches(
                InstrumentType.DOUBLE_GAUGE,
                ThreadPool.THREAD_POOL_METRIC_NAME_UTILIZATION,
                Measurement::getDouble,
                equalTo(0.0d)
            );

            final AtomicLong minimumDurationNanos = new AtomicLong(Long.MAX_VALUE);
            final long beforeStartNanos = System.nanoTime();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            Future<?> future = executor.submit(() -> {
                long innerStartTimeNanos = System.nanoTime();
                safeSleep(100);
                safeAwait(barrier);
                minimumDurationNanos.set(System.nanoTime() - innerStartTimeNanos);
            });
            safeAwait(barrier);
            safeGet(future);
            final long maxDurationNanos = System.nanoTime() - beforeStartNanos;

            // Wait for TaskExecutionTimeTrackingEsThreadPoolExecutor#afterExecute to run
            assertBusy(() -> assertThat(executor.getTotalTaskExecutionTime(), greaterThan(0L)));

            final long beforeMetricsCollectedNanos = System.nanoTime();
            meterRegistry.getRecorder().collect();
            final long afterMetricsCollectedNanos = System.nanoTime();

            // Calculate upper bound on utilisation metric
            final long minimumPollIntervalNanos = beforeMetricsCollectedNanos - afterPreviousCollectNanos;
            final long minimumMaxExecutionTimeNanos = minimumPollIntervalNanos * threadPoolInfo.getMax();
            final double maximumUtilization = (double) maxDurationNanos / minimumMaxExecutionTimeNanos;

            // Calculate lower bound on utilisation metric
            final long maximumPollIntervalNanos = afterMetricsCollectedNanos - beforePreviousCollectNanos;
            final long maximumMaxExecutionTimeNanos = maximumPollIntervalNanos * threadPoolInfo.getMax();
            final double minimumUtilization = (double) minimumDurationNanos.get() / maximumMaxExecutionTimeNanos;

            logger.info("Utilization must be in [{}, {}]", minimumUtilization, maximumUtilization);
            Matcher<Double> matcher = allOf(greaterThan(minimumUtilization), lessThan(maximumUtilization));
            metricAsserter.assertLatestMetricValueMatches(
                InstrumentType.DOUBLE_GAUGE,
                ThreadPool.THREAD_POOL_METRIC_NAME_UTILIZATION,
                Measurement::getDouble,
                matcher
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testThreadCountMetrics() throws Exception {
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final BuiltInExecutorBuilders builtInExecutorBuilders = new DefaultBuiltInExecutorBuilders();
        final ThreadPool threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(),
            meterRegistry,
            builtInExecutorBuilders
        );
        try {
            final String threadPoolName = randomFrom(
                ThreadPool.Names.GENERIC,
                ThreadPool.Names.ANALYZE,
                ThreadPool.Names.WRITE,
                ThreadPool.Names.SEARCH
            );
            final ThreadPool.Info threadPoolInfo = threadPool.info(threadPoolName);
            final MetricAsserter metricAsserter = new MetricAsserter(meterRegistry, threadPoolName);

            meterRegistry.getRecorder().collect();
            metricAsserter.assertLatestLongValueMatches(ThreadPool.THREAD_POOL_METRIC_NAME_ACTIVE, InstrumentType.LONG_GAUGE, equalTo(0L));
            metricAsserter.assertLatestLongValueMatches(ThreadPool.THREAD_POOL_METRIC_NAME_CURRENT, InstrumentType.LONG_GAUGE, equalTo(0L));
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_COMPLETED,
                InstrumentType.LONG_ASYNC_COUNTER,
                equalTo(0L)
            );
            metricAsserter.assertLatestLongValueMatches(ThreadPool.THREAD_POOL_METRIC_NAME_LARGEST, InstrumentType.LONG_GAUGE, equalTo(0L));

            final int numThreads = randomIntBetween(1, Math.min(10, threadPoolInfo.getMax()));
            final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
            final List<Future<?>> futures = new ArrayList<>();
            final EsThreadPoolExecutor executor = asInstanceOf(EsThreadPoolExecutor.class, threadPool.executor(threadPoolName));
            for (int i = 0; i < numThreads; i++) {
                futures.add(executor.submit(() -> {
                    safeAwait(barrier);
                    safeAwait(barrier);
                }));
            }
            // Wait for all threads to start
            safeAwait(barrier);

            meterRegistry.getRecorder().collect();
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_ACTIVE,
                InstrumentType.LONG_GAUGE,
                equalTo((long) numThreads)
            );
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_CURRENT,
                InstrumentType.LONG_GAUGE,
                equalTo((long) numThreads)
            );
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_COMPLETED,
                InstrumentType.LONG_ASYNC_COUNTER,
                equalTo(0L)
            );
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_LARGEST,
                InstrumentType.LONG_GAUGE,
                equalTo((long) numThreads)
            );

            // Let all threads complete
            safeAwait(barrier);
            futures.forEach(ESTestCase::safeGet);
            // Wait for TaskExecutionTimeTrackingEsThreadPoolExecutor#afterExecute to complete
            assertBusy(() -> assertThat(executor.getActiveCount(), equalTo(0)));

            meterRegistry.getRecorder().collect();
            metricAsserter.assertLatestLongValueMatches(ThreadPool.THREAD_POOL_METRIC_NAME_ACTIVE, InstrumentType.LONG_GAUGE, equalTo(0L));
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_CURRENT,
                InstrumentType.LONG_GAUGE,
                equalTo((long) numThreads)
            );
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_COMPLETED,
                InstrumentType.LONG_ASYNC_COUNTER,
                equalTo((long) numThreads)
            );
            metricAsserter.assertLatestLongValueMatches(
                ThreadPool.THREAD_POOL_METRIC_NAME_LARGEST,
                InstrumentType.LONG_GAUGE,
                equalTo((long) numThreads)
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static class MetricAsserter {
        private final RecordingMeterRegistry meterRegistry;
        private final String threadPoolName;

        MetricAsserter(RecordingMeterRegistry meterRegistry, String threadPoolName) {
            this.meterRegistry = meterRegistry;
            this.threadPoolName = threadPoolName;
        }

        void assertLatestLongValueMatches(String metricName, InstrumentType instrumentType, Matcher<Long> matcher) {
            assertLatestMetricValueMatches(instrumentType, metricName, Measurement::getLong, matcher);
        }

        <T> void assertLatestMetricValueMatches(
            InstrumentType instrumentType,
            String name,
            Function<Measurement, T> valueExtractor,
            Matcher<T> matcher
        ) {
            List<Measurement> measurements = meterRegistry.getRecorder()
                .getMeasurements(instrumentType, ThreadPool.THREAD_POOL_METRIC_PREFIX + threadPoolName + name);
            assertFalse(name + " has no measurements", measurements.isEmpty());
            assertThat(valueExtractor.apply(measurements.getLast()), matcher);
        }
    }

    private static AbstractRunnable forceExecution(AbstractRunnable delegate) {
        return new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }

            @Override
            protected void doRun() {
                delegate.run();
            }

            @Override
            public void onRejection(Exception e) {
                delegate.onRejection(e);
            }

            @Override
            public void onAfter() {
                delegate.onAfter();
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        };
    }

    private static void blockExecution(ExecutorService executor, CountDownLatch latch) {
        while (true) {
            try {
                executor.execute(() -> safeAwait(latch));
            } catch (EsRejectedExecutionException e) {
                break;
            }
        }
    }

}
