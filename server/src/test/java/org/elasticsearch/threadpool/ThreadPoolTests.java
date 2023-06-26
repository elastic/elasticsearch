/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.LATE_TIME_INTERVAL_WARN_THRESHOLD_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.assertCurrentMethodIsNotCalledRecursively;
import static org.elasticsearch.threadpool.ThreadPool.getMaxSnapshotThreadPoolSize;
import static org.elasticsearch.threadpool.ThreadPool.halfAllocatedProcessorsMaxFive;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

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
        final Logger threadPoolLogger = LogManager.getLogger(ThreadPool.class);
        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        try {
            Loggers.addAppender(threadPoolLogger, appender);
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected warning for absolute clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [*] on absolute clock which is above the warn threshold of [100ms]"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected warning for relative clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [*] on relative clock which is above the warn threshold of [100ms]"
                )
            );

            final ThreadPool.CachedTimeThread thread = new ThreadPool.CachedTimeThread("[timer]", 200, 100);
            thread.start();

            assertBusy(appender::assertAllExpectationsMatched);

            thread.interrupt();
            thread.join();
        } finally {
            Loggers.removeAppender(threadPoolLogger, appender);
            appender.stop();
        }
    }

    public void testTimeChangeChecker() throws Exception {
        final Logger threadPoolLogger = LogManager.getLogger(ThreadPool.class);
        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        try {
            Loggers.addAppender(threadPoolLogger, appender);

            long absoluteMillis = randomLong(); // overflow should still be handled correctly
            long relativeNanos = randomLong(); // overflow should still be handled correctly

            final ThreadPool.TimeChangeChecker timeChangeChecker = new ThreadPool.TimeChangeChecker(100, absoluteMillis, relativeNanos);

            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected warning for absolute clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [2s/2000ms] on absolute clock which is above the warn threshold of [100ms]"
                )
            );

            absoluteMillis += TimeValue.timeValueSeconds(2).millis();
            timeChangeChecker.check(absoluteMillis, relativeNanos);
            appender.assertAllExpectationsMatched();

            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected warning for relative clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "timer thread slept for [3s/3000000000ns] on relative clock which is above the warn threshold of [100ms]"
                )
            );

            relativeNanos += TimeValue.timeValueSeconds(3).nanos();
            timeChangeChecker.check(absoluteMillis, relativeNanos);
            appender.assertAllExpectationsMatched();

            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected warning for absolute clock",
                    ThreadPool.class.getName(),
                    Level.WARN,
                    "absolute clock went backwards by [1ms/1ms] while timer thread was sleeping"
                )
            );

            absoluteMillis -= 1;
            timeChangeChecker.check(absoluteMillis, relativeNanos);
            appender.assertAllExpectationsMatched();

            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
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
            appender.assertAllExpectationsMatched();

        } finally {
            Loggers.removeAppender(threadPoolLogger, appender);
            appender.stop();
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
            }, TimeValue.timeValueMillis(randomInt(100)), randomFrom(ThreadPool.Names.SAME, ThreadPool.Names.GENERIC));
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
        final Logger logger = LogManager.getLogger(ThreadPool.class);
        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        try {
            Loggers.addAppender(logger, appender);
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
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
            threadPool.schedule(runnable, TimeValue.timeValueMillis(randomLongBetween(0, 300)), ThreadPool.Names.SAME);
            assertBusy(appender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
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
}
