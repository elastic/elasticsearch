/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class EsThreadPoolExecutorHotThreadsTests extends ESTestCase {

    private EsThreadPoolExecutor executor;

    public void tearDown() throws Exception {
        super.tearDown();
        if (executor != null) {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }
    }

    public void testEsThreadPoolExecutor() throws Exception {
        final long startTime = randomLongBetween(0, 1000);
        final var timer = new AtomicLong(startTime);
        final String threadName = randomIdentifier();
        final int maxThreads = between(1, 5);
        final var sizeThreshold = between(10, 100);
        final TimeValue durationThreshold = randomTimeValue(1, 5, TimeUnit.MINUTES);
        final TimeValue interval = randomTimeValue(30, 60, TimeUnit.MINUTES);

        executor = new EsThreadPoolExecutor(
            threadName,
            1,
            maxThreads,
            5,
            TimeUnit.MINUTES,
            new EsExecutors.ExecutorScalingQueue<>(),
            EsExecutors.daemonThreadFactory(randomIdentifier(), threadName),
            new EsExecutors.ForceQueuePolicy(false, false),
            new ThreadContext(Settings.EMPTY),
            new EsExecutors.HotThreadsOnLargeQueueConfig(sizeThreshold, durationThreshold.millis(), interval.millis()),
            timer::get
        );
        assertThat(executor.getStartTimeMillisOfLargeQueue(), equalTo(-1L));

        final var executingLatch = new CountDownLatch(maxThreads);
        final var continueLatch = new CountDownLatch(1);

        final Runnable blockingTask = () -> {
            executingLatch.countDown();
            safeAwait(continueLatch);
        };

        // Activate all available threads
        for (int i = 0; i < maxThreads; i++) {
            runTask(executor, blockingTask);
        }
        safeAwait(executingLatch);
        assertThat(executor.getActiveCount(), equalTo(maxThreads));

        try {
            // 1. Fill the queue up to the threshold - No logging but tracking should start due to queue size reaching the threshold
            assertThatLogger(() -> {
                for (int i = 0; i < sizeThreshold; i++) {
                    runTask(executor, () -> {});
                }
                assertThat(executor.getQueue().size(), equalTo(sizeThreshold));
                assertThat(executor.getStartTimeMillisOfLargeQueue(), equalTo(timer.get()));
            },
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );

            // 2. Queue more tasks but should not see logging yet since duration threshold not met
            assertThatLogger(
                () -> IntStream.range(0, between(1, 5)).forEach(ignore -> runTask(executor, () -> {})),
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );

            // 3. Advance time and we should observe logging when adding more task
            final long elapsedMillis1 = durationThreshold.millis() + randomLongBetween(0, 1000);
            timer.addAndGet(elapsedMillis1);
            assertThatLogger(
                () -> runTask(executor, () -> {}),
                EsThreadPoolExecutor.class,
                new MockLog.SeenEventExpectation(
                    "should log starting hot threads",
                    EsThreadPoolExecutor.class.getCanonicalName(),
                    Level.INFO,
                    "start logging hot-threads for large queue size [" + executor.getQueue().size() + "] on [" + threadName + "] executor"
                ),
                new MockLog.SeenEventExpectation(
                    "should log hot threads",
                    EsThreadPoolExecutor.class.getCanonicalName(),
                    Level.INFO,
                    "ThreadPoolExecutor ["
                        + threadName
                        + "] queue size ["
                        + executor.getQueue().size()
                        + "] has been over threshold for ["
                        + TimeValue.timeValueMillis(elapsedMillis1)
                        + "]*"
                )
            );

            // 4. Add more task and there should be no more logging since logging interval has not passed yet
            assertThatLogger(
                () -> IntStream.range(0, between(1, 5)).forEach(ignore -> runTask(executor, () -> {})),
                EsThreadPoolExecutor.class,
                new MockLog.UnseenEventExpectation("should not log", EsThreadPoolExecutor.class.getCanonicalName(), Level.INFO, "*")
            );

            // 5. Advance time to pass the logging interval and we should observe logging again when adding more task
            final long elapsedMillis2 = interval.millis() + randomLongBetween(0, 1000);
            timer.addAndGet(elapsedMillis2);
            assertThatLogger(
                () -> runTask(executor, () -> {}),
                EsThreadPoolExecutor.class,
                new MockLog.SeenEventExpectation(
                    "should log starting hot threads",
                    EsThreadPoolExecutor.class.getCanonicalName(),
                    Level.INFO,
                    "start logging hot-threads for large queue size [" + executor.getQueue().size() + "] on [" + threadName + "] executor"
                ),
                new MockLog.SeenEventExpectation(
                    "should log hot threads",
                    EsThreadPoolExecutor.class.getCanonicalName(),
                    Level.INFO,
                    "ThreadPoolExecutor ["
                        + threadName
                        + "] queue size ["
                        + executor.getQueue().size()
                        + "] has been over threshold for ["
                        + TimeValue.timeValueMillis(elapsedMillis1 + elapsedMillis2)
                        + "]*"
                )
            );
        } finally {
            continueLatch.countDown();
        }

        // 6. Wait for the queue to drain and add one more task and the tracking should be reset
        assertBusy(() -> assertThat(executor.getQueue().size(), lessThan(sizeThreshold - 1)));
        assertThat(executor.getStartTimeMillisOfLargeQueue(), equalTo(startTime));
        runTask(executor, () -> {});
        assertThat(executor.getStartTimeMillisOfLargeQueue(), equalTo(-1L));
    }

    private void runTask(EsThreadPoolExecutor executor, Runnable task) {
        if (randomBoolean()) {
            executor.execute(task);
        } else {
            executor.submit(task);
        }
    }
}
