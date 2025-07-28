/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.network;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class ThreadWatchdogTests extends ESTestCase {

    public void testSimpleActivityTracking() throws InterruptedException {
        final var watchdog = new ThreadWatchdog();
        final var barrier = new CyclicBarrier(2);
        final var threadName = "watched-thread";
        final var thread = new Thread(() -> {
            final var activityTracker = watchdog.getActivityTrackerForCurrentThread();

            assertEquals(0L, activityTracker.get());
            if (randomBoolean()) {
                // ensure overflow is no problem
                activityTracker.set(Long.MAX_VALUE - randomFrom(1, 3, 5));
            }

            safeAwait(barrier);
            // step 1: thread is idle
            safeAwait(barrier);

            startActivity(activityTracker);

            safeAwait(barrier);
            // step 2: thread is active
            safeAwait(barrier);

            for (int i = between(1, 10); i > 0; i--) {
                stopActivity(activityTracker);
                startActivity(activityTracker);
            }

            safeAwait(barrier);
            // step 3: thread still active, but made progress
            safeAwait(barrier);

            stopActivity(activityTracker);

            safeAwait(barrier);
            // step 4: thread is idle again
            safeAwait(barrier);

        }, threadName);
        thread.start();

        safeAwait(barrier);

        // step 1: thread is idle
        assertEquals(List.of(), watchdog.getStuckThreadNames());
        assertEquals(List.of(), watchdog.getStuckThreadNames());

        safeAwait(barrier);
        safeAwait(barrier);

        // step 2: thread is active
        assertEquals(List.of(), watchdog.getStuckThreadNames());
        assertEquals(List.of(threadName), watchdog.getStuckThreadNames());
        assertEquals(List.of(threadName), watchdog.getStuckThreadNames()); // just to check it's still reported as stuck

        safeAwait(barrier);
        safeAwait(barrier);

        // step 3: thread still active, but made progress
        assertEquals(List.of(), watchdog.getStuckThreadNames());
        assertEquals(List.of(threadName), watchdog.getStuckThreadNames());
        assertEquals(List.of(threadName), watchdog.getStuckThreadNames()); // just to check it's still reported as stuck

        safeAwait(barrier);
        safeAwait(barrier);

        // step 4: thread is idle again
        assertEquals(List.of(), watchdog.getStuckThreadNames());
        assertEquals(List.of(), watchdog.getStuckThreadNames());

        safeAwait(barrier);

        thread.join();
    }

    public void testMultipleBlockedThreads() throws InterruptedException {
        final var threadNames = randomList(2, 10, ESTestCase::randomIdentifier);

        final var watchdog = new ThreadWatchdog();
        final var barrier = new CyclicBarrier(threadNames.size() + 1);
        final var threads = new Thread[threadNames.size()];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                safeAwait(barrier);
                final var activityTracker = watchdog.getActivityTrackerForCurrentThread();
                startActivity(activityTracker);
                safeAwait(barrier);
                // wait for main test thread
                safeAwait(barrier);
                stopActivity(activityTracker);
            }, threadNames.get(i));
            threads[i].start();
        }

        safeAwait(barrier);
        safeAwait(barrier);

        try {
            assertEquals(List.of(), watchdog.getStuckThreadNames());
            threadNames.sort(Comparator.naturalOrder()); // stuck threads are sorted by name
            assertEquals(threadNames, watchdog.getStuckThreadNames());
            assertEquals(threadNames, watchdog.getStuckThreadNames()); // just to check they're all still reported as stuck
        } finally {
            safeAwait(barrier);
            for (final var thread : threads) {
                thread.join();
            }
        }
    }

    public void testConcurrency() throws Exception {
        final var keepGoing = new AtomicBoolean(true);
        final var watchdog = new ThreadWatchdog();
        final var threads = new Thread[between(1, 5)];
        final var semaphoresByThreadName = new HashMap<String, Semaphore>();
        final var warmUpLatches = new CountDownLatch[threads.length];
        try {
            for (int i = 0; i < threads.length; i++) {
                final var threadName = "watched-thread-" + i;
                final var semaphore = new Semaphore(1);
                final var warmUpLatch = new CountDownLatch(20);
                warmUpLatches[i] = warmUpLatch;
                semaphoresByThreadName.put(threadName, semaphore);
                threads[i] = new Thread(() -> {
                    final var activityTracker = watchdog.getActivityTrackerForCurrentThread();
                    while (keepGoing.get()) {
                        startActivity(activityTracker);
                        try {
                            safeAcquire(semaphore);
                            Thread.yield();
                            semaphore.release();
                            Thread.yield();
                        } finally {
                            stopActivity(activityTracker);
                            warmUpLatch.countDown();
                        }
                    }
                }, threadName);
                threads[i].start();
            }

            for (final var warmUpLatch : warmUpLatches) {
                safeAwait(warmUpLatch);
            }

            final var threadToBlock = randomFrom(semaphoresByThreadName.keySet());
            final var semaphore = semaphoresByThreadName.get(threadToBlock);
            safeAcquire(semaphore);
            assertBusy(() -> assertThat(watchdog.getStuckThreadNames(), hasItem(threadToBlock)));
            semaphore.release();
            assertBusy(() -> assertThat(watchdog.getStuckThreadNames(), not(hasItem(threadToBlock))));
        } finally {
            keepGoing.set(false);
            for (final var thread : threads) {
                thread.join();
            }
        }
    }

    /**
     * This logger is mentioned in the docs by name, so we cannot rename it without adjusting the docs. Thus we fix the expected logger
     * name in this string constant rather than using {@code ThreadWatchdog.class.getCanonicalName()}.
     */
    private static final String LOGGER_NAME = "org.elasticsearch.common.network.ThreadWatchdog";

    public void testLoggingAndScheduling() {
        final var watchdog = new ThreadWatchdog();
        final var activityTracker = watchdog.getActivityTrackerForCurrentThread();
        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var settings = Settings.builder();
        final var lifecycle = new Lifecycle();
        assertTrue(lifecycle.moveToStarted());

        final long checkIntervalMillis;
        if (randomBoolean()) {
            checkIntervalMillis = ThreadWatchdog.NETWORK_THREAD_WATCHDOG_INTERVAL.get(Settings.EMPTY).millis();
        } else {
            checkIntervalMillis = between(1, 100000);
            settings.put(ThreadWatchdog.NETWORK_THREAD_WATCHDOG_INTERVAL.getKey(), timeValueMillis(checkIntervalMillis));
        }

        final long quietTimeMillis;
        if (randomBoolean()) {
            quietTimeMillis = ThreadWatchdog.NETWORK_THREAD_WATCHDOG_QUIET_TIME.get(Settings.EMPTY).millis();
        } else {
            quietTimeMillis = between(1, 100000);
            settings.put(ThreadWatchdog.NETWORK_THREAD_WATCHDOG_QUIET_TIME.getKey(), timeValueMillis(quietTimeMillis));
        }

        watchdog.run(settings.build(), deterministicTaskQueue.getThreadPool(), lifecycle);

        for (int i = 0; i < 3; i++) {
            assertAdvanceTime(deterministicTaskQueue, checkIntervalMillis);
            MockLog.assertThatLogger(
                deterministicTaskQueue::runAllRunnableTasks,
                ThreadWatchdog.class,
                new MockLog.UnseenEventExpectation("no logging", LOGGER_NAME, Level.WARN, "*")
            );
        }

        startActivity(activityTracker);
        assertAdvanceTime(deterministicTaskQueue, checkIntervalMillis);
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            ThreadWatchdog.class,
            new MockLog.UnseenEventExpectation("no logging", LOGGER_NAME, Level.WARN, "*")
        );
        assertAdvanceTime(deterministicTaskQueue, checkIntervalMillis);
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            ThreadWatchdog.class,
            new MockLog.SeenEventExpectation(
                "stuck threads logging",
                LOGGER_NAME,
                Level.WARN,
                Strings.format(
                    "the following threads are active but did not make progress in the preceding [%s]: [%s]",
                    TimeValue.timeValueMillis(checkIntervalMillis),
                    Thread.currentThread().getName()
                )
            ),
            new MockLog.SeenEventExpectation(
                "thread dump",
                LOGGER_NAME,
                Level.WARN,
                "hot threads dump due to active threads not making progress (gzip compressed*base64-encoded*"
            )
        );
        assertAdvanceTime(deterministicTaskQueue, Math.max(quietTimeMillis, checkIntervalMillis));
        stopActivity(activityTracker);
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllRunnableTasks,
            ThreadWatchdog.class,
            new MockLog.UnseenEventExpectation("no logging", LOGGER_NAME, Level.WARN, "*")
        );
        assertAdvanceTime(deterministicTaskQueue, checkIntervalMillis);
        deterministicTaskQueue.scheduleNow(lifecycle::moveToStopped);
        deterministicTaskQueue.runAllTasksInTimeOrder(); // ensures that the rescheduling stops
    }

    public void testDisableWithZeroInterval() {
        final var watchdog = new ThreadWatchdog();
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var lifecycle = new Lifecycle();
        assertTrue(lifecycle.moveToStarted());

        watchdog.run(
            Settings.builder()
                .put(ThreadWatchdog.NETWORK_THREAD_WATCHDOG_INTERVAL.getKey(), randomFrom(TimeValue.ZERO, TimeValue.MINUS_ONE))
                .build(),
            deterministicTaskQueue.getThreadPool(),
            lifecycle
        );
        assertFalse(deterministicTaskQueue.hasAnyTasks());

        watchdog.run(
            Settings.builder().put(ThreadWatchdog.NETWORK_THREAD_WATCHDOG_INTERVAL.getKey(), timeValueMillis(between(1, 100000))).build(),
            deterministicTaskQueue.getThreadPool(),
            lifecycle
        );
        assertTrue(deterministicTaskQueue.hasDeferredTasks());
        lifecycle.moveToStopped();
        deterministicTaskQueue.runAllTasksInTimeOrder(); // ensures that the rescheduling stops
    }

    private static void assertAdvanceTime(DeterministicTaskQueue deterministicTaskQueue, long expectedMillis) {
        final var currentTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
        deterministicTaskQueue.advanceTime();
        assertEquals(expectedMillis, deterministicTaskQueue.getCurrentTimeMillis() - currentTimeMillis);
    }

    private static void startActivity(ThreadWatchdog.ActivityTracker activityTracker) {
        if (randomBoolean()) {
            activityTracker.startActivity();
        } else {
            assertTrue(activityTracker.maybeStartActivity());
        }
        if (randomBoolean()) {
            assertFalse(activityTracker.maybeStartActivity());
        }
    }

    private static void stopActivity(ThreadWatchdog.ActivityTracker activityTracker) {
        if (randomBoolean()) {
            assertFalse(activityTracker.maybeStartActivity());
        }
        activityTracker.stopActivity();
    }
}
