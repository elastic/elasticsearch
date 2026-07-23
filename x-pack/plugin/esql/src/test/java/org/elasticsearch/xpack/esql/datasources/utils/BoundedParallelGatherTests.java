/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.utils;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BoundedParallelGatherTests extends ESTestCase {

    public void testEmptyList() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            List<Integer> empty = List.of();
            List<Integer> result = BoundedParallelGather.gather(empty, item -> item * 2, 4, executor);
            assertEquals(List.of(), result);
        } finally {
            executor.shutdown();
        }
    }

    public void testSingleItemRunsInline() throws Exception {
        // Single item must execute on the calling thread (no executor dispatch).
        Thread callingThread = Thread.currentThread();
        AtomicInteger executionThread = new AtomicInteger();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            List<Integer> result = BoundedParallelGather.gather(List.of(42), item -> {
                // Record whether we are on the calling thread (0) or another thread (1).
                executionThread.set(Thread.currentThread() == callingThread ? 0 : 1);
                return item * 2;
            }, 4, executor);
            assertEquals(List.of(84), result);
            assertEquals("single item should run inline, not on executor", 0, executionThread.get());
        } finally {
            executor.shutdown();
        }
    }

    public void testResultOrderMatchesInput() throws Exception {
        int count = 50;
        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            items.add(i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Integer> results = BoundedParallelGather.gather(items, item -> item * 10, 8, executor);
            assertEquals(count, results.size());
            for (int i = 0; i < count; i++) {
                assertEquals(Integer.valueOf(i * 10), results.get(i));
            }
        } finally {
            executor.shutdown();
        }
    }

    public void testMaxConcurrencyRespected() throws Exception {
        int itemCount = 20;
        int maxConcurrency = 4;
        AtomicInteger inFlight = new AtomicInteger(0);
        AtomicInteger peakConcurrency = new AtomicInteger(0);
        // Latch to ensure all tasks start before any complete, amplifying the concurrency check.
        CountDownLatch allStarted = new CountDownLatch(maxConcurrency);
        Semaphore proceed = new Semaphore(0);

        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            items.add(i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(itemCount);
        try {
            List<Integer> results = BoundedParallelGather.gather(items, item -> {
                int current = inFlight.incrementAndGet();
                int peak = peakConcurrency.get();
                while (current > peak) {
                    if (peakConcurrency.compareAndSet(peak, current)) break;
                    peak = peakConcurrency.get();
                }
                // Simulate some work
                Thread.sleep(5);
                inFlight.decrementAndGet();
                return item;
            }, maxConcurrency, executor);

            assertEquals(itemCount, results.size());
            assertTrue(
                "peak concurrency " + peakConcurrency.get() + " should be <= maxConcurrency " + maxConcurrency,
                peakConcurrency.get() <= maxConcurrency
            );
        } finally {
            executor.shutdown();
        }
    }

    public void testFastFailOnException() throws Exception {
        int itemCount = 100;
        AtomicInteger startedCount = new AtomicInteger(0);
        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            items.add(i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            Exception thrown = expectThrows(Exception.class, () -> {
                BoundedParallelGather.gather(items, item -> {
                    startedCount.incrementAndGet();
                    if (item == 5) {
                        throw new IllegalArgumentException("test failure at item 5");
                    }
                    // Small delay so failure is detected before all items start.
                    Thread.sleep(10);
                    return item;
                }, 4, executor);
            });
            assertNotNull(thrown);
            // Not all 100 items should have started — fast-fail should have skipped some.
            assertTrue(
                "Expected fewer than " + itemCount + " items to start, but got " + startedCount.get(),
                startedCount.get() < itemCount
            );
        } finally {
            executor.shutdown();
        }
    }

    public void testFirstExceptionPropagated() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            Exception thrown = expectThrows(IllegalStateException.class, () -> {
                BoundedParallelGather.gather(List.of(1, 2, 3), item -> {
                    if (item == 1) throw new IllegalStateException("first error");
                    return item;
                }, 4, executor);
            });
            assertEquals("first error", thrown.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    public void testInvalidMaxConcurrency() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            expectThrows(IllegalArgumentException.class, () -> BoundedParallelGather.gather(List.of(1, 2), item -> item, 0, executor));
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Regression for #960: with a {@code SEARCH}-style bounded executor whose queue is far smaller than the
     * number of items, the gather must not overflow the queue. Because submission (not just execution) is
     * bounded, the executor never sees more than {@code maxConcurrency} runnables at once, so no
     * {@link EsRejectedExecutionException} is thrown and every result is still produced in order.
     */
    public void testBoundedExecutorIsNeverFlooded() throws Exception {
        int itemCount = 100;
        int maxConcurrency = 4;
        // Queue capacity == maxConcurrency, far smaller than itemCount. The old eager-submit implementation
        // would push all 100 runnables onto this queue at once and overflow it.
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            "test-no-flood",
            maxConcurrency,
            maxConcurrency,
            EsExecutors.daemonThreadFactory("test", "no-flood"),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        try {
            AtomicInteger inFlight = new AtomicInteger(0);
            AtomicInteger peakConcurrency = new AtomicInteger(0);

            List<Integer> items = new ArrayList<>();
            for (int i = 0; i < itemCount; i++) {
                items.add(i);
            }

            List<Integer> results = BoundedParallelGather.gather(items, item -> {
                int current = inFlight.incrementAndGet();
                peakConcurrency.accumulateAndGet(current, Math::max);
                Thread.sleep(1);
                inFlight.decrementAndGet();
                return item * 2;
            }, maxConcurrency, executor);

            assertEquals(itemCount, results.size());
            for (int i = 0; i < itemCount; i++) {
                assertEquals(Integer.valueOf(i * 2), results.get(i));
            }
            assertThat(
                "peak in-flight " + peakConcurrency.get() + " must never exceed maxConcurrency " + maxConcurrency,
                peakConcurrency.get(),
                lessThanOrEqualTo(maxConcurrency)
            );
        } finally {
            terminate(executor);
        }
    }

    /**
     * When the executor is already saturated by real work and rejects a running slot, the rejection must be
     * surfaced cleanly: the call fails fast by throwing the {@link EsRejectedExecutionException} (rather than
     * hanging on a latch that never completes). A pool of size 1 with a zero-length queue, whose single
     * worker is occupied by an unrelated blocking task, rejects every gather task via {@code EsAbortPolicy}.
     */
    public void testRunningSlotRejectionSurfacesCleanly() throws Exception {
        int maxConcurrency = 4;
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            "test-reject",
            1,
            0,
            EsExecutors.daemonThreadFactory("test", "reject"),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        CountDownLatch workerBusy = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            // Occupy the single worker thread for the whole gather call so every gather task is rejected.
            executor.execute(() -> {
                workerBusy.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue("blocking task did not start", workerBusy.await(10, TimeUnit.SECONDS));

            AtomicInteger startedCount = new AtomicInteger(0);
            List<Integer> items = List.of(0, 1, 2, 3);

            EsRejectedExecutionException thrown = expectThrows(
                EsRejectedExecutionException.class,
                () -> BoundedParallelGather.gather(items, item -> {
                    startedCount.incrementAndGet();
                    return item;
                }, maxConcurrency, executor)
            );
            assertNotNull(thrown);
            // Every task was rejected before it could run, and the call returned (no hang) by throwing.
            assertEquals("no task should run while the executor is saturated", 0, startedCount.get());
        } finally {
            release.countDown();
            terminate(executor);
        }
    }

    /**
     * Cancellation rides the existing fast-fail path: the gather primitive has no task knowledge, so a
     * cancelled query is modelled by a function that throws once a flag flips (exactly what
     * {@code ExternalSourceResolver}'s functions do). Once one item throws, the {@code failed} flag must
     * short-circuit the not-yet-started items and the thrown exception must propagate to the caller.
     */
    public void testCancellationShortCircuitsRemainingItems() throws Exception {
        int itemCount = 100;
        int maxConcurrency = 4;
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicInteger startedCount = new AtomicInteger(0);

        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            "test-cancel",
            maxConcurrency,
            maxConcurrency,
            EsExecutors.daemonThreadFactory("test", "cancel"),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        try {
            List<Integer> items = new ArrayList<>();
            for (int i = 0; i < itemCount; i++) {
                items.add(i);
            }

            TaskCancelledException thrown = expectThrows(TaskCancelledException.class, () -> BoundedParallelGather.gather(items, item -> {
                // Mirror ExternalSourceResolver's fns: check cancellation first, before doing any work.
                if (cancelled.get()) {
                    throw new TaskCancelledException("cancelled");
                }
                if (startedCount.incrementAndGet() == 5) {
                    cancelled.set(true); // flip mid-flight, after a few items have started
                }
                Thread.sleep(5);
                return item;
            }, maxConcurrency, executor));

            assertEquals("cancelled", thrown.getMessage());
            assertThat(
                "cancellation must stop new items from starting; started " + startedCount.get() + " of " + itemCount,
                startedCount.get(),
                lessThan(itemCount)
            );
        } finally {
            terminate(executor);
        }
    }

    /**
     * If the calling thread is interrupted while waiting for results, {@code gather} must not abandon the
     * tasks it already submitted: they are running on the executor and would keep doing work (and writing
     * into the shared results array) after the method returned. The fix marks failure (so not-yet-started
     * items short-circuit), drains the in-flight tasks, restores the interrupt status, and then fails.
     * <p>
     * The two in-flight tasks are pinned on a latch so the calling thread is forced to actually wait for
     * them: a correct implementation does not return until they settle (asserted via {@code gatherReturned}),
     * whereas the old implementation returned immediately on interrupt leaving them running.
     */
    public void testInterruptDrainsInFlightTasksBeforeFailing() throws Exception {
        int itemCount = 20;
        int maxConcurrency = 2;
        EsThreadPoolExecutor executor = EsExecutors.newFixed(
            "test-interrupt",
            maxConcurrency,
            maxConcurrency,
            EsExecutors.daemonThreadFactory("test", "interrupt"),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );

        AtomicInteger started = new AtomicInteger(0);
        AtomicInteger completed = new AtomicInteger(0);
        CountDownLatch firstTaskRunning = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interruptFlagOnReturn = new AtomicBoolean(false);
        AtomicBoolean gatherReturned = new AtomicBoolean(false);

        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            items.add(i);
        }

        Thread caller = new Thread(() -> {
            try {
                BoundedParallelGather.gather(items, item -> {
                    started.incrementAndGet();
                    firstTaskRunning.countDown();
                    // Pin the in-flight slot until the test releases it, so the calling thread is forced to
                    // wait (drain) for this task rather than abandon it when interrupted.
                    release.await();
                    completed.incrementAndGet();
                    return item;
                }, maxConcurrency, executor);
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                interruptFlagOnReturn.set(Thread.currentThread().isInterrupted());
                gatherReturned.set(true);
            }
        }, "gather-caller");

        try {
            caller.start();
            assertTrue("first task did not start", firstTaskRunning.await(10, TimeUnit.SECONDS));
            // Interrupt the calling thread while it is blocked waiting for results.
            caller.interrupt();

            // With the in-flight tasks still pinned, a correct gather must keep draining and must NOT have
            // returned yet (the buggy version returned immediately on interrupt, abandoning the tasks).
            Thread.sleep(200);
            assertFalse("gather must drain in-flight tasks before returning on interrupt", gatherReturned.get());
            assertEquals("pinned in-flight tasks must not have completed yet", 0, completed.get());

            // Release the pinned tasks so the in-flight ones finish and the runner drains the rest.
            release.countDown();
            caller.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("gather caller thread should have finished", caller.isAlive());

            assertNotNull("gather must fail when the calling thread is interrupted", thrown.get());
            assertTrue("interrupt status must be restored on the calling thread", interruptFlagOnReturn.get());
            // Drain contract: every task that started also settled before gather returned — none was abandoned.
            assertEquals("all started tasks must settle before gather returns", started.get(), completed.get());
            assertThat("fast-fail must short-circuit some not-yet-started items", started.get(), lessThan(itemCount));
        } finally {
            release.countDown();
            terminate(executor);
        }
    }
}
