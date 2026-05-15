/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RecoveryThrottlingQueueTests extends ESTestCase {

    private static TestThreadPool threadPool;

    @BeforeClass
    public static void beforeClass() throws Exception {
        threadPool = new TestThreadPool(RecoveryThrottlingQueueTests.class.getName());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        threadPool.close();
    }

    /** Enqueued work runs asynchronously (worker thread differs from enqueueing thread). */
    public void testTasksRunOutsideEnqueueingThread() throws Exception {
        RecoveryThrottlingQueue queue = new RecoveryThrottlingQueue(threadPool, between(2, 4));
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> executionThread = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        queue.enqueue(() -> {
            executionThread.set(Thread.currentThread());
            done.countDown();
        });
        assertTrue("recovery task never finished", done.await(10, TimeUnit.SECONDS));
        assertThat("recovery task executed on enqueueing thread instead of asynchronously", executionThread.get(), not(equalTo(caller)));
    }

    /**
     * Never more than {@code maxConcurrentRecoveries} runnable bodies may overlap; extra tasks stay pending until a slot frees.
     */
    public void testMaxConcurrencyBound() throws Exception {
        final int maxConcurrentRecoveries = between(2, 5);
        RecoveryThrottlingQueue queue = new RecoveryThrottlingQueue(threadPool, maxConcurrentRecoveries);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peakConcurrent = new AtomicInteger();
        CountDownLatch maxConcurrentReached = new CountDownLatch(maxConcurrentRecoveries);
        CountDownLatch unblock = new CountDownLatch(1);
        int totalEnqueuedTasks = maxConcurrentRecoveries * 3;
        CountDownLatch allFinished = new CountDownLatch(totalEnqueuedTasks);

        Runnable recoveryTask = () -> {
            try {
                int current = running.incrementAndGet();
                peakConcurrent.accumulateAndGet(current, Integer::max);
                maxConcurrentReached.countDown();
                unblock.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                running.decrementAndGet();
                allFinished.countDown();
            }
        };

        int enqueued = 0;
        for (; enqueued < maxConcurrentRecoveries; enqueued++) {
            queue.enqueue(recoveryTask);
        }

        assertTrue(
            "timed out waiting for expected number of concurrent tasks to execute",
            maxConcurrentReached.await(10, TimeUnit.SECONDS)
        );
        assertThat(enqueued, equalTo(maxConcurrentRecoveries));
        assertThat(running.get(), equalTo(maxConcurrentRecoveries));

        for (; enqueued < totalEnqueuedTasks; enqueued++) {
            queue.enqueue(recoveryTask);
        }

        unblock.countDown();

        assertTrue("timed out waiting for tasks to finish", allFinished.await(10, TimeUnit.SECONDS));
        assertThat(running.get(), equalTo(0));
        assertThat(peakConcurrent.get(), lessThanOrEqualTo(maxConcurrentRecoveries));
    }

    /**
     * With a single execution slot, throttled backlog runs in enqueue order ({@linkplain ConcurrentLinkedQueue FIFO} semantics).
     */
    public void testFifoWhenThrottledToOneConcurrent() throws Exception {
        RecoveryThrottlingQueue queue = new RecoveryThrottlingQueue(threadPool, 1);
        int total = between(10, 20);
        List<Integer> startOrder = new CopyOnWriteArrayList<>();
        CountDownLatch allDone = new CountDownLatch(total);

        for (int i = 0; i < total; i++) {
            final int sequence = i;
            queue.enqueue(() -> {
                startOrder.add(sequence);
                allDone.countDown();
            });
        }

        assertTrue(allDone.await(30, TimeUnit.SECONDS));
        assertThat(startOrder.size(), equalTo(total));
        for (int i = 0; i < total; i++) {
            assertThat(startOrder.get(i), equalTo(Integer.valueOf(i)));
        }
    }

    /**
     * Stress one {@link RecoveryThrottlingQueue} from many producer threads for one second: alternating
     * bursty submits (high contention on {@link RecoveryThrottlingQueue}) and idle periods (little
     * to no contention). Verify that all tasks are finished and that no more maxConcurrentRecoveries run concurrently.
     *
     * By randomizing the burst sizes we exercise different queue pressure where in some executions we will always have
     * tasks in the queue, and in other executions we manage to empty the queue of tasks during the idle periods.
     */
    public void testStressConcurrentEnqueueMaintainsBoundsFifoAndCompleteness() throws Exception {
        final int maxConcurrentRecoveries = between(1, 20);
        final int highContentionBurstSizeMin = between(1, maxConcurrentRecoveries);
        final int highContentionBurstSizeMax = between(highContentionBurstSizeMin, maxConcurrentRecoveries * 2);
        final RecoveryThrottlingQueue queue = new RecoveryThrottlingQueue(threadPool, maxConcurrentRecoveries);
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger peakRunning = new AtomicInteger();
        final AtomicInteger totalTasksEnqueued = new AtomicInteger();
        final AtomicInteger totalTasksFinished = new AtomicInteger();
        final DynamicTaskLatch taskLatch = new DynamicTaskLatch();

        final long deadlineMillis = System.currentTimeMillis() + TimeUnit.MILLISECONDS.toMillis(1000);
        final int producerThreads = between(1, 6);
        final List<Thread> threads = new ArrayList<>(producerThreads);
        for (int t = 0; t < producerThreads; t++) {
            Thread thread = new Thread(() -> {
                try {
                    long phaseClock = System.currentTimeMillis();
                    while (System.currentTimeMillis() < deadlineMillis) {
                        long elapsedPhase = System.currentTimeMillis() - phaseClock;
                        boolean highContention = (elapsedPhase / TimeUnit.MILLISECONDS.toMillis(100)) % 2 == 0;
                        if (highContention) {
                            int burst = between(highContentionBurstSizeMin, highContentionBurstSizeMax);
                            for (int b = 0; b < burst && System.currentTimeMillis() < deadlineMillis; b++) {
                                taskLatch.taskStarted();
                                totalTasksEnqueued.incrementAndGet();
                                queue.enqueue(() -> runStressTask(running, peakRunning, totalTasksFinished, taskLatch));
                            }
                        } else {
                            int sleepMs = between(15, 80);
                            Thread.sleep(sleepMs);
                            if (System.currentTimeMillis() < deadlineMillis) {
                                taskLatch.taskStarted();
                                totalTasksEnqueued.incrementAndGet();
                                queue.enqueue(() -> runStressTask(running, peakRunning, totalTasksFinished, taskLatch));
                            }
                        }
                        // Sleep between each iteration
                        Thread.sleep(between(1, 10));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }, "recovery-throttle-stress-" + "-" + t);
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        taskLatch.awaitZero();
        assertThat(totalTasksFinished.get(), equalTo(totalTasksEnqueued.get()));
        assertThat(peakRunning.get(), lessThanOrEqualTo(maxConcurrentRecoveries));
        assertThat(running.get(), equalTo(0));
    }

    private void runStressTask(AtomicInteger running, AtomicInteger peakRunning, AtomicInteger totalTasksFinished, DynamicTaskLatch latch) {
        int current = running.incrementAndGet();
        peakRunning.accumulateAndGet(current, Integer::max);
        try {
            // Sleep to make sure scheduling threads also get to execute
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            running.decrementAndGet();
            totalTasksFinished.incrementAndGet();
            latch.taskFinished();
        }
    }

    public final class DynamicTaskLatch {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition zero = lock.newCondition();
        private long count;

        public void taskStarted() {
            lock.lock();
            try {
                if (count == Long.MAX_VALUE) {
                    throw new IllegalStateException("Too many active tasks");
                }
                count++;
            } finally {
                lock.unlock();
            }
        }

        public void taskFinished() {
            lock.lock();
            try {
                if (count == 0) {
                    throw new IllegalStateException("taskFinished without taskStarted");
                }

                count--;

                if (count == 0) {
                    zero.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        public void awaitZero() throws InterruptedException {
            lock.lock();
            try {
                while (count != 0) {
                    zero.await();
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
