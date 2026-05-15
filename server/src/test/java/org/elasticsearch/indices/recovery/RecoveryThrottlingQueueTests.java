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

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

        RecoveryThrottlingQueue.RecoveryTask recoveryTask = () -> {
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
}
