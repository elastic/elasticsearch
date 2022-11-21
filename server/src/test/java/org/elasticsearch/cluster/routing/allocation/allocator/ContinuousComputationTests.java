/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;

public class ContinuousComputationTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool("test");
    }

    @AfterClass
    public static void terminateThreadPool() {
        try {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        } finally {
            threadPool = null;
        }
    }

    public void testConcurrency() throws Exception {

        final var result = new AtomicReference<Integer>();
        final var computation = new ContinuousComputation<Integer>(threadPool) {

            public final Semaphore executePermit = new Semaphore(1);

            @Override
            protected void processInput(Integer input) {
                assertTrue(executePermit.tryAcquire(1));
                result.set(input);
                executePermit.release();
            }
        };

        final Thread[] threads = new Thread[between(1, 5)];
        final int[] valuePerThread = new int[threads.length];
        final CountDownLatch startLatch = new CountDownLatch(1);
        for (int i = 0; i < threads.length; i++) {
            final int threadIndex = i;
            valuePerThread[threadIndex] = randomInt();
            threads[threadIndex] = new Thread(() -> {
                try {
                    assertTrue(startLatch.await(10, TimeUnit.SECONDS));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                for (int j = 1000; j >= 0; j--) {
                    computation.onNewInput(valuePerThread[threadIndex] = valuePerThread[threadIndex] + j);
                }
            }, "submit-thread-" + threadIndex);
            threads[threadIndex].start();
        }

        startLatch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        assertBusy(() -> assertFalse(computation.isActive()));

        assertTrue(Arrays.toString(valuePerThread) + " vs " + result.get(), Arrays.stream(valuePerThread).anyMatch(i -> i == result.get()));
    }

    public void testSkipsObsoleteValues() throws Exception {
        final var barrier = new CyclicBarrier(2);
        final Runnable await = () -> {
            try {
                barrier.await(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        final var initialInput = new Object();
        final var becomesStaleInput = new Object();
        final var skippedInput = new Object();
        final var finalInput = new Object();

        final var result = new AtomicReference<Object>();
        final var computation = new ContinuousComputation<Object>(threadPool) {
            @Override
            protected void processInput(Object input) {
                assertNotEquals(input, skippedInput);
                await.run();
                result.set(input);
                await.run();
                // becomesStaleInput should have become stale by now, but other inputs should remain fresh
                assertEquals(isFresh(input), input != becomesStaleInput);
                await.run();
            }
        };

        computation.onNewInput(initialInput);
        await.run();
        assertTrue(computation.isActive());
        await.run();
        assertThat(result.get(), sameInstance(initialInput));
        await.run();
        assertBusy(() -> assertFalse(computation.isActive()));

        computation.onNewInput(becomesStaleInput); // triggers a computation
        await.run();
        assertTrue(computation.isActive());

        computation.onNewInput(skippedInput); // obsoleted by computation 4 before computation 2 is finished, so skipped
        computation.onNewInput(finalInput); // triggers a computation once 2 is finished

        await.run();
        await.run();
        assertThat(result.get(), equalTo(becomesStaleInput));
        assertTrue(computation.isActive());

        await.run();
        assertTrue(computation.isActive());
        await.run();
        assertThat(result.get(), equalTo(finalInput));
        await.run();
        assertBusy(() -> assertFalse(computation.isActive()));
    }
}
