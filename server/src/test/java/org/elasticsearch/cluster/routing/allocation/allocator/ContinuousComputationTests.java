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
        final var computation = new ContinuousComputation<Integer>(threadPool.generic()) {

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

        final var result = new AtomicReference<Integer>();
        final var computation = new ContinuousComputation<Integer>(threadPool.generic()) {
            @Override
            protected void processInput(Integer input) {
                await.run();
                result.set(input);
                await.run();
            }
        };

        computation.onNewInput(1);
        await.run();
        assertTrue(computation.isActive());
        await.run();
        assertThat(result.get(), equalTo(1));
        assertBusy(() -> assertFalse(computation.isActive()));

        computation.onNewInput(2); // triggers a computation
        await.run();
        assertTrue(computation.isActive());

        computation.onNewInput(3); // obsoleted by computation 4 before computation 2 is finished, so skipped
        computation.onNewInput(4); // triggers a computation once 2 is finished

        await.run();
        assertThat(result.get(), equalTo(2));
        assertTrue(computation.isActive());

        await.run();
        assertTrue(computation.isActive());
        await.run();
        assertThat(result.get(), equalTo(4));
        assertBusy(() -> assertFalse(computation.isActive()));
    }
}
