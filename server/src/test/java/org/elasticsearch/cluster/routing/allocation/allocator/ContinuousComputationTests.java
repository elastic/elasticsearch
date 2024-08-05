/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

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

        final int threads = between(1, 5);
        final int[] valuePerThread = new int[threads];
        startInParallel(threads, threadIndex -> {
            for (int j = 1000; j >= 0; j--) {
                computation.onNewInput(valuePerThread[threadIndex] = valuePerThread[threadIndex] + j);
            }
        });

        assertBusy(() -> assertFalse(computation.isActive()));

        assertTrue(Arrays.toString(valuePerThread) + " vs " + result.get(), Arrays.stream(valuePerThread).anyMatch(i -> i == result.get()));
    }

    public void testSkipsObsoleteValues() throws Exception {
        final var barrier = new CyclicBarrier(2);
        final Runnable await = () -> safeAwait(barrier);

        final var initialInput = new Object();
        final var becomesStaleInput = new Object();
        final var skippedInput = new Object();
        final var finalInput = new Object();

        final var result = new AtomicReference<Object>();
        final var computation = new ContinuousComputation<Object>(threadPool.generic()) {
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

    public void testFailureHandling() {
        final var input1 = new Object();
        final var input2 = new Object();

        final var successCount = new AtomicInteger();
        final var failureCount = new AtomicInteger();

        final var computation = new ContinuousComputation<>(r -> {
            try {
                r.run();
                successCount.incrementAndGet();
            } catch (AssertionError e) {
                assertEquals("simulated", asInstanceOf(RuntimeException.class, e.getCause()).getMessage());
                failureCount.incrementAndGet();
            }
        }) {
            @Override
            protected void processInput(Object input) {
                if (input == input1) {
                    onNewInput(input2);
                    throw new RuntimeException("simulated");
                }
            }

            @Override
            public String toString() {
                return "test computation";
            }
        };

        MockLog.assertThatLogger(
            () -> computation.onNewInput(input1),
            ContinuousComputation.class,
            new MockLog.SeenEventExpectation(
                "error log",
                ContinuousComputation.class.getCanonicalName(),
                Level.ERROR,
                "unexpected error processing [test computation]"
            )
        );

        // check that both inputs were processed
        assertEquals(1, failureCount.get());
        assertEquals(1, successCount.get());

        // check that the computation still accepts and processes new inputs
        computation.onNewInput(input2);
        assertEquals(1, failureCount.get());
        assertEquals(2, successCount.get());

        computation.onNewInput(input1);
        assertEquals(2, failureCount.get());
        assertEquals(3, successCount.get());
    }
}
