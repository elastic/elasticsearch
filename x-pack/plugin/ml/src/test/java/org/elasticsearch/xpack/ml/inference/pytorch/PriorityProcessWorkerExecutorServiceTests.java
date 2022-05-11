/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.inference.pytorch.PriorityProcessWorkerExecutorService.RequestPriority;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class PriorityProcessWorkerExecutorServiceTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool("PriorityProcessWorkerExecutorServiceTests");

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testOrderedRunnables_NormalPriority() throws InterruptedException {
        var executor = createProcessWorkerExecutorService();

        var counter = new AtomicInteger();

        var r1 = new RunOrderValidator(1, counter);
        executor.executeWithPriority(r1, RequestPriority.NORMAL, 100L);
        var r2 = new RunOrderValidator(2, counter);
        executor.executeWithPriority(r2, RequestPriority.NORMAL, 101L);
        var r3 = new RunOrderValidator(3, counter);
        executor.executeWithPriority(r3, RequestPriority.NORMAL, 102L);

        // final action stops the executor
        executor.executeWithPriority(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                executor.shutdown();
                fail(e.getMessage());
            }

            @Override
            protected void doRun() {
                executor.shutdown();
            }
        }, RequestPriority.NORMAL, 10000L);

        executor.start();

        assertTrue(r1.hasBeenRun);
        assertTrue(r2.hasBeenRun);
        assertTrue(r3.hasBeenRun);
    }

    public void testOrderedRunnables_MixedPriorities() throws InterruptedException {
        var executor = createProcessWorkerExecutorService();

        assertThat(RequestPriority.HIGH.compareTo(RequestPriority.NORMAL), lessThan(0));

        var counter = new AtomicInteger();
        long requestId = 1;
        var r1 = new RunOrderValidator(2, counter);
        executor.executeWithPriority(r1, RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(new RunOrderValidator(3, counter), RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(new RunOrderValidator(4, counter), RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(new RunOrderValidator(1, counter), RequestPriority.HIGH, requestId++);
        executor.executeWithPriority(new RunOrderValidator(5, counter), RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(new RunOrderValidator(6, counter), RequestPriority.NORMAL, requestId++);

        // final action stops the executor
        executor.executeWithPriority(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                executor.shutdown();
                fail(e.getMessage());
            }

            @Override
            protected void doRun() {
                executor.shutdown();
            }
        }, RequestPriority.NORMAL, 10000L);

        executor.start();

        assertTrue(r1.hasBeenRun);
    }

    private PriorityProcessWorkerExecutorService createProcessWorkerExecutorService() {
        return new PriorityProcessWorkerExecutorService(threadPool.getThreadContext(), "PriorityProcessWorkerExecutorServiceTests", 100);
    }

    private static class RunOrderValidator extends AbstractRunnable {

        private boolean hasBeenRun = false;
        private final int expectedOrder;
        private final AtomicInteger counter;

        RunOrderValidator(int expectedOrder, AtomicInteger counter) {
            this.expectedOrder = expectedOrder;
            this.counter = counter;
        }

        @Override
        public void onFailure(Exception e) {
            fail(e.getMessage());
        }

        @Override
        protected void doRun() {
            hasBeenRun = true;
            assertThat(expectedOrder, equalTo(counter.incrementAndGet()));
        }
    }
}
