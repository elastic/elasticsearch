/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.process.AbstractInitializableRunnable;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.inference.pytorch.PriorityProcessWorkerExecutorService.RequestPriority;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class PriorityProcessWorkerExecutorServiceTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool("PriorityProcessWorkerExecutorServiceTests");

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testQueueCapacityReached() {
        var executor = createProcessWorkerExecutorService(2);

        var counter = new AtomicInteger();
        var r1 = new RunOrderValidator(1, counter);
        executor.executeWithPriority(r1, RequestPriority.NORMAL, 100L);
        var r2 = new RunOrderValidator(2, counter);
        executor.executeWithPriority(r2, RequestPriority.NORMAL, 101L);
        var r3 = new RunOrderValidator(3, counter);
        executor.executeWithPriority(r3, RequestPriority.NORMAL, 101L);

        assertTrue(r1.initialized);
        assertTrue(r2.initialized);
        assertTrue(r3.initialized);
        assertTrue(r3.hasBeenRejected);
        assertFalse(r1.hasBeenRejected);
        assertFalse(r2.hasBeenRejected);
    }

    public void testQueueCapacityReached_HighestPriority() {
        var executor = createProcessWorkerExecutorService(2);

        var counter = new AtomicInteger();
        executor.executeWithPriority(new RunOrderValidator(1, counter), RequestPriority.NORMAL, 100L);
        executor.executeWithPriority(new RunOrderValidator(2, counter), RequestPriority.NORMAL, 102L);
        // queue is now full
        var r3 = new RunOrderValidator(3, counter);
        executor.executeWithPriority(r3, RequestPriority.HIGH, 103L);
        var highestPriorityAlwaysAccepted = new RunOrderValidator(4, counter);
        executor.executeWithPriority(highestPriorityAlwaysAccepted, RequestPriority.HIGHEST, 104L);
        var r5 = new RunOrderValidator(5, counter);
        executor.executeWithPriority(r5, RequestPriority.NORMAL, 105L);

        assertTrue(r3.initialized);
        assertTrue(r3.hasBeenRejected);
        assertTrue(highestPriorityAlwaysAccepted.initialized);
        assertFalse(highestPriorityAlwaysAccepted.hasBeenRejected);
        assertTrue(r5.initialized);
        assertTrue(r5.hasBeenRejected);
    }

    public void testOrderedRunnables_NormalPriority() {
        var executor = createProcessWorkerExecutorService(100);

        var counter = new AtomicInteger();

        var r1 = new RunOrderValidator(1, counter);
        executor.executeWithPriority(r1, RequestPriority.NORMAL, 100L);
        var r2 = new RunOrderValidator(2, counter);
        executor.executeWithPriority(r2, RequestPriority.NORMAL, 101L);
        var r3 = new RunOrderValidator(3, counter);
        executor.executeWithPriority(r3, RequestPriority.NORMAL, 102L);

        // final action stops the executor
        executor.executeWithPriority(new ShutdownExecutorRunnable(executor), RequestPriority.NORMAL, 10000L);

        executor.start();

        assertTrue(r1.initialized);
        assertTrue(r2.initialized);
        assertTrue(r3.initialized);

        assertTrue(r1.hasBeenRun);
        assertTrue(r2.hasBeenRun);
        assertTrue(r3.hasBeenRun);
    }

    public void testExecutorShutsDownAfterCompletingWork() {
        var executor = createProcessWorkerExecutorService(100);

        var counter = new AtomicInteger();

        var r1 = new RunOrderValidator(1, counter);
        executor.executeWithPriority(r1, RequestPriority.NORMAL, 100L);
        var r2 = new RunOrderValidator(2, counter);
        executor.executeWithPriority(r2, RequestPriority.NORMAL, 101L);
        var r3 = new RunOrderValidator(3, counter);
        executor.executeWithPriority(r3, RequestPriority.NORMAL, 102L);

        runExecutorAndAssertTermination(executor);

        assertTrue(r1.initialized);
        assertTrue(r2.initialized);
        assertTrue(r3.initialized);

        assertTrue(r1.hasBeenRun);
        assertTrue(r2.hasBeenRun);
        assertTrue(r3.hasBeenRun);
    }

    private void runExecutorAndAssertTermination(PriorityProcessWorkerExecutorService executor) {
        Future<?> executorTermination = threadPool.generic().submit(() -> {
            try {
                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                fail(Strings.format("Failed to gracefully shutdown executor: %s", e.getMessage()));
            }
        });

        executor.start();

        try {
            executorTermination.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail(Strings.format("Executor finished before it was signaled to shutdown gracefully"));
        }

        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());
    }

    public void testOrderedRunnables_MixedPriorities() {
        var executor = createProcessWorkerExecutorService(100);

        assertThat(RequestPriority.HIGH.compareTo(RequestPriority.NORMAL), lessThan(0));

        var counter = new AtomicInteger();
        long requestId = 1;
        List<RunOrderValidator> validators = new ArrayList<>();
        validators.add(new RunOrderValidator(2, counter));
        validators.add(new RunOrderValidator(3, counter));
        validators.add(new RunOrderValidator(4, counter));
        validators.add(new RunOrderValidator(1, counter));   // high priority request runs first
        validators.add(new RunOrderValidator(5, counter));
        validators.add(new RunOrderValidator(6, counter));

        executor.executeWithPriority(validators.get(0), RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(validators.get(1), RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(validators.get(2), RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(validators.get(3), RequestPriority.HIGH, requestId++);
        executor.executeWithPriority(validators.get(4), RequestPriority.NORMAL, requestId++);
        executor.executeWithPriority(validators.get(5), RequestPriority.NORMAL, requestId++);

        // final action stops the executor
        executor.executeWithPriority(new ShutdownExecutorRunnable(executor), RequestPriority.NORMAL, 10000L);

        executor.start();

        for (var validator : validators) {
            assertTrue(validator.hasBeenRun);
        }
    }

    public void testNotifyQueueRunnables_notifiesAllQueuedRunnables() throws InterruptedException {
        notifyQueueRunnables(false);
    }

    public void testNotifyQueueRunnables_notifiesAllQueuedRunnables_withError() throws InterruptedException {
        notifyQueueRunnables(true);
    }

    private void notifyQueueRunnables(boolean withError) {
        int queueSize = 10;
        var executor = createProcessWorkerExecutorService(queueSize);

        List<QueueDrainingRunnable> runnables = new ArrayList<>(queueSize);
        // First fill the queue
        for (int i = 0; i < queueSize; ++i) {
            QueueDrainingRunnable runnable = new QueueDrainingRunnable();
            runnables.add(runnable);
            executor.executeWithPriority(runnable, RequestPriority.NORMAL, i);
        }

        assertThat(executor.queueSize(), is(queueSize));

        // Set the executor to be stopped
        if (withError) {
            executor.shutdownNowWithError(new Exception());
        } else {
            executor.shutdownNow();
        }

        // Start the executor, which will cause notifyQueueRunnables() to be called immediately since the executor is already stopped
        executor.start();

        // Confirm that all the runnables were notified
        for (QueueDrainingRunnable runnable : runnables) {
            assertThat(runnable.initialized, is(true));
            assertThat(runnable.hasBeenRun, is(false));
            assertThat(runnable.hasBeenRejected, not(withError));
            assertThat(runnable.hasBeenFailed, is(withError));
        }

        assertThat(executor.queueSize(), is(0));
    }

    private PriorityProcessWorkerExecutorService createProcessWorkerExecutorService(int queueSize) {
        return new PriorityProcessWorkerExecutorService(
            threadPool.getThreadContext(),
            "PriorityProcessWorkerExecutorServiceTests",
            queueSize
        );
    }

    private static class RunOrderValidator extends AbstractInitializableRunnable {

        private boolean hasBeenRun = false;
        private boolean hasBeenRejected = false;
        private final int expectedOrder;
        private final AtomicInteger counter;
        private boolean initialized = false;

        RunOrderValidator(int expectedOrder, AtomicInteger counter) {
            this.expectedOrder = expectedOrder;
            this.counter = counter;
        }

        @Override
        public void init() {
            initialized = true;
        }

        @Override
        public void onRejection(Exception e) {
            hasBeenRejected = true;
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

    private static class ShutdownExecutorRunnable extends AbstractInitializableRunnable {

        PriorityProcessWorkerExecutorService executor;

        ShutdownExecutorRunnable(PriorityProcessWorkerExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void onFailure(Exception e) {
            executor.shutdown();
            fail(e.getMessage());
        }

        @Override
        protected void doRun() {
            executor.shutdown();
        }

        @Override
        public void init() {
            // do nothing
        }
    }

    private static class QueueDrainingRunnable extends AbstractInitializableRunnable {

        private boolean initialized = false;
        private boolean hasBeenRun = false;
        private boolean hasBeenRejected = false;
        private boolean hasBeenFailed = false;

        @Override
        public void init() {
            initialized = true;
        }

        @Override
        public void onRejection(Exception e) {
            hasBeenRejected = true;
        }

        @Override
        public void onFailure(Exception e) {
            hasBeenFailed = true;
        }

        @Override
        protected void doRun() {
            hasBeenRun = true;
        }
    }
}
