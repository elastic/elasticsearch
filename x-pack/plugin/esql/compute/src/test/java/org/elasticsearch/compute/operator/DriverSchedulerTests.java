/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class DriverSchedulerTests extends ESTestCase {

    public void testClearPendingTaskOnRejection() {
        DriverScheduler scheduler = new DriverScheduler();
        AtomicInteger counter = new AtomicInteger();
        var threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "test", 1, 2, "test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        CountDownLatch latch = new CountDownLatch(1);
        Executor executor = threadPool.executor("test");
        try {
            for (int i = 0; i < 10; i++) {
                try {
                    executor.execute(() -> safeAwait(latch));
                } catch (EsRejectedExecutionException e) {
                    break;
                }
            }
            scheduler.scheduleOrRunTask(executor, executor, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    counter.incrementAndGet();
                }

                @Override
                protected void doRun() {
                    counter.incrementAndGet();
                }
            });
            scheduler.runPendingTasks();
            assertThat(counter.get(), equalTo(1));
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }

    /**
     * Once the driver is completing it must be resumed on the completion executor. The driver's own executor may be
     * saturated by long-running drivers, and neither queueing behind them nor force-queueing on it is acceptable.
     */
    public void testCompletingResumesOnCompletionExecutorWhenQueueIsFull() throws Exception {
        DriverScheduler scheduler = new DriverScheduler();
        var threadPool = threadPool();
        CountDownLatch latch = new CountDownLatch(1);
        Executor executor = threadPool.executor("test");
        try {
            fillQueue(executor, latch);
            scheduler.runPendingTasks();
            RecordingTask task = new RecordingTask();
            scheduler.scheduleOrRunTask(executor, threadPool.generic(), task);
            task.awaitRun();
            assertThat(task.runs.get(), equalTo(1));
            assertThat(task.failures.get(), equalTo(0));
            assertThat(EsExecutors.executorName(task.runThread.get()), equalTo(ThreadPool.Names.GENERIC));
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }

    /**
     * A task already waiting in the driver's queue must not delay cancellation: it is taken over by the completion executor,
     * and the queued entry becomes a no-op when it eventually reaches the front.
     */
    public void testQueuedTaskIsTakenOverByCompletionExecutor() throws Exception {
        DriverScheduler scheduler = new DriverScheduler();
        var threadPool = threadPool();
        CountDownLatch latch = new CountDownLatch(1);
        Executor executor = threadPool.executor("test");
        try {
            executor.execute(() -> safeAwait(latch)); // occupy the only thread; the task below waits in the queue
            RecordingTask task = new RecordingTask();
            scheduler.scheduleOrRunTask(executor, threadPool.generic(), task);
            assertThat(task.runs.get(), equalTo(0));
            scheduler.runPendingTasks();
            task.awaitRun();
            assertThat(EsExecutors.executorName(task.runThread.get()), equalTo(ThreadPool.Names.GENERIC));
            latch.countDown();
            // Drain the driver's executor: the queued entry must not run the task a second time.
            PlainActionFuture<Void> drained = new PlainActionFuture<>();
            executor.execute(() -> drained.onResponse(null));
            drained.actionGet(10, TimeUnit.SECONDS);
            assertThat(task.runs.get(), equalTo(1));
            assertThat(task.failures.get(), equalTo(0));
        } finally {
            latch.countDown();
            terminate(threadPool);
        }
    }

    /**
     * A cancellation can land after the scheduler decided not to force the submission but before the executor rejects it.
     * The driver must still finish instead of failing with a rejection.
     */
    public void testRejectionRacingWithCompletionDoesNotFailTask() throws Exception {
        DriverScheduler scheduler = new DriverScheduler();
        var threadPool = threadPool();
        try {
            RecordingTask task = new RecordingTask();
            Executor rejecting = command -> {
                scheduler.runPendingTasks();
                ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("full", false));
            };
            scheduler.scheduleOrRunTask(rejecting, threadPool.generic(), task);
            task.awaitRun();
            assertThat(task.runs.get(), equalTo(1));
            assertThat(task.failures.get(), equalTo(0));
            assertThat(EsExecutors.executorName(task.runThread.get()), equalTo(ThreadPool.Names.GENERIC));
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * If the completion executor itself rejects, which only happens at shutdown, the driver finishes on the calling thread
     * instead of being failed and never closing its operators.
     */
    public void testRejectedByCompletionExecutorRunsTask() {
        DriverScheduler scheduler = new DriverScheduler();
        Executor shutDown = command -> ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("shut down", true));
        RecordingTask task = new RecordingTask();
        scheduler.runPendingTasks();
        scheduler.scheduleOrRunTask(command -> fail("driver executor must not be used once completing"), shutDown, task);
        assertThat(task.runs.get(), equalTo(1));
        assertThat(task.failures.get(), equalTo(0));
        assertThat(task.runThread.get(), equalTo(Thread.currentThread()));
    }

    /**
     * When the driver's executor rejects a driver that is not completing, the driver is failed on the completion executor:
     * failing it closes its operators, and the rejecting thread may be a transport worker.
     */
    public void testRejectionFailsTaskOnCompletionExecutor() throws Exception {
        DriverScheduler scheduler = new DriverScheduler();
        var threadPool = threadPool();
        try {
            RecordingTask task = new RecordingTask();
            Executor rejecting = command -> ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("full", false));
            scheduler.scheduleOrRunTask(rejecting, threadPool.generic(), task);
            task.awaitRun();
            assertThat(task.runs.get(), equalTo(0));
            assertThat(task.failures.get(), equalTo(1));
            assertThat(EsExecutors.executorName(task.runThread.get()), equalTo(ThreadPool.Names.GENERIC));
        } finally {
            terminate(threadPool);
        }
    }

    private static class RecordingTask extends AbstractRunnable {
        final AtomicInteger runs = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        final AtomicReference<Thread> runThread = new AtomicReference<>();
        private final CountDownLatch ran = new CountDownLatch(1);

        @Override
        public void onFailure(Exception e) {
            runThread.set(Thread.currentThread());
            failures.incrementAndGet();
            ran.countDown();
        }

        @Override
        protected void doRun() {
            runThread.set(Thread.currentThread());
            runs.incrementAndGet();
            ran.countDown();
        }

        void awaitRun() {
            safeAwait(ran);
        }
    }

    private static void fillQueue(Executor executor, CountDownLatch latch) {
        for (int i = 0; i < 10; i++) {
            try {
                executor.execute(() -> safeAwait(latch));
            } catch (EsRejectedExecutionException e) {
                return;
            }
        }
        fail("executor queue was not filled");
    }

    private static TestThreadPool threadPool() {
        return new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "test", 1, 2, "test", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }
}
