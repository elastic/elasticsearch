/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AbstractThrottledTaskRunnerTests extends ESTestCase {

    private static final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory("test");
    private static final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    private ExecutorService executor;
    private int maxThreads;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        maxThreads = between(1, 10);
        executor = EsExecutors.newScaling("test", maxThreads, maxThreads, 0, TimeUnit.MILLISECONDS, false, threadFactory, threadContext);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(executor);
    }

    public void testMultiThreadedEnqueue() throws Exception {
        final int maxTasks = randomIntBetween(1, 2 * maxThreads);
        final var permits = new Semaphore(maxTasks);
        final int totalTasks = randomIntBetween(2 * maxTasks, 10 * maxTasks);
        final var latch = new CountDownLatch(totalTasks);

        class TestTask implements ActionListener<Releasable> {

            private final ExecutorService taskExecutor = randomFrom(executor, EsExecutors.DIRECT_EXECUTOR_SERVICE);

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                assertTrue(permits.tryAcquire());
                try {
                    Thread.sleep(between(0, 10));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                taskExecutor.execute(() -> {
                    permits.release();
                    releasable.close();
                    latch.countDown();
                });
            }
        }

        final BlockingQueue<TestTask> queue = ConcurrentCollections.newBlockingQueue();
        final AbstractThrottledTaskRunner<TestTask> taskRunner = new AbstractThrottledTaskRunner<>("test", maxTasks, executor, queue);

        final var threadBlocker = new CyclicBarrier(totalTasks);
        for (int i = 0; i < totalTasks; i++) {
            new Thread(() -> {
                safeAwait(threadBlocker);
                taskRunner.enqueueTask(new TestTask());
                assertThat(taskRunner.runningTasks(), lessThanOrEqualTo(maxTasks));
            }).start();
        }
        // Eventually all tasks are executed
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(queue.isEmpty());
        assertTrue(permits.tryAcquire(maxTasks));
        assertNoRunningTasks(taskRunner);
    }

    public void testEnqueueSpawnsNewTasksUpToMax() throws Exception {
        int maxTasks = randomIntBetween(1, maxThreads);
        final int enqueued = maxTasks - 1; // So that it is possible to run at least one more task
        final int newTasks = randomIntBetween(1, 10);

        CountDownLatch taskBlocker = new CountDownLatch(1);
        CountDownLatch executedCountDown = new CountDownLatch(enqueued + newTasks);

        class TestTask implements ActionListener<Releasable> {

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try {
                    taskBlocker.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    executedCountDown.countDown();
                    releasable.close();
                }
            }
        }

        final BlockingQueue<TestTask> queue = ConcurrentCollections.newBlockingQueue();
        final AbstractThrottledTaskRunner<TestTask> taskRunner = new AbstractThrottledTaskRunner<>("test", maxTasks, executor, queue);
        for (int i = 0; i < enqueued; i++) {
            taskRunner.enqueueTask(new TestTask());
            assertThat(taskRunner.runningTasks(), equalTo(i + 1));
            assertTrue(queue.isEmpty());
        }
        // Enqueueing one or more new tasks would create only one new running task
        for (int i = 0; i < newTasks; i++) {
            taskRunner.enqueueTask(new TestTask());
            assertThat(taskRunner.runningTasks(), equalTo(maxTasks));
            assertThat(queue.size(), equalTo(i));
        }
        taskBlocker.countDown();
        /// Eventually all tasks are executed
        assertTrue(executedCountDown.await(10, TimeUnit.SECONDS));
        assertTrue(queue.isEmpty());
        assertNoRunningTasks(taskRunner);
    }

    public void testFailsTasksOnRejectionOrShutdown() throws Exception {
        final var executor = randomBoolean()
            ? EsExecutors.newScaling("test", maxThreads, maxThreads, 0, TimeUnit.MILLISECONDS, true, threadFactory, threadContext)
            : EsExecutors.newFixed("test", maxThreads, between(1, 5), threadFactory, threadContext, TaskTrackingConfig.DO_NOT_TRACK);

        final var totalPermits = between(1, maxThreads * 2);
        final var permits = new Semaphore(totalPermits);
        final var taskCompleted = new CountDownLatch(between(1, maxThreads * 2));
        final var rejectionCountDown = new CountDownLatch(between(1, maxThreads * 2));

        class TestTask implements ActionListener<Releasable> {

            @Override
            public void onFailure(Exception e) {
                rejectionCountDown.countDown();
                permits.release();
            }

            @Override
            public void onResponse(Releasable releasable) {
                permits.release();
                taskCompleted.countDown();
                releasable.close();
            }
        }

        final BlockingQueue<TestTask> queue = ConcurrentCollections.newBlockingQueue();
        final AbstractThrottledTaskRunner<TestTask> taskRunner = new AbstractThrottledTaskRunner<>(
            "test",
            between(1, maxThreads * 2),
            executor,
            queue
        );

        final var spawnThread = new Thread(() -> {
            try {
                while (true) {
                    assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
                    taskRunner.enqueueTask(new TestTask());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        spawnThread.start();
        assertTrue(taskCompleted.await(10, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(rejectionCountDown.await(10, TimeUnit.SECONDS));
        spawnThread.interrupt();
        spawnThread.join();
        assertThat(taskRunner.runningTasks(), equalTo(0));
        assertTrue(queue.isEmpty());
        assertTrue(permits.tryAcquire(totalPermits));
    }

    private void assertNoRunningTasks(AbstractThrottledTaskRunner<?> taskRunner) {
        final var barrier = new CyclicBarrier(maxThreads + 1);
        for (int i = 0; i < maxThreads; i++) {
            executor.execute(() -> safeAwait(barrier));
        }
        safeAwait(barrier);
        assertThat(taskRunner.runningTasks(), equalTo(0));
    }

}
