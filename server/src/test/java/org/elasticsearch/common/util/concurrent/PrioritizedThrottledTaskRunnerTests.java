/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PrioritizedThrottledTaskRunnerTests extends ESTestCase {

    private static final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory("test");
    private static final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    private ExecutorService executor;
    private int maxThreads;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        maxThreads = between(1, 10);
        executor = EsExecutors.newScaling("test", maxThreads, maxThreads, 0, TimeUnit.NANOSECONDS, false, threadFactory, threadContext);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        TestThreadPool.terminate(executor, 30, TimeUnit.SECONDS);
    }

    static class TestTask extends AbstractRunnable implements Comparable<TestTask> {
        private final Runnable runnable;
        private final int priority;

        TestTask(Runnable runnable, int priority) {
            this.runnable = runnable;
            this.priority = priority;
        }

        @Override
        public int compareTo(TestTask o) {
            return priority - o.priority;
        }

        @Override
        public void doRun() {
            runnable.run();
        }

        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    public void testMultiThreadedEnqueue() throws Exception {
        final int maxTasks = randomIntBetween(1, maxThreads);
        PrioritizedThrottledTaskRunner<TestTask> taskRunner = new PrioritizedThrottledTaskRunner<>("test", maxTasks, executor);
        final int enqueued = randomIntBetween(2 * maxTasks, 10 * maxTasks);
        final var threadBlocker = new CyclicBarrier(enqueued);
        final var executedCountDown = new CountDownLatch(enqueued);
        for (int i = 0; i < enqueued; i++) {
            new Thread(() -> {
                safeAwait(threadBlocker);
                taskRunner.enqueueTask(new TestTask(() -> {
                    safeSleep(randomLongBetween(0, 10));
                    executedCountDown.countDown();
                }, getRandomPriority()));
                assertThat(taskRunner.runningTasks(), lessThanOrEqualTo(maxTasks));
            }).start();
        }
        // Eventually all tasks are executed
        safeAwait(executedCountDown);
        assertThat(taskRunner.queueSize(), equalTo(0));
        assertNoRunningTasks(taskRunner);
    }

    public void testTasksRunInOrder() throws Exception {
        final int n = randomIntBetween(1, maxThreads);
        // To check that tasks are run in the order (based on their priority), limit max running tasks to 1
        // and wait until all tasks are enqueued.

        final var taskRunner = new PrioritizedThrottledTaskRunner<TestTask>("test", 1, executor);

        final var blockBarrier = new CyclicBarrier(2);
        taskRunner.enqueueTask(new TestTask(() -> {
            // notify main thread that the runner is blocked
            safeAwait(blockBarrier);
            // wait for main thread to finish enqueuing tasks
            safeAwait(blockBarrier);
        }, getRandomPriority()));

        blockBarrier.await(10, TimeUnit.SECONDS); // wait for blocking task to start executing

        final int enqueued = randomIntBetween(2 * n, 10 * n);
        List<Integer> taskPriorities = new ArrayList<>(enqueued);
        List<Integer> executedPriorities = new ArrayList<>(enqueued);
        final var enqueuedBarrier = new CyclicBarrier(enqueued + 1);
        final var executedCountDown = new CountDownLatch(enqueued);
        for (int i = 0; i < enqueued; i++) {
            final int priority = getRandomPriority();
            taskPriorities.add(priority);
            new Thread(() -> {
                // wait until all threads are ready so the enqueueTask() calls are as concurrent as possible
                safeAwait(enqueuedBarrier);
                taskRunner.enqueueTask(new TestTask(() -> {
                    executedPriorities.add(priority);
                    executedCountDown.countDown();
                }, priority));
                // notify main thread that the task is enqueued
                safeAwait(enqueuedBarrier);
            }).start();
        }
        // release all the threads at once
        enqueuedBarrier.await(10, TimeUnit.SECONDS);
        // wait for all threads to confirm the task is enqueued
        enqueuedBarrier.await(10, TimeUnit.SECONDS);
        assertThat(taskRunner.queueSize(), equalTo(enqueued));

        blockBarrier.await(10, TimeUnit.SECONDS); // notify blocking task that it can continue

        // Eventually all tasks are executed
        assertTrue(executedCountDown.await(10, TimeUnit.SECONDS));
        assertThat(executedPriorities.size(), equalTo(enqueued));
        assertThat(taskRunner.queueSize(), equalTo(0));
        Collections.sort(taskPriorities);
        assertThat(executedPriorities, equalTo(taskPriorities));
        assertNoRunningTasks(taskRunner);
    }

    public void testEnqueueSpawnsNewTasksUpToMax() throws Exception {
        int maxTasks = randomIntBetween(1, maxThreads);
        final int enqueued = maxTasks - 1; // So that it is possible to run at least one more task
        final int newTasks = randomIntBetween(1, 10);

        CountDownLatch taskBlocker = new CountDownLatch(1);
        CountDownLatch executedCountDown = new CountDownLatch(enqueued + newTasks);
        PrioritizedThrottledTaskRunner<TestTask> taskRunner = new PrioritizedThrottledTaskRunner<>("test", maxTasks, executor);
        for (int i = 0; i < enqueued; i++) {
            final int taskId = i;
            taskRunner.enqueueTask(new TestTask(() -> {
                try {
                    taskBlocker.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executedCountDown.countDown();
            }, getRandomPriority()));
            assertThat(taskRunner.runningTasks(), equalTo(i + 1));
        }
        // Enqueueing one or more new tasks would create only one new running task
        for (int i = 0; i < newTasks; i++) {
            final int taskId = i;
            taskRunner.enqueueTask(new TestTask(() -> {
                try {
                    taskBlocker.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executedCountDown.countDown();
            }, getRandomPriority()));
            assertThat(taskRunner.runningTasks(), equalTo(maxTasks));
        }
        assertThat(taskRunner.queueSize(), equalTo(newTasks - 1));
        taskBlocker.countDown();
        /// Eventually all tasks are executed
        assertTrue(executedCountDown.await(10, TimeUnit.SECONDS));
        assertThat(taskRunner.queueSize(), equalTo(0));
        assertNoRunningTasks(taskRunner);
    }

    public void testFailsTasksOnRejectionOrShutdown() throws Exception {
        final var executor = randomBoolean()
            ? EsExecutors.newScaling("test", maxThreads, maxThreads, 0, TimeUnit.MILLISECONDS, true, threadFactory, threadContext)
            : EsExecutors.newFixed("test", maxThreads, between(1, 5), threadFactory, threadContext, TaskTrackingConfig.DO_NOT_TRACK);
        final var taskRunner = new PrioritizedThrottledTaskRunner<TestTask>("test", between(1, maxThreads * 2), executor);
        final var totalPermits = between(1, maxThreads * 2);
        final var permits = new Semaphore(totalPermits);
        final var taskCompleted = new CountDownLatch(between(1, maxThreads * 2));
        final var rejectionCountDown = new CountDownLatch(between(1, maxThreads * 2));

        final var spawnThread = new Thread(() -> {
            try {
                while (true) {
                    assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
                    taskRunner.enqueueTask(new TestTask(taskCompleted::countDown, getRandomPriority()) {
                        @Override
                        public void onRejection(Exception e) {
                            rejectionCountDown.countDown();
                        }

                        @Override
                        public void onAfter() {
                            permits.release();
                        }
                    });
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
        assertThat(taskRunner.queueSize(), equalTo(0));
        assertTrue(permits.tryAcquire(totalPermits));
    }

    private int getRandomPriority() {
        return randomIntBetween(-1000, 1000);
    }

    private void assertNoRunningTasks(PrioritizedThrottledTaskRunner<TestTask> taskRunner) {
        final var barrier = new CyclicBarrier(maxThreads + 1);
        for (int i = 0; i < maxThreads; i++) {
            executor.execute(() -> safeAwait(barrier));
        }
        safeAwait(barrier);
        assertThat(taskRunner.runningTasks(), equalTo(0));
    }

}
