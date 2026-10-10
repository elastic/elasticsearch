/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestEsExecutors;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AbstractThrottledTaskRunnerTests extends ESTestCase {

    private static final ThreadFactory threadFactory = TestEsExecutors.testOnlyDaemonThreadFactory("test");
    private static final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    private ExecutorService executor;
    private int maxThreads;

    @Before
    public void createExecutor() throws Exception {
        maxThreads = between(1, 10);
        executor = EsExecutors.newScaling("test", maxThreads, maxThreads, 0, TimeUnit.MILLISECONDS, false, threadFactory, threadContext);
    }

    @After
    public void terminateExecutor() throws Exception {
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

    public void testEnqueueSpawnsNewTasksUpToMax() {
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
                    safeAwait(taskBlocker);
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
        safeAwait(executedCountDown);
        assertTrue(queue.isEmpty());
        assertNoRunningTasks(taskRunner);
    }

    public void testRaisingMaxRunningTasksStartsQueuedTasks() {
        final int newMax = maxThreads;
        final int totalTasks = newMax + randomIntBetween(1, 10);
        final CountDownLatch taskBlocker = new CountDownLatch(1);
        final CountDownLatch startedCountDown = new CountDownLatch(newMax);
        final CountDownLatch executedCountDown = new CountDownLatch(totalTasks);

        class TestTask implements ActionListener<Releasable> {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try {
                    startedCountDown.countDown();
                    safeAwait(taskBlocker);
                } finally {
                    executedCountDown.countDown();
                    releasable.close();
                }
            }
        }

        final BlockingQueue<TestTask> queue = ConcurrentCollections.newBlockingQueue();
        final AbstractThrottledTaskRunner<TestTask> taskRunner = new AbstractThrottledTaskRunner<>("test", 1, executor, queue);
        for (int i = 0; i < totalTasks; i++) {
            taskRunner.enqueueTask(new TestTask());
        }
        assertThat(taskRunner.runningTasks(), equalTo(1));
        assertThat(queue.size(), equalTo(totalTasks - 1));

        taskRunner.setMaxRunningTasks(newMax);
        assertThat(taskRunner.getMaxRunningTasks(), equalTo(newMax));
        // the raise starts queued tasks straight away, without waiting for a running task to finish
        assertThat(taskRunner.runningTasks(), equalTo(newMax));
        assertThat(queue.size(), equalTo(totalTasks - newMax));
        safeAwait(startedCountDown);

        taskBlocker.countDown();
        safeAwait(executedCountDown);
        assertTrue(queue.isEmpty());
        assertNoRunningTasks(taskRunner);
    }

    public void testLoweringMaxRunningTasksLimitsNewStarts() throws Exception {
        final int initialMax = maxThreads;
        final int newMax = randomIntBetween(1, initialMax);
        final int queuedTasks = newMax + randomIntBetween(1, 10);
        final CountDownLatch firstBlocker = new CountDownLatch(1);
        final CountDownLatch secondBlocker = new CountDownLatch(1);
        final CountDownLatch executedCountDown = new CountDownLatch(initialMax + queuedTasks);
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger maxActive = new AtomicInteger();

        class TestTask implements ActionListener<Releasable> {
            private final boolean first;

            TestTask(boolean first) {
                this.first = first;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try {
                    if (first) {
                        safeAwait(firstBlocker);
                    } else {
                        maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                        safeAwait(secondBlocker);
                        active.decrementAndGet();
                    }
                } finally {
                    executedCountDown.countDown();
                    releasable.close();
                }
            }
        }

        final BlockingQueue<TestTask> queue = ConcurrentCollections.newBlockingQueue();
        final AbstractThrottledTaskRunner<TestTask> taskRunner = new AbstractThrottledTaskRunner<>("test", initialMax, executor, queue);
        for (int i = 0; i < initialMax; i++) {
            taskRunner.enqueueTask(new TestTask(true));
        }
        for (int i = 0; i < queuedTasks; i++) {
            taskRunner.enqueueTask(new TestTask(false));
        }
        assertThat(taskRunner.runningTasks(), equalTo(initialMax));
        assertThat(queue.size(), equalTo(queuedTasks));

        taskRunner.setMaxRunningTasks(newMax);
        // running tasks are not interrupted
        assertThat(taskRunner.runningTasks(), equalTo(initialMax));
        assertThat(queue.size(), equalTo(queuedTasks));

        firstBlocker.countDown();
        // as the first tasks finish only newMax of the queued tasks start
        assertBusy(() -> {
            assertThat(active.get(), equalTo(newMax));
            assertThat(taskRunner.runningTasks(), equalTo(newMax));
            assertThat(queue.size(), equalTo(queuedTasks - newMax));
            // a task counts itself as active before it records the most that were
            assertThat(maxActive.get(), equalTo(newMax));
        });

        secondBlocker.countDown();
        safeAwait(executedCountDown);
        assertThat(maxActive.get(), lessThanOrEqualTo(newMax));
        assertTrue(queue.isEmpty());
        assertNoRunningTasks(taskRunner);
    }

    public void testSetMaxRunningTasksRejectsNonPositive() {
        final AbstractThrottledTaskRunner<ActionListener<Releasable>> taskRunner = new AbstractThrottledTaskRunner<>(
            "test",
            1,
            executor,
            ConcurrentCollections.newBlockingQueue()
        );
        expectThrows(IllegalArgumentException.class, () -> taskRunner.setMaxRunningTasks(randomIntBetween(Integer.MIN_VALUE, 0)));
        assertThat(taskRunner.getMaxRunningTasks(), equalTo(1));
    }

    /**
     * Permits that are given out by the test, and count how often they were asked for and how many are given back.
     */
    private static class TestPermits implements AbstractThrottledTaskRunner.StartPermits {
        final AtomicInteger available = new AtomicInteger();
        final AtomicInteger asked = new AtomicInteger();
        final AtomicInteger acquired = new AtomicInteger();
        final AtomicInteger released = new AtomicInteger();

        @Override
        public Releasable tryAcquire() {
            asked.incrementAndGet();
            while (true) {
                final int current = available.get();
                if (current == 0) {
                    return null;
                }
                if (available.compareAndSet(current, current - 1)) {
                    acquired.incrementAndGet();
                    // closing twice is a bug of the runner
                    return Releasables.assertOnce(released::incrementAndGet);
                }
            }
        }
    }

    public void testTasksStartOnlyWithAPermitAndGiveItBack() throws Exception {
        final var permits = new TestPermits();
        final var blocker = new CountDownLatch(1);
        final int totalTasks = maxThreads + randomIntBetween(1, 5);
        final var finished = new CountDownLatch(totalTasks);
        final var started = new AtomicInteger();

        class TestTask implements ActionListener<Releasable> {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try {
                    started.incrementAndGet();
                    safeAwait(blocker);
                } finally {
                    finished.countDown();
                    releasable.close();
                }
            }
        }

        final BlockingQueue<TestTask> queue = ConcurrentCollections.newBlockingQueue();
        final var taskRunner = new AbstractThrottledTaskRunner<>("test", maxThreads, executor, queue, permits);
        for (int i = 0; i < totalTasks; i++) {
            taskRunner.enqueueTask(new TestTask());
        }
        // no permit: nothing starts, and the slot the attempt took is free again
        assertThat(taskRunner.runningTasks(), equalTo(0));
        assertThat(queue.size(), equalTo(totalTasks));
        assertThat(started.get(), equalTo(0));

        // as many permits as there is room for: the runner is not asked to start more than it has room for
        final int granted = randomIntBetween(1, maxThreads);
        permits.available.set(granted);
        taskRunner.runQueuedTasks();
        assertThat(taskRunner.runningTasks(), equalTo(granted));
        assertThat(queue.size(), equalTo(totalTasks - granted));
        assertThat(permits.released.get(), equalTo(0));

        // permits that are given back are not needed to run the rest, as long as there are more
        permits.available.set(totalTasks);
        taskRunner.runQueuedTasks();
        assertThat(taskRunner.runningTasks(), equalTo(maxThreads));
        assertThat(queue.size(), equalTo(totalTasks - maxThreads));

        blocker.countDown();
        safeAwait(finished);
        assertTrue(queue.isEmpty());
        assertNoRunningTasks(taskRunner);
        // every permit taken was given back, once, including one that was taken and then not needed
        assertThat(permits.acquired.get(), greaterThanOrEqualTo(totalTasks));
        assertBusy(() -> assertThat(permits.released.get(), equalTo(permits.acquired.get())));
    }

    public void testAsksOnceMoreForAPermitAfterTheSlotIsFree() {
        // a permit is given back, and the runner asked to run its tasks, while a call holds the only slot: that call must not miss it
        final var ran = new CountDownLatch(1);
        final var asked = new AtomicInteger();
        final var taskRunner = new AbstractThrottledTaskRunner<ActionListener<Releasable>>(
            "test",
            1,
            executor,
            ConcurrentCollections.newBlockingQueue(),
            () -> asked.incrementAndGet() == 1 ? null : () -> {}
        );
        taskRunner.enqueueTask(ActionListener.wrap(releasable -> {
            ran.countDown();
            releasable.close();
        }, e -> { throw new AssertionError(e); }));
        safeAwait(ran);
        assertThat(asked.get(), equalTo(2));
    }

    public void testPermitIsNotAskedForWithoutATaskAndGivenBackWhenTheTaskIsRejected() {
        final var permits = new TestPermits();
        permits.available.set(1);
        final var failed = new AtomicInteger();
        final var taskRunner = new AbstractThrottledTaskRunner<ActionListener<Releasable>>(
            "test",
            1,
            command -> ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("test")),
            ConcurrentCollections.newBlockingQueue(),
            permits
        );
        taskRunner.runQueuedTasks();
        assertThat(permits.asked.get(), equalTo(0));

        taskRunner.enqueueTask(ActionListener.wrap(releasable -> { throw new AssertionError("rejected"); }, e -> failed.incrementAndGet()));
        assertThat(failed.get(), equalTo(1));
        assertThat(permits.released.get(), equalTo(1));
        assertThat(taskRunner.runningTasks(), equalTo(0));
    }

    public void testPermitsDoNotOverflowTheStackOfADirectExecutor() {
        final int taskCount = randomIntBetween(5_000, 10_000);
        final var counter = new AtomicInteger();
        final var permits = new TestPermits();
        permits.available.set(taskCount + 1);
        final BlockingQueue<ActionListener<Releasable>> queue = ConcurrentCollections.newBlockingQueue();
        final var taskRunner = new AbstractThrottledTaskRunner<>(
            "test",
            between(1, 10),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            queue,
            permits
        );
        final ActionListener<Releasable> task = ActionListener.wrap(releasable -> {
            counter.incrementAndGet();
            releasable.close();
        }, e -> { throw new AssertionError(e); });
        for (int i = 0; i < taskCount; i++) {
            queue.add(task);
        }
        taskRunner.enqueueTask(task);
        assertThat(counter.get(), equalTo(taskCount + 1));
        assertThat(permits.released.get(), equalTo(taskCount + 1));
    }

    public void testRunSyncTasksEagerly() {
        final int maxTasks = randomIntBetween(1, maxThreads);
        final int taskCount = between(maxTasks, maxTasks * 2);
        final var barrier = new CyclicBarrier(maxTasks + 1);
        final var executedCountDown = new CountDownLatch(taskCount);
        final var testThread = Thread.currentThread();

        class TestTask implements ActionListener<Releasable> {

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    if (Thread.currentThread() != testThread) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                    }
                } finally {
                    executedCountDown.countDown();
                }
            }
        }

        final BlockingQueue<TestTask> queue = ConcurrentCollections.newBlockingQueue();
        final AbstractThrottledTaskRunner<TestTask> taskRunner = new AbstractThrottledTaskRunner<>("test", maxTasks, executor, queue);
        for (int i = 0; i < taskCount; i++) {
            taskRunner.enqueueTask(new TestTask());
        }

        safeAwait(barrier);
        assertThat(taskRunner.runningTasks(), equalTo(maxTasks)); // maxTasks tasks are running now
        assertEquals(taskCount - maxTasks, queue.size()); // the remainder are enqueued

        final var capturedTask = new AtomicReference<Runnable>();
        taskRunner.runSyncTasksEagerly(t -> assertTrue(capturedTask.compareAndSet(null, t)));
        assertEquals(taskCount - maxTasks, queue.size()); // hasn't run any tasks yet
        capturedTask.get().run();
        assertTrue(queue.isEmpty());

        safeAwait(barrier);
        safeAwait(executedCountDown);
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

    public void testDirectExecutorDoesNotOverflowStack() {
        final int maxTasks = randomIntBetween(1, 10);
        final int taskCount = randomIntBetween(5_000, 10_000);
        final var counter = new AtomicInteger();
        ActionListener<Releasable> task = ActionListener.wrap(releasable -> {
            counter.incrementAndGet();
            releasable.close();
        }, e -> { throw new AssertionError(e); });

        final BlockingQueue<ActionListener<Releasable>> queue = ConcurrentCollections.newBlockingQueue();
        final AbstractThrottledTaskRunner<ActionListener<Releasable>> taskRunner = new AbstractThrottledTaskRunner<>(
            "test",
            maxTasks,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            queue
        );

        for (int i = 0; i < taskCount; i++) {
            queue.add(task);
        }
        taskRunner.enqueueTask(task);

        assertThat(counter.get(), equalTo(taskCount + 1));
        assertTrue(queue.isEmpty());
        assertThat(taskRunner.runningTasks(), equalTo(0));
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
