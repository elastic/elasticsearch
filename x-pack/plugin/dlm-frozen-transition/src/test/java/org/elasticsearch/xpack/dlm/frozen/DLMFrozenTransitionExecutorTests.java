/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.WrappedRunnable;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

public class DLMFrozenTransitionExecutorTests extends ESTestCase {
    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setup() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool("test-dlm-frozen-transition-executor");
        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        if (this.clusterService != null) {
            this.clusterService.close();
        }
        if (this.threadPool != null) {
            terminate(threadPool);
        }
        super.tearDown();
    }

    public void testTransitionSubmitted() throws Exception {
        try (var executor = new DLMFrozenTransitionExecutor(clusterService, 2, 10, Settings.EMPTY, makeErrorStore())) {
            var task = new TestDLMFrozenTransitionRunnable("running-index");
            task.blockUntil = new CountDownLatch(1);

            assertFalse(executor.transitionSubmitted("running-index"));

            Future<?> future = executor.submit(task);
            safeAwait(task.started);

            assertTrue(executor.transitionSubmitted("running-index"));
            assertFalse(executor.transitionSubmitted("other-index"));

            task.blockUntil.countDown();
        }
    }

    public void testTransitionRemovedAfterCompletion() throws Exception {
        try (var executor = new DLMFrozenTransitionExecutor(clusterService, 2, 100, Settings.EMPTY, makeErrorStore())) {
            var task = new TestDLMFrozenTransitionRunnable("done-index");

            executor.submit(task).get(10, TimeUnit.SECONDS);

            assertFalse(executor.transitionSubmitted("done-index"));
        }
    }

    public void testTransitionRemovedAfterFailure() throws Exception {
        var errorStore = makeErrorStore();
        try (var executor = new DLMFrozenTransitionExecutor(clusterService, 2, 100, Settings.EMPTY, errorStore)) {
            var runtimeTask = new TestDLMFrozenTransitionRunnable("exception-index");
            runtimeTask.throwOnRun = new IllegalStateException("simulated failure");
            executor.submit(runtimeTask).get(10, TimeUnit.SECONDS);
            assertFalse(executor.transitionSubmitted("exception-index"));
            ErrorEntry err = errorStore.getError(ProjectId.DEFAULT, "exception-index");
            assertNotNull("expected an error to be recorded in the error store", err);
            assertThat(err.error(), containsString("simulated failure"));
        }
    }

    public void testHasCapacity() throws Exception {
        int maxQueue = randomIntBetween(2, 50);
        try (var executor = new DLMFrozenTransitionExecutor(clusterService, 1, maxQueue, Settings.EMPTY, makeErrorStore())) {
            CountDownLatch tasksStarted = new CountDownLatch(1);
            CountDownLatch firstTaskBlock = new CountDownLatch(1);
            CountDownLatch taskBlock = new CountDownLatch(1);

            assertTrue(executor.hasCapacity());

            var firstTask = new TestDLMFrozenTransitionRunnable("index-first");
            firstTask.started = tasksStarted;
            firstTask.blockUntil = firstTaskBlock;
            executor.submit(firstTask);

            // Fill remaining queue
            for (int i = 0; i < maxQueue; i++) {
                var task = new TestDLMFrozenTransitionRunnable("index-" + i);
                task.started = tasksStarted;
                task.blockUntil = taskBlock;
                executor.submit(task);
            }

            assertTrue(tasksStarted.await(10, TimeUnit.SECONDS));
            assertFalse(executor.hasCapacity());

            firstTaskBlock.countDown();
            assertBusy(() -> assertTrue(executor.hasCapacity()));
            taskBlock.countDown();
        }
    }

    public void testShutdownNow() throws Exception {
        var executor = new DLMFrozenTransitionExecutor(clusterService, 1, 10, Settings.EMPTY, makeErrorStore());
        var task = new TestDLMFrozenTransitionRunnable("block-index");
        task.blockUntil = new CountDownLatch(1);

        executor.submit(task);
        safeAwait(task.started);

        List<Runnable> cancelled = executor.shutdownNow();
        assertNotNull(cancelled);
        executor.close();
    }

    /**
     * A task that is submitted to the executor but waiting in the queue (single thread occupied) must still
     * be reported as "submitted" by {@link DLMFrozenTransitionExecutor#transitionSubmitted}, because the entry
     * is added to {@code submittedTransitions} at submission time, not when the thread actually starts.
     * This is the invariant that {@code checkForFrozenIndices} relies on to prevent re-submission of queued tasks.
     */
    public void testTransitionSubmittedReturnsTrueForQueuedTask() throws Exception {
        try (var executor = new DLMFrozenTransitionExecutor(clusterService, 1, 2, Settings.EMPTY, makeErrorStore())) {
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch block = new CountDownLatch(1);

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index");
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            executor.submit(runningTask);
            safeAwait(firstStarted); // single thread is now occupied

            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index");
            queuedTask.blockUntil = block;
            executor.submit(queuedTask); // sits in the queue; has not started

            assertEquals("Queued task should not have started yet", 1, queuedTask.started.getCount());
            assertTrue("transitionSubmitted must return true for a queued task", executor.transitionSubmitted("queued-index"));

            block.countDown();
        }
    }

    /**
     * When the underlying executor rejects a submission (queue full), {@link DLMFrozenTransitionExecutor#submit}
     * must remove the index from {@code submittedTransitions} before rethrowing, so that a future poll can retry.
     */
    public void testSubmitCleansUpEntryOnRejectedExecution() throws Exception {
        var executor = new DLMFrozenTransitionExecutor(clusterService, 1, 1, Settings.EMPTY, makeErrorStore());
        try {
            CountDownLatch block = new CountDownLatch(1);
            CountDownLatch firstStarted = new CountDownLatch(1);

            var runningTask = new TestDLMFrozenTransitionRunnable("running-index");
            runningTask.started = firstStarted;
            runningTask.blockUntil = block;
            executor.submit(runningTask);
            safeAwait(firstStarted); // single thread occupied

            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index");
            queuedTask.blockUntil = block;
            executor.submit(queuedTask); // fills the one queue slot

            // Thread and queue are both full; next submit must be rejected
            var rejectedTask = new TestDLMFrozenTransitionRunnable("rejected-index");
            expectThrows(RejectedExecutionException.class, () -> executor.submit(rejectedTask));

            // The cleanup branch in submit() must have removed the entry so the index is no longer tracked
            assertFalse("Rejected index must be removed from submittedTransitions", executor.transitionSubmitted("rejected-index"));

            block.countDown();
        } finally {
            executor.close();
        }
    }

    /**
     * {@link DLMFrozenTransitionExecutor#shutdownNow()} must return tasks that were waiting in the queue
     * and had not yet started, not only the currently-executing task.
     */
    public void testShutdownNowReturnsQueuedTasks() throws Exception {
        var executor = new DLMFrozenTransitionExecutor(clusterService, 1, 5, Settings.EMPTY, makeErrorStore());
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch firstStarted = new CountDownLatch(1);

        var runningTask = new TestDLMFrozenTransitionRunnable("running-index");
        runningTask.started = firstStarted;
        runningTask.blockUntil = block;
        executor.submit(runningTask);
        safeAwait(firstStarted); // single thread occupied

        Set<Runnable> submittedFutures = new HashSet<>(3);
        for (int i = 0; i < 3; i++) {
            var queuedTask = new TestDLMFrozenTransitionRunnable("queued-index-" + i);
            queuedTask.blockUntil = block;
            submittedFutures.add((Runnable) executor.submit(queuedTask));
        }

        List<Runnable> cancelled = executor.shutdownNow();
        Set<Runnable> unwrappedCancelled = cancelled.stream().map(r -> {
            Runnable unwrapped = r;
            while (unwrapped instanceof WrappedRunnable wr) {
                unwrapped = wr.unwrap();
            }
            return unwrapped;
        }).collect(Collectors.toSet());
        assertEquals(submittedFutures, unwrappedCancelled);

        executor.close();
    }

    /**
     * Uses a {@link CyclicBarrier} to ensure all submitting threads call {@code submit()} at the same time,
     * verifying the executor accepts {@code maxConcurrency} simultaneous submissions without rejection.
     */
    public void testSimultaneousSubmissionsFromMultipleThreads() throws Exception {
        int maxConcurrency = between(2, 50);
        try (var executor = new DLMFrozenTransitionExecutor(clusterService, maxConcurrency, 1, Settings.EMPTY, makeErrorStore())) {
            CyclicBarrier barrier = new CyclicBarrier(maxConcurrency + 1);
            List<Future<?>> futures = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            List<Thread> submitters = new ArrayList<>(maxConcurrency + 1);

            for (int i = 0; i < maxConcurrency + 1; i++) {
                final String indexName = "simultaneous-" + i;
                Thread submitter = new Thread(() -> {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                        futures.add(executor.submit(new TestDLMFrozenTransitionRunnable(indexName)));
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }, "submitter-" + i);
                submitters.add(submitter);
                submitter.start();
            }

            for (Thread submitter : submitters) {
                submitter.join(10_000);
                assertFalse("Submitter thread should have finished", submitter.isAlive());
            }

            assertTrue("All submissions should succeed without error: " + errors, errors.isEmpty());
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Minimal test double implementing {@link DLMFrozenTransitionRunnable} with deterministic, test-controlled behavior.
     * The {@code started} latch always counts down when the task begins. Set {@code blockUntil} to a non-released latch
     * to hold the task, or leave it at the default (already released) for tasks that complete immediately.
     */
    static class TestDLMFrozenTransitionRunnable implements DLMFrozenTransitionRunnable {
        private final String indexName;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch blockUntil = new CountDownLatch(0);
        Throwable throwOnRun;

        TestDLMFrozenTransitionRunnable(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public ProjectId getProjectId() {
            return ProjectId.DEFAULT;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                blockUntil.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (throwOnRun instanceof RuntimeException rte) {
                throw rte;
            } else if (throwOnRun instanceof Error error) {
                throw error;
            }
        }
    }

    private DataStreamLifecycleErrorStore makeErrorStore() {
        return new DataStreamLifecycleErrorStore(System::currentTimeMillis);
    }
}
