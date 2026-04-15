/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DlmFrozenTransitionExecutorTests extends ESTestCase {

    /**
     * Minimal test double implementing {@link DlmFrozenTransitionRunnable} with deterministic, test-controlled behavior.
     * The {@code started} latch always counts down when the task begins. Set {@code blockUntil} to a non-released latch
     * to hold the task, or leave it at the default (already released) for tasks that complete immediately.
     */
    static class TestDlmFrozenTransitionRunnable implements DlmFrozenTransitionRunnable {
        private final String indexName;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch blockUntil = new CountDownLatch(0);
        Throwable throwOnRun;

        TestDlmFrozenTransitionRunnable(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public String getIndexName() {
            return indexName;
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

    public void testIsTransitionRunning() throws Exception {
        try (var executor = new DlmFrozenTransitionExecutor(2, Settings.EMPTY)) {
            var task = new TestDlmFrozenTransitionRunnable("running-index");
            task.blockUntil = new CountDownLatch(1);

            assertFalse(executor.isTransitionRunning("running-index"));

            executor.submit(task);
            safeAwait(task.started);

            assertTrue(executor.isTransitionRunning("running-index"));
            assertFalse(executor.isTransitionRunning("other-index"));

            task.blockUntil.countDown();
        }
    }

    public void testTransitionRemovedAfterCompletion() throws Exception {
        try (var executor = new DlmFrozenTransitionExecutor(2, Settings.EMPTY)) {
            var task = new TestDlmFrozenTransitionRunnable("done-index");

            executor.submit(task).get(10, TimeUnit.SECONDS);

            assertFalse(executor.isTransitionRunning("done-index"));
        }
    }

    public void testTransitionRemovedAfterFailure() throws Exception {
        try (var executor = new DlmFrozenTransitionExecutor(2, Settings.EMPTY)) {
            var runtimeTask = new TestDlmFrozenTransitionRunnable("exception-index");
            runtimeTask.throwOnRun = new IllegalStateException("simulated failure");
            executor.submit(runtimeTask).get(10, TimeUnit.SECONDS);
            assertFalse(executor.isTransitionRunning("exception-index"));
        }
    }

    public void testHasCapacity() throws Exception {
        int maxConcurrency = 2;
        try (var executor = new DlmFrozenTransitionExecutor(maxConcurrency, Settings.EMPTY)) {
            CountDownLatch tasksStarted = new CountDownLatch(2);
            CountDownLatch taskBlock = new CountDownLatch(1);

            assertTrue(executor.hasCapacity());

            for (int i = 0; i < maxConcurrency; i++) {
                var task = new TestDlmFrozenTransitionRunnable("index-" + i);
                task.started = tasksStarted;
                task.blockUntil = taskBlock;
                executor.submit(task);
            }

            assertTrue(tasksStarted.await(10, TimeUnit.SECONDS));
            assertFalse(executor.hasCapacity());

            taskBlock.countDown();
            assertBusy(() -> assertTrue(executor.hasCapacity()));
        }
    }

    public void testShutdownNow() throws Exception {
        var executor = new DlmFrozenTransitionExecutor(1, Settings.EMPTY);
        var task = new TestDlmFrozenTransitionRunnable("block-index");
        task.blockUntil = new CountDownLatch(1);

        executor.submit(task);
        safeAwait(task.started);

        List<Runnable> cancelled = executor.shutdownNow();
        assertNotNull(cancelled);
        executor.close();
    }

    /**
     * Uses a {@link CyclicBarrier} to ensure all submitting threads call {@code submit()} at the same time,
     * verifying the executor accepts {@code maxConcurrency} simultaneous submissions without rejection.
     */
    public void testSimultaneousSubmissionsFromMultipleThreads() throws Exception {
        int maxConcurrency = between(2, 50);
        try (var executor = new DlmFrozenTransitionExecutor(maxConcurrency, Settings.EMPTY)) {
            CyclicBarrier barrier = new CyclicBarrier(maxConcurrency);
            List<Future<?>> futures = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            List<Thread> submitters = new ArrayList<>(maxConcurrency);

            for (int i = 0; i < maxConcurrency; i++) {
                final String indexName = "simultaneous-" + i;
                Thread submitter = new Thread(() -> {
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                        futures.add(executor.submit(new TestDlmFrozenTransitionRunnable(indexName)));
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
}
