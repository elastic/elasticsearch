/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ComputeExecutorTests extends ESTestCase {

    public void testRunsEveryTaskOnce() throws Exception {
        ThreadPool threadPool = threadPool(between(1, 8), 10_000);
        try {
            ComputeExecutor executor = new ComputeExecutor(threadPool.executor("esql"), threadPool.getThreadContext());
            int count = between(1, 500);
            CountDownLatch done = new CountDownLatch(count);
            List<AtomicInteger> runs = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                AtomicInteger run = new AtomicInteger();
                runs.add(run);
                submit(executor, () -> {
                    run.incrementAndGet();
                    done.countDown();
                }, randomBoolean());
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
            for (AtomicInteger run : runs) {
                assertThat(run.get(), equalTo(1));
            }
        } finally {
            terminate(threadPool);
        }
    }

    public void testResumedGoesFirst() throws Exception {
        ThreadPool threadPool = threadPool(1, 10_000);
        try {
            ComputeExecutor executor = new ComputeExecutor(threadPool.executor("esql"), threadPool.getThreadContext());
            List<String> order = new CopyOnWriteArrayList<>();
            CountDownLatch gateEntered = new CountDownLatch(1);
            CountDownLatch gate = new CountDownLatch(1);
            // The gate is the first placeholder to run; it holds the only thread while the rest queue up.
            submit(executor, () -> {
                gateEntered.countDown();
                safeAwait(gate);
            }, false);
            assertTrue(gateEntered.await(10, TimeUnit.SECONDS));

            submit(executor, () -> order.add("normal-1"), false);
            submit(executor, () -> order.add("normal-2"), false);
            submit(executor, () -> order.add("resumed-1"), true);
            submit(executor, () -> order.add("normal-3"), false);
            submit(executor, () -> order.add("resumed-2"), true);
            submit(executor, () -> order.add("resumed-3"), true);
            // One placeholder per task is what the pool sees.
            assertThat(((EsThreadPoolExecutor) threadPool.executor("esql")).getQueue().size(), equalTo(6));

            gate.countDown();
            assertBusy(() -> assertThat(order.size(), equalTo(6)));
            assertThat(order, contains("resumed-1", "resumed-2", "resumed-3", "normal-1", "normal-2", "normal-3"));
        } finally {
            terminate(threadPool);
        }
    }

    public void testNormalGetsEveryNthTurn() throws Exception {
        ThreadPool threadPool = threadPool(1, 10_000);
        try {
            ComputeExecutor executor = new ComputeExecutor(threadPool.executor("esql"), threadPool.getThreadContext());
            List<String> order = new CopyOnWriteArrayList<>();
            CountDownLatch gateEntered = new CountDownLatch(1);
            CountDownLatch gate = new CountDownLatch(1);
            submit(executor, () -> {
                gateEntered.countDown();
                safeAwait(gate);
            }, false);
            assertTrue(gateEntered.await(10, TimeUnit.SECONDS));

            int normals = 2;
            int resumeds = 3 * ComputeExecutor.NORMAL_EVERY;
            for (int i = 0; i < normals; i++) {
                submit(executor, () -> order.add("normal"), false);
            }
            for (int i = 0; i < resumeds; i++) {
                submit(executor, () -> order.add("resumed"), true);
            }
            // the gate took the first turn; a normal task gets every NORMAL_EVERY-th turn after it
            List<String> expected = new ArrayList<>();
            for (int turn = 2; turn <= normals + resumeds + 1; turn++) {
                expected.add(
                    turn % ComputeExecutor.NORMAL_EVERY == 0 && turn / ComputeExecutor.NORMAL_EVERY <= normals ? "normal" : "resumed"
                );
            }

            gate.countDown();
            assertBusy(() -> assertThat(order.size(), equalTo(normals + resumeds)));
            assertThat(order, equalTo(expected));
        } finally {
            terminate(threadPool);
        }
    }

    public void testRejection() throws Exception {
        ThreadPool threadPool = threadPool(1, 1);
        try {
            ComputeExecutor executor = new ComputeExecutor(threadPool.executor("esql"), threadPool.getThreadContext());
            CountDownLatch gateEntered = new CountDownLatch(1);
            CountDownLatch gate = new CountDownLatch(1);
            submit(executor, () -> {
                gateEntered.countDown();
                safeAwait(gate);
            }, false);
            assertTrue(gateEntered.await(10, TimeUnit.SECONDS));

            AtomicReference<Exception> acceptedRejection = new AtomicReference<>();
            AtomicInteger acceptedRuns = new AtomicInteger();
            boolean acceptedNext = randomBoolean();
            AbstractRunnable accepted = abstractRunnable(acceptedRuns::incrementAndGet, acceptedRejection);
            if (acceptedNext) {
                executor.executeResumed(accepted); // fills the pool queue
            } else {
                executor.execute(accepted);
            }

            AtomicReference<Exception> rejection = new AtomicReference<>();
            AtomicInteger rejectedRuns = new AtomicInteger();
            AbstractRunnable rejected = abstractRunnable(rejectedRuns::incrementAndGet, rejection);
            if (randomBoolean()) {
                executor.executeResumed(rejected);
            } else {
                executor.execute(rejected);
            }
            assertThat(rejection.get(), instanceOf(EsRejectedExecutionException.class));
            assertNull(acceptedRejection.get());

            EsRejectedExecutionException e = expectThrows(
                EsRejectedExecutionException.class,
                () -> executor.execute(() -> fail("rejected task must not run"))
            );
            assertThat(e.isExecutorShutdown(), equalTo(false));

            gate.countDown();
            assertBusy(() -> assertThat(acceptedRuns.get(), equalTo(1)));
            assertThat(rejectedRuns.get(), equalTo(0));
            assertThat(((EsThreadPoolExecutor) threadPool.executor("esql")).getQueue().size(), equalTo(0));
        } finally {
            terminate(threadPool);
        }
    }

    public void testSameTaskSubmittedTwice() throws Exception {
        ThreadPool threadPool = threadPool(1, 2);
        try {
            ThreadContext threadContext = threadPool.getThreadContext();
            ComputeExecutor executor = new ComputeExecutor(threadPool.executor("esql"), threadContext);
            CountDownLatch gateEntered = new CountDownLatch(1);
            CountDownLatch gate = new CountDownLatch(1);
            submit(executor, () -> {
                gateEntered.countDown();
                safeAwait(gate);
            }, false);
            assertTrue(gateEntered.await(10, TimeUnit.SECONDS));

            AtomicInteger runs = new AtomicInteger();
            AtomicInteger rejections = new AtomicInteger();
            AbstractRunnable task = (AbstractRunnable) threadContext.preserveContext(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    runs.incrementAndGet();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                public void onRejection(Exception e) {
                    rejections.incrementAndGet();
                }
            });
            for (int i = 0; i < 3; i++) {
                if (randomBoolean()) {
                    executor.executeResumed(task);
                } else {
                    executor.execute(task);
                }
            }
            // The pool queue holds two placeholders, so the third submission is rejected.
            assertThat(rejections.get(), equalTo(1));
            assertThat(((EsThreadPoolExecutor) threadPool.executor("esql")).getQueue().size(), equalTo(2));

            gate.countDown();
            assertBusy(() -> assertThat(runs.get(), equalTo(2)));
            assertThat(rejections.get(), equalTo(1));
        } finally {
            terminate(threadPool);
        }
    }

    public void testPreservesEachTasksThreadContext() throws Exception {
        ThreadPool threadPool = threadPool(1, 10_000);
        try {
            ThreadContext threadContext = threadPool.getThreadContext();
            ComputeExecutor executor = new ComputeExecutor(threadPool.executor("esql"), threadContext);
            CountDownLatch gateEntered = new CountDownLatch(1);
            CountDownLatch gate = new CountDownLatch(1);
            submit(executor, () -> {
                gateEntered.countDown();
                safeAwait(gate);
            }, false);
            assertTrue(gateEntered.await(10, TimeUnit.SECONDS));

            List<String> seen = new CopyOnWriteArrayList<>();
            // Submitted first, so its placeholder is first in the pool queue, but it runs second: the placeholder runs the
            // task that jumped the queue.
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putHeader("user", "first-user");
                submit(executor, () -> seen.add("first:" + threadContext.getHeader("user")), false);
            }
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putHeader("user", "resumed-user");
                submit(executor, () -> seen.add("resumed:" + threadContext.getHeader("user")), true);
            }
            gate.countDown();
            assertBusy(() -> assertThat(seen.size(), equalTo(2)));
            assertThat(seen, contains("resumed:resumed-user", "first:first-user"));
        } finally {
            terminate(threadPool);
        }
    }

    public void testDirectExecutorRunsInline() {
        ComputeExecutor executor = new ComputeExecutor(EsExecutors.DIRECT_EXECUTOR_SERVICE, new ThreadContext(Settings.EMPTY));
        AtomicReference<Thread> ranOn = new AtomicReference<>();
        submit(executor, () -> ranOn.set(Thread.currentThread()), randomBoolean());
        assertThat(ranOn.get(), sameInstance(Thread.currentThread()));
    }

    private static void submit(ComputeExecutor executor, Runnable task, boolean next) {
        AbstractRunnable abstractRunnable = abstractRunnable(task, new AtomicReference<>());
        if (next) {
            executor.executeResumed(abstractRunnable);
        } else {
            executor.execute(abstractRunnable);
        }
    }

    private static AbstractRunnable abstractRunnable(Runnable task, AtomicReference<Exception> rejection) {
        return new AbstractRunnable() {
            @Override
            protected void doRun() {
                task.run();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void onRejection(Exception e) {
                rejection.set(e);
            }
        };
    }

    private ThreadPool threadPool(int threads, int queueSize) {
        return new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, "esql", threads, queueSize, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }
}
