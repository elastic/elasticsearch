/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class CapacityResponseCacheTests extends AutoscalingTestCase {
    private ThreadPool threadPool;
    // randomly run on generic pool, since it has at least 4 threads.
    private final Consumer<Runnable> runOnThread = randomBoolean()
        ? run -> threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(run)
        : run -> threadPool.executor(ThreadPool.Names.GENERIC).execute(run);

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
        threadPool = null;
    }

    public void testSingleResponse() throws ExecutionException, InterruptedException {
        Queue<Long> responses = new ArrayDeque<>();
        CapacityResponseCache<Long> cache = new CapacityResponseCache<>(runOnThread, cancelled -> responses.remove());
        long response = randomLong();
        responses.add(response);
        PlainActionFuture<Long> future = new PlainActionFuture<>();

        cache.get(() -> false, future);

        assertThat(future.get(), equalTo(response));
    }

    public void testSingleCancel() {
        CyclicBarrier barrier = new CyclicBarrier(2);
        CapacityResponseCache<Long> cache = new CapacityResponseCache<>(runOnThread, cancelled -> {
            await(barrier);
            await(barrier);
            cancelled.run();
            return 0L;
        });

        AtomicBoolean cancelled = new AtomicBoolean();
        PlainActionFuture<Long> future = new PlainActionFuture<>();
        cache.get(cancelled::get, future);

        await(barrier);
        cancelled.set(true);
        await(barrier);

        expectThrows(TaskCancelledException.class, future::actionGet);
    }

    public void testMultipleRequests() throws Exception {
        Queue<Supplier<Integer>> responses = new ArrayDeque<>();
        CapacityResponseCache<Integer> cache = new CapacityResponseCache<>(runOnThread, ensureNotCancelled -> responses.remove().get());

        CyclicBarrier barrier = new CyclicBarrier(2);
        responses.add(() -> {
            await(barrier);
            await(barrier);
            return 0;
        });

        List<Future<?>> spawned = new ArrayList<>();
        Consumer<Runnable> maybeSpawn = (runnable) -> {
            // by randomly not spawning, we check that it is non-blocking.
            if (randomBoolean()) {
                spawned.add(threadPool.generic().submit(runnable));
            } else {
                runnable.run();
            }
        };

        int count = between(1, 10);
        List<PlainActionFuture<Integer>> blockingFutures = new ArrayList<>(count + 1);
        for (int i = 0; i < count; ++i) {
            PlainActionFuture<Integer> blockingFuture = new PlainActionFuture<>();
            maybeSpawn.accept(() -> cache.get(() -> false, blockingFuture));
            blockingFutures.add(blockingFuture);
        }
        await(barrier);

        List<PlainActionFuture<Integer>> supersededFutures = new ArrayList<>(count + 1);
        for (int i = 0; i < count; ++i) {
            PlainActionFuture<Integer> supersededFuture = new PlainActionFuture<>();
            supersededFutures.add(supersededFuture);
            maybeSpawn.accept(() -> cache.get(ESTestCase::randomBoolean, supersededFuture));
        }

        int response = randomInt();
        responses.add(() -> response);
        PlainActionFuture<Integer> responseFuture = new PlainActionFuture<>();
        cache.get(() -> false, responseFuture);

        Stream.concat(blockingFutures.stream(), supersededFutures.stream()).forEach(future -> assertThat(future.isDone(), is(false)));
        assertThat(responseFuture.isDone(), is(false));

        spawned.forEach(FutureUtils::get);
        // release the initial blocked request.
        await(barrier);

        assertThat(blockingFutures.stream().mapToInt(ActionFuture::actionGet).filter(i -> i == 0).count(), greaterThan(0L));
        blockingFutures.stream().mapToInt(ActionFuture::actionGet).forEach(i -> assertThat(i, anyOf(is(0), is(response))));

        supersededFutures.forEach(future -> assertThat(future.actionGet(), equalTo(response)));
        assertThat(responseFuture.actionGet(), equalTo(response));

        assertThat(responses, empty());
        assertBusy(() -> assertThat(cache.jobQueueSize(), equalTo(0)));
        assertThat(cache.jobQueueCount(), equalTo(0));
    }

    public void testMultipleRequestsCancelled() throws Exception {
        Queue<Function<Runnable, Integer>> responses = new ArrayDeque<>();
        CapacityResponseCache<Integer> cache = new CapacityResponseCache<>(
            runOnThread,
            ensureNotCancelled -> responses.remove().apply(ensureNotCancelled)
        );

        CyclicBarrier barrier = new CyclicBarrier(2);
        responses.add(ensureNotCancelled -> {
            await(barrier);
            await(barrier);
            return 0;
        });

        PlainActionFuture<Integer> blockingFuture = new PlainActionFuture<>();
        cache.get(() -> false, blockingFuture);
        await(barrier);

        AtomicBoolean cancelled = new AtomicBoolean();
        int count = between(1, 10);
        List<PlainActionFuture<Integer>> supersededFutures = new ArrayList<>(count + 1);
        for (int i = 0; i < count; ++i) {
            PlainActionFuture<Integer> supersededFuture = new PlainActionFuture<>();
            supersededFutures.add(supersededFuture);
            cache.get(cancelled::get, supersededFuture);
        }

        CyclicBarrier responseBarrier = new CyclicBarrier(2);
        responses.add(ensureNotCancelled -> {
            await(responseBarrier);
            await(responseBarrier);
            ensureNotCancelled.run();
            fail();
            return 0;
        });
        PlainActionFuture<Integer> responseFuture = new PlainActionFuture<>();
        cache.get(cancelled::get, responseFuture);

        assertThat(blockingFuture.isDone(), is(false));
        for (PlainActionFuture<Integer> future : supersededFutures) {
            assertThat(future.isDone(), is(false));
        }
        assertThat(responseFuture.isDone(), is(false));

        // release the first blocking thread.
        await(barrier);
        assertThat(blockingFuture.actionGet(), equalTo(0));

        await(responseBarrier);
        cancelled.set(true);
        await(responseBarrier);
        supersededFutures.forEach(future -> expectThrows(TaskCancelledException.class, future::actionGet));
        expectThrows(TaskCancelledException.class, responseFuture::actionGet);

        assertThat(responses, empty());
        assertBusy(() -> assertThat(cache.jobQueueSize(), equalTo(0)));
        assertThat(cache.jobQueueCount(), equalTo(0));
    }

    public void testConcurrentRequests() throws Exception {
        AtomicInteger response = new AtomicInteger();
        CapacityResponseCache<Integer> cache = new CapacityResponseCache<>(runOnThread, cancellable -> response.incrementAndGet());
        int threadCount = between(2, 10);
        int iterations = between(2, 10);

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int t = 0; t < threadCount; ++t) {
            Thread thread = new Thread(() -> {
                await(barrier);
                int lastResult = -1;
                for (int i = 0; i < iterations; ++i) {
                    PlainActionFuture<Integer> future = new PlainActionFuture<>();
                    cache.get(() -> false, future);
                    int result = future.actionGet();
                    assertThat(result, greaterThan(lastResult));
                    lastResult = result;
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertBusy(() -> assertThat(cache.jobQueueSize(), equalTo(0)));
        assertThat(cache.jobQueueCount(), equalTo(0));
    }

    public void testThreadPoolExhausted() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        CapacityResponseCache<Integer> cache = new CapacityResponseCache<>(run -> {
            await(barrier);
            await(barrier);
            throw new EsRejectedExecutionException();
        }, cancellable -> {
            fail();
            return 0;
        });
        PlainActionFuture<Integer> future1 = new PlainActionFuture<>();
        runOnThread.accept(() -> { cache.get(() -> false, future1); });
        await(barrier);
        PlainActionFuture<Integer> future2 = new PlainActionFuture<>();
        cache.get(() -> false, future2);

        // release the spawned get request.
        await(barrier);

        expectThrows(EsRejectedExecutionException.class, future1::actionGet);
        expectThrows(EsRejectedExecutionException.class, future2::actionGet);
        assertBusy(() -> assertThat(cache.jobQueueSize(), equalTo(0)));
        assertThat(cache.jobQueueCount(), equalTo(0));
    }

    private void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            fail();
        }
    }
}
