/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class IndexShardOperationPermitsTests extends ESTestCase {

    private static ThreadPool threadPool;

    private IndexShardOperationPermits permits;

    private static final String REJECTING_EXECUTOR = "rejecting";

    @BeforeClass
    public static void setupThreadPool() {
        int writeThreadPoolSize = randomIntBetween(1, 2);
        int writeThreadPoolQueueSize = randomIntBetween(1, 2);
        threadPool = new TestThreadPool(
            "IndexShardOperationPermitsTests",
            Settings.builder()
                .put("thread_pool." + ThreadPool.Names.WRITE + ".size", writeThreadPoolSize)
                .put("thread_pool." + ThreadPool.Names.WRITE + ".queue_size", writeThreadPoolQueueSize)
                .build(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                REJECTING_EXECUTOR,
                1,
                0,
                REJECTING_EXECUTOR,
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
        assertThat(threadPool.executor(ThreadPool.Names.WRITE), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.WRITE)).getCorePoolSize(), equalTo(writeThreadPoolSize));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.WRITE)).getMaximumPoolSize(), equalTo(writeThreadPoolSize));
        assertThat(
            ((EsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.WRITE)).getQueue().remainingCapacity(),
            equalTo(writeThreadPoolQueueSize)
        );
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void createIndexShardOperationsLock() {
        permits = new IndexShardOperationPermits(new ShardId("blubb", "id", 0), threadPool);
    }

    @After
    public void checkNoInflightOperations() {
        assertThat(permits.semaphore.availablePermits(), equalTo(Integer.MAX_VALUE));
        assertThat(permits.getActiveOperationsCount(), equalTo(0));
    }

    public void testAllOperationsInvoked() throws InterruptedException, TimeoutException {
        int numThreads = 10;

        List<PlainActionFuture<Releasable>> futures = new ArrayList<>();
        List<Thread> operationThreads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads / 4);
        boolean forceExecution = randomBoolean();
        for (int i = 0; i < numThreads; i++) {
            // the write thread pool uses a bounded size and can get rejections, see setupThreadPool
            Executor executor = threadPool.executor(randomFrom(ThreadPool.Names.WRITE, ThreadPool.Names.GENERIC));
            PlainActionFuture<Releasable> future = new PlainActionFuture<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    super.onResponse(releasable);
                }
            };
            Thread thread = new Thread(() -> {
                latch.countDown();
                permits.acquire(future, executor, forceExecution);
            });
            futures.add(future);
            operationThreads.add(thread);
        }

        boolean closeAfterBlocking = randomBoolean();
        CountDownLatch blockFinished = new CountDownLatch(1);
        threadPool.generic().execute(() -> {
            try {
                latch.await();
                blockAndWait().close();
                blockFinished.countDown();
                if (closeAfterBlocking) {
                    permits.close();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        for (Thread thread : operationThreads) {
            thread.start();
        }

        for (PlainActionFuture<Releasable> future : futures) {
            try {
                assertNotNull(future.get(1, TimeUnit.MINUTES));
            } catch (ExecutionException e) {
                if (closeAfterBlocking) {
                    assertThat(
                        e.getCause(),
                        either(instanceOf(EsRejectedExecutionException.class)).or(instanceOf(IndexShardClosedException.class))
                    );
                } else {
                    assertThat(e.getCause(), instanceOf(EsRejectedExecutionException.class));
                }
            }
        }

        for (Thread thread : operationThreads) {
            thread.join();
        }

        blockFinished.await();
    }

    public void testOperationsInvokedImmediatelyIfNoBlock() throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        permits.acquire(future, threadPool.generic(), true);
        assertTrue(future.isDone());
        future.get().close();
    }

    public void testOperationsIfClosed() {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        permits.close();
        permits.acquire(future, threadPool.generic(), true);
        ExecutionException exception = expectThrows(ExecutionException.class, future::get);
        assertThat(exception.getCause(), instanceOf(IndexShardClosedException.class));
    }

    public void testBlockIfClosed() {
        permits.close();
        expectThrows(
            IndexShardClosedException.class,
            () -> permits.blockOperations(
                wrap(() -> { throw new IllegalArgumentException("fake error"); }),
                randomInt(10),
                TimeUnit.MINUTES,
                threadPool.generic()
            )
        );
    }

    public void testOperationsDelayedIfBlock() throws ExecutionException, InterruptedException, TimeoutException {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        try (Releasable ignored = blockAndWait()) {
            permits.acquire(future, threadPool.generic(), true);
            assertFalse(future.isDone());
        }
        future.get(1, TimeUnit.HOURS).close();
    }

    public void testGetBlockWhenBlocked() throws ExecutionException, InterruptedException, TimeoutException {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        final CountDownLatch blockAcquired = new CountDownLatch(1);
        final CountDownLatch releaseBlock = new CountDownLatch(1);
        final AtomicBoolean blocked = new AtomicBoolean();
        try (Releasable ignored = blockAndWait()) {
            permits.acquire(future, threadPool.generic(), true);

            permits.blockOperations(wrap(() -> {
                blocked.set(true);
                blockAcquired.countDown();
                releaseBlock.await();
            }), 30, TimeUnit.MINUTES, threadPool.generic());
            assertFalse(blocked.get());
            assertFalse(future.isDone());
        }
        blockAcquired.await();
        assertTrue(blocked.get());
        assertFalse(future.isDone());
        releaseBlock.countDown();

        future.get(1, TimeUnit.HOURS).close();
    }

    /**
     * Tests that the ThreadContext is restored when a operation is executed after it has been delayed due to a block
     */
    public void testThreadContextPreservedIfBlock() throws ExecutionException, InterruptedException, TimeoutException {
        final ThreadContext context = threadPool.getThreadContext();
        final Function<ActionListener<Releasable>, Boolean> contextChecker = (listener) -> {
            if ("bar".equals(context.getHeader("foo")) == false) {
                listener.onFailure(
                    new IllegalStateException(
                        "context did not have value [bar] for header [foo]. Actual value [" + context.getHeader("foo") + "]"
                    )
                );
            } else if ("baz".equals(context.getTransient("bar")) == false) {
                listener.onFailure(
                    new IllegalStateException(
                        "context did not have value [baz] for transient [bar]. Actual value [" + context.getTransient("bar") + "]"
                    )
                );
            } else {
                return true;
            }
            return false;
        };
        PlainActionFuture<Releasable> future = new PlainActionFuture<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                if (contextChecker.apply(this)) {
                    super.onResponse(releasable);
                }
            }
        };
        PlainActionFuture<Releasable> future2 = new PlainActionFuture<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                if (contextChecker.apply(this)) {
                    super.onResponse(releasable);
                }
            }
        };

        try (Releasable ignored = blockAndWait()) {
            // we preserve the thread context here so that we have a different context in the call to acquire than the context present
            // when the releasable is closed
            try (ThreadContext.StoredContext ignore = context.newStoredContext()) {
                context.putHeader("foo", "bar");
                context.putTransient("bar", "baz");
                // test both with and without a executor name
                permits.acquire(future, threadPool.generic(), true);
                permits.acquire(future2, null, true);
            }
            assertFalse(future.isDone());
        }
        future.get(1, TimeUnit.HOURS).close();
        future2.get(1, TimeUnit.HOURS).close();
    }

    private Releasable blockAndWait() throws InterruptedException {
        CountDownLatch blockAcquired = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);
        CountDownLatch blockReleased = new CountDownLatch(1);
        boolean throwsException = randomBoolean();
        IndexShardClosedException exception = new IndexShardClosedException(new ShardId("blubb", "id", 0));
        permits.blockOperations(ActionListener.runAfter(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    blockAcquired.countDown();
                    releaseBlock.await();
                    if (throwsException) {
                        onFailure(exception);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException();
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e != exception) {
                    throw new RuntimeException(e);
                }
            }
        }, blockReleased::countDown), 1, TimeUnit.MINUTES, threadPool.generic());
        blockAcquired.await();
        return () -> {
            releaseBlock.countDown();
            try {
                blockReleased.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public void testAsyncBlockOperationsOperationWhileBlocked() throws InterruptedException {
        final CountDownLatch blockAcquired = new CountDownLatch(1);
        final CountDownLatch releaseBlock = new CountDownLatch(1);
        final AtomicBoolean blocked = new AtomicBoolean();
        permits.blockOperations(wrap(() -> {
            blocked.set(true);
            blockAcquired.countDown();
            releaseBlock.await();
        }), 30, TimeUnit.MINUTES, threadPool.generic());
        blockAcquired.await();
        assertTrue(blocked.get());

        // an operation that is submitted while there is a delay in place should be delayed
        final CountDownLatch delayedOperation = new CountDownLatch(1);
        final AtomicBoolean delayed = new AtomicBoolean();
        final Thread thread = new Thread(() -> permits.acquire(new ActionListener<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                delayed.set(true);
                releasable.close();
                delayedOperation.countDown();
            }

            @Override
            public void onFailure(Exception e) {

            }
        }, threadPool.generic(), false));
        thread.start();
        assertFalse(delayed.get());
        releaseBlock.countDown();
        delayedOperation.await();
        assertTrue(delayed.get());
        thread.join();
    }

    public void testAsyncBlockOperationsOperationBeforeBlocked() throws InterruptedException, BrokenBarrierException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch operationExecutingLatch = new CountDownLatch(1);
        final CountDownLatch firstOperationLatch = new CountDownLatch(1);
        final CountDownLatch firstOperationCompleteLatch = new CountDownLatch(1);
        final Thread firstOperationThread = new Thread(
            controlledAcquire(barrier, operationExecutingLatch, firstOperationLatch, firstOperationCompleteLatch)
        );
        firstOperationThread.start();

        barrier.await();

        operationExecutingLatch.await();

        // now we will delay operations while the first operation is still executing (because it is latched)
        final CountDownLatch blockedLatch = new CountDownLatch(1);
        final AtomicBoolean onBlocked = new AtomicBoolean();
        permits.blockOperations(wrap(() -> {
            onBlocked.set(true);
            blockedLatch.countDown();
        }), 30, TimeUnit.MINUTES, threadPool.generic());
        assertFalse(onBlocked.get());

        // if we submit another operation, it should be delayed
        final CountDownLatch secondOperationExecuting = new CountDownLatch(1);
        final CountDownLatch secondOperationComplete = new CountDownLatch(1);
        final AtomicBoolean secondOperation = new AtomicBoolean();
        final Thread secondOperationThread = new Thread(() -> {
            secondOperationExecuting.countDown();
            permits.acquire(new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    secondOperation.set(true);
                    releasable.close();
                    secondOperationComplete.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            }, threadPool.generic(), false);
        });
        secondOperationThread.start();

        secondOperationExecuting.await();
        assertFalse(secondOperation.get());

        firstOperationLatch.countDown();
        firstOperationCompleteLatch.await();
        blockedLatch.await();
        assertTrue(onBlocked.get());

        secondOperationComplete.await();
        assertTrue(secondOperation.get());

        firstOperationThread.join();
        secondOperationThread.join();
    }

    public void testAsyncBlockOperationsRace() throws Exception {
        // we racily submit operations and a delay, and then ensure that all operations were actually completed
        final int operations = scaledRandomIntBetween(1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(1 + 1 + operations);
        final CountDownLatch operationLatch = new CountDownLatch(1 + operations);
        final Set<Integer> values = ConcurrentCollections.newConcurrentSet();
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < operations; i++) {
            final int value = i;
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                permits.acquire(new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        values.add(value);
                        releasable.close();
                        operationLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {

                    }
                }, threadPool.generic(), false);
            });
            thread.start();
            threads.add(thread);
        }

        final Thread blockingThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            permits.blockOperations(wrap(() -> {
                values.add(operations);
                operationLatch.countDown();
            }), 30, TimeUnit.MINUTES, threadPool.generic());
        });
        blockingThread.start();

        barrier.await();

        operationLatch.await();
        for (final Thread thread : threads) {
            thread.join();
        }
        blockingThread.join();

        // check that all operations completed
        for (int i = 0; i < operations; i++) {
            assertTrue(values.contains(i));
        }
        assertTrue(values.contains(operations));
        /*
         * The block operation is executed on another thread and the operations can have completed before this thread has returned all the
         * permits to the semaphore. We wait here until all generic threads are idle as an indication that all permits have been returned to
         * the semaphore.
         */
        assertBusy(() -> {
            for (final ThreadPoolStats.Stats stats : threadPool.stats()) {
                if (ThreadPool.Names.GENERIC.equals(stats.name())) {
                    assertThat("Expected no active threads in GENERIC pool", stats.active(), equalTo(0));
                    return;
                }
            }
            fail("Failed to find stats for the GENERIC thread pool");
        });
    }

    public void testActiveOperationsCount() throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> future1 = new PlainActionFuture<>();
        permits.acquire(future1, threadPool.generic(), true);
        assertTrue(future1.isDone());
        assertThat(permits.getActiveOperationsCount(), equalTo(1));

        PlainActionFuture<Releasable> future2 = new PlainActionFuture<>();
        permits.acquire(future2, threadPool.generic(), true);
        assertTrue(future2.isDone());
        assertThat(permits.getActiveOperationsCount(), equalTo(2));

        future1.get().close();
        assertThat(permits.getActiveOperationsCount(), equalTo(1));
        future1.get().close(); // check idempotence
        assertThat(permits.getActiveOperationsCount(), equalTo(1));
        future2.get().close();
        assertThat(permits.getActiveOperationsCount(), equalTo(0));

        try (Releasable ignored = blockAndWait()) {
            assertThat(permits.getActiveOperationsCount(), equalTo(IndexShard.OPERATIONS_BLOCKED));
        }

        PlainActionFuture<Releasable> future3 = new PlainActionFuture<>();
        permits.acquire(future3, threadPool.generic(), true);
        assertTrue(future3.isDone());
        assertThat(permits.getActiveOperationsCount(), equalTo(1));
        future3.get().close();
        assertThat(permits.getActiveOperationsCount(), equalTo(0));
    }

    private Releasable acquirePermitImmediately() {
        final var listener = SubscribableListener.<Releasable>newForked(l -> permits.acquire(l, threadPool.generic(), false));
        assertTrue(listener.isDone());
        return safeAwait(listener);
    }

    public void testAsyncBlockOperationsOnRejection() {
        final PlainActionFuture<Void> threadBlock = new PlainActionFuture<>();
        try (Releasable firstPermit = acquirePermitImmediately()) {
            assertNotNull(firstPermit);

            final var rejectingExecutor = threadPool.executor(REJECTING_EXECUTOR);
            rejectingExecutor.execute(threadBlock::actionGet);
            assertThat(
                safeAwaitFailure(Releasable.class, l -> permits.blockOperations(l, 1, TimeUnit.HOURS, rejectingExecutor)),
                instanceOf(EsRejectedExecutionException.class)
            );

            // ensure that the exception means no block was put in place
            try (Releasable secondPermit = acquirePermitImmediately()) {
                assertNotNull(secondPermit);
            }
        } finally {
            threadBlock.onResponse(null);
        }

        // ensure that another block can still be acquired
        try (Releasable block = safeAwait(l -> permits.blockOperations(l, 1, TimeUnit.HOURS, threadPool.generic()))) {
            assertNotNull(block);
        }
    }

    public void testAsyncBlockOperationsOnTimeout() {
        final PlainActionFuture<Void> threadBlock = new PlainActionFuture<>();
        try (Releasable firstPermit = acquirePermitImmediately()) {
            assertNotNull(firstPermit);

            assertEquals(
                "timeout while blocking operations after [0s]",
                asInstanceOf(
                    ElasticsearchTimeoutException.class,
                    safeAwaitFailure(Releasable.class, f -> permits.blockOperations(f, 0, TimeUnit.SECONDS, threadPool.generic()))
                ).getMessage()
            );

            // ensure that the exception means no block was put in place
            try (Releasable secondPermit = acquirePermitImmediately()) {
                assertNotNull(secondPermit);
            }

        } finally {
            threadBlock.onResponse(null);
        }

        // ensure that another block can still be acquired
        try (Releasable block = safeAwait(l -> permits.blockOperations(l, 1, TimeUnit.HOURS, threadPool.generic()))) {
            assertNotNull(block);
        }
    }

    public void testTimeout() throws BrokenBarrierException, InterruptedException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch operationExecutingLatch = new CountDownLatch(1);
        final CountDownLatch operationLatch = new CountDownLatch(1);
        final CountDownLatch operationCompleteLatch = new CountDownLatch(1);

        final Thread thread = new Thread(controlledAcquire(barrier, operationExecutingLatch, operationLatch, operationCompleteLatch));
        thread.start();

        barrier.await();

        operationExecutingLatch.await();

        final AtomicReference<Exception> reference = new AtomicReference<>();
        final CountDownLatch onFailureLatch = new CountDownLatch(1);
        permits.blockOperations(new ActionListener<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                releasable.close();
            }

            @Override
            public void onFailure(final Exception e) {
                reference.set(e);
                onFailureLatch.countDown();
            }
        }, 1, TimeUnit.MILLISECONDS, threadPool.generic());
        onFailureLatch.await();
        assertThat(reference.get(), hasToString(containsString("timeout while blocking operations")));

        operationLatch.countDown();

        operationCompleteLatch.await();

        thread.join();
    }

    public void testNoPermitsRemaining() throws InterruptedException {
        permits.semaphore.tryAcquire(IndexShardOperationPermits.TOTAL_PERMITS, 1, TimeUnit.SECONDS);
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> this.permits.acquire(new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    assert false;
                }

                @Override
                public void onFailure(Exception e) {
                    assert false;
                }
            }, threadPool.generic(), false)
        );
        assertThat(e, hasToString(containsString("failed to obtain permit but operations are not delayed")));
        permits.semaphore.release(IndexShardOperationPermits.TOTAL_PERMITS);
    }

    /**
     * Returns an operation that acquires a permit and synchronizes in the following manner:
     * <ul>
     * <li>waits on the {@code barrier} before acquiring a permit</li>
     * <li>counts down the {@code operationExecutingLatch} when it acquires the permit</li>
     * <li>waits on the {@code operationLatch} before releasing the permit</li>
     * <li>counts down the {@code operationCompleteLatch} after releasing the permit</li>
     * </ul>
     *
     * @param barrier                 the barrier to wait on
     * @param operationExecutingLatch the latch to countdown after acquiring the permit
     * @param operationLatch          the latch to wait on before releasing the permit
     * @param operationCompleteLatch  the latch to countdown after releasing the permit
     * @return a controllable runnable that acquires a permit
     */
    private Runnable controlledAcquire(
        final CyclicBarrier barrier,
        final CountDownLatch operationExecutingLatch,
        final CountDownLatch operationLatch,
        final CountDownLatch operationCompleteLatch
    ) {
        return () -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            permits.acquire(new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    operationExecutingLatch.countDown();
                    try {
                        operationLatch.await();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    releasable.close();
                    operationCompleteLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            }, threadPool.generic(), false);
        };
    }

    private static ActionListener<Releasable> wrap(final CheckedRunnable<Exception> onResponse) {
        return ActionTestUtils.assertNoFailureListener(releasable -> {
            try (Releasable ignored = releasable) {
                onResponse.run();
            }
        });
    }
}
