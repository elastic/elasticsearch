/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IndexShardOperationsLockTests extends ESTestCase {

    private static ThreadPool threadPool;

    private IndexShardOperationsLock block;

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool("IndexShardOperationsLockTests");
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void createIndexShardOperationsLock() {
         block = new IndexShardOperationsLock(new ShardId("blubb", "id", 0), logger, threadPool);
    }

    @After
    public void checkNoInflightOperations() {
        assertThat(block.semaphore.availablePermits(), equalTo(Integer.MAX_VALUE));
        assertThat(block.getActiveOperationsCount(), equalTo(0));
    }

    public void testAllOperationsInvoked() throws InterruptedException, TimeoutException, ExecutionException {
        int numThreads = 10;

        List<PlainActionFuture<Releasable>> futures = new ArrayList<>();
        List<Thread> operationThreads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads / 2);
        for (int i = 0; i < numThreads; i++) {
            PlainActionFuture<Releasable> future = new PlainActionFuture<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    super.onResponse(releasable);
                }
            };
            Thread thread = new Thread() {
                public void run() {
                    latch.countDown();
                    block.acquire(future, ThreadPool.Names.GENERIC, true);
                }
            };
            futures.add(future);
            operationThreads.add(thread);
        }

        CountDownLatch blockFinished = new CountDownLatch(1);
        threadPool.generic().execute(() -> {
            try {
                latch.await();
                blockAndWait().close();
                blockFinished.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        for (Thread thread : operationThreads) {
            thread.start();
        }

        for (PlainActionFuture<Releasable> future : futures) {
            assertNotNull(future.get(1, TimeUnit.MINUTES));
        }

        for (Thread thread : operationThreads) {
            thread.join();
        }

        blockFinished.await();
    }


    public void testOperationsInvokedImmediatelyIfNoBlock() throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        block.acquire(future, ThreadPool.Names.GENERIC, true);
        assertTrue(future.isDone());
        future.get().close();
    }

    public void testOperationsIfClosed() throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        block.close();
        block.acquire(future, ThreadPool.Names.GENERIC, true);
        ExecutionException exception = expectThrows(ExecutionException.class, future::get);
        assertThat(exception.getCause(), instanceOf(IndexShardClosedException.class));
    }

    public void testBlockIfClosed() throws ExecutionException, InterruptedException {
        block.close();
        expectThrows(IndexShardClosedException.class, () -> block.blockOperations(randomInt(10), TimeUnit.MINUTES,
            () -> { throw new IllegalArgumentException("fake error"); }));
    }

    public void testOperationsDelayedIfBlock() throws ExecutionException, InterruptedException, TimeoutException {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        try (Releasable releasable = blockAndWait()) {
            block.acquire(future, ThreadPool.Names.GENERIC, true);
            assertFalse(future.isDone());
        }
        future.get(1, TimeUnit.HOURS).close();
    }

    /**
     * Tests that the ThreadContext is restored when a operation is executed after it has been delayed due to a block
     */
    public void testThreadContextPreservedIfBlock() throws ExecutionException, InterruptedException, TimeoutException {
        final ThreadContext context = threadPool.getThreadContext();
        final Function<ActionListener<Releasable>, Boolean> contextChecker = (listener) -> {
            if ("bar".equals(context.getHeader("foo")) == false) {
                listener.onFailure(new IllegalStateException("context did not have value [bar] for header [foo]. Actual value [" +
                    context.getHeader("foo") + "]"));
            } else if ("baz".equals(context.getTransient("bar")) == false) {
                listener.onFailure(new IllegalStateException("context did not have value [baz] for transient [bar]. Actual value [" +
                    context.getTransient("bar") + "]"));
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

        try (Releasable releasable = blockAndWait()) {
            // we preserve the thread context here so that we have a different context in the call to acquire than the context present
            // when the releasable is closed
            try (ThreadContext.StoredContext ignore = context.newStoredContext(false)) {
                context.putHeader("foo", "bar");
                context.putTransient("bar", "baz");
                // test both with and without a executor name
                block.acquire(future, ThreadPool.Names.GENERIC, true);
                block.acquire(future2, null, true);
            }
            assertFalse(future.isDone());
        }
        future.get(1, TimeUnit.HOURS).close();
        future2.get(1, TimeUnit.HOURS).close();
    }

    protected Releasable blockAndWait() throws InterruptedException {
        CountDownLatch blockAcquired = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);
        CountDownLatch blockReleased = new CountDownLatch(1);
        boolean throwsException = randomBoolean();
        IndexShardClosedException exception = new IndexShardClosedException(new ShardId("blubb", "id", 0));
        threadPool.generic().execute(() -> {
                try {
                    block.blockOperations(1, TimeUnit.MINUTES, () -> {
                        try {
                            blockAcquired.countDown();
                            releaseBlock.await();
                            if (throwsException) {
                                throw exception;
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    });
                } catch (Exception e) {
                    if (e != exception) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    blockReleased.countDown();
                }
            });
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

    public void testActiveOperationsCount() throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> future1 = new PlainActionFuture<>();
        block.acquire(future1, ThreadPool.Names.GENERIC, true);
        assertTrue(future1.isDone());
        assertThat(block.getActiveOperationsCount(), equalTo(1));

        PlainActionFuture<Releasable> future2 = new PlainActionFuture<>();
        block.acquire(future2, ThreadPool.Names.GENERIC, true);
        assertTrue(future2.isDone());
        assertThat(block.getActiveOperationsCount(), equalTo(2));

        future1.get().close();
        assertThat(block.getActiveOperationsCount(), equalTo(1));
        future1.get().close(); // check idempotence
        assertThat(block.getActiveOperationsCount(), equalTo(1));
        future2.get().close();
        assertThat(block.getActiveOperationsCount(), equalTo(0));

        try (Releasable releasable = blockAndWait()) {
            assertThat(block.getActiveOperationsCount(), equalTo(0));
        }

        PlainActionFuture<Releasable> future3 = new PlainActionFuture<>();
        block.acquire(future3, ThreadPool.Names.GENERIC, true);
        assertTrue(future3.isDone());
        assertThat(block.getActiveOperationsCount(), equalTo(1));
        future3.get().close();
        assertThat(block.getActiveOperationsCount(), equalTo(0));
    }
}
