/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class RewriteableTests extends ESTestCase {

    public void testRewrite() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(null, null, null);
        TestRewriteable rewrite = Rewriteable.rewrite(
            new TestRewriteable(randomIntBetween(0, Rewriteable.MAX_REWRITE_ROUNDS)),
            context,
            randomBoolean()
        );
        assertEquals(0, rewrite.numRewrites);
        IllegalStateException ise = expectThrows(
            IllegalStateException.class,
            () -> Rewriteable.rewrite(new TestRewriteable(Rewriteable.MAX_REWRITE_ROUNDS + 1), context)
        );
        assertEquals("too many rewrite rounds, rewriteable might return new objects even if they are not rewritten", ise.getMessage());
        ise = expectThrows(
            IllegalStateException.class,
            () -> Rewriteable.rewrite(new TestRewriteable(Rewriteable.MAX_REWRITE_ROUNDS + 1, true), context, true)
        );
        assertEquals("async actions are left after rewrite", ise.getMessage());
    }

    public void testRewriteAndFetch() throws ExecutionException, InterruptedException {
        QueryRewriteContext context = new QueryRewriteContext(null, null, null);
        PlainActionFuture<TestRewriteable> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(new TestRewriteable(randomIntBetween(0, Rewriteable.MAX_REWRITE_ROUNDS), true), context, future);
        TestRewriteable rewrite = future.get();
        assertEquals(0, rewrite.numRewrites);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> {
            PlainActionFuture<TestRewriteable> f = new PlainActionFuture<>();
            Rewriteable.rewriteAndFetch(new TestRewriteable(Rewriteable.MAX_REWRITE_ROUNDS + 1, true), context, f);
            try {
                f.get();
            } catch (ExecutionException e) {
                throw e.getCause(); // we expect the underlying exception here
            }
        });
        assertEquals("too many rewrite rounds, rewriteable might return new objects even if they are not rewritten", ise.getMessage());
    }

    public void testRewriteList() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(null, null, null);
        List<TestRewriteable> rewriteableList = new ArrayList<>();
        int numInstances = randomIntBetween(1, 10);
        rewriteableList.add(new TestRewriteable(randomIntBetween(1, Rewriteable.MAX_REWRITE_ROUNDS)));
        for (int i = 0; i < numInstances; i++) {
            rewriteableList.add(new TestRewriteable(randomIntBetween(0, Rewriteable.MAX_REWRITE_ROUNDS)));
        }
        List<TestRewriteable> rewrittenList = Rewriteable.rewrite(rewriteableList, context);
        assertNotSame(rewrittenList, rewriteableList);
        for (TestRewriteable instance : rewrittenList) {
            assertEquals(0, instance.numRewrites);
        }
        rewriteableList = Collections.emptyList();
        assertSame(rewriteableList, Rewriteable.rewrite(rewriteableList, context));
        rewriteableList = null;
        assertNull(Rewriteable.rewrite(rewriteableList, context));

        rewriteableList = new ArrayList<>();
        for (int i = 0; i < numInstances; i++) {
            rewriteableList.add(new TestRewriteable(0));
        }
        assertSame(rewriteableList, Rewriteable.rewrite(rewriteableList, context));
    }

    public void testRewriteAndFetchWithExecutor() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            Executor searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
            AtomicReference<String> completionThreadName = new AtomicReference<>();
            CountDownLatch completionLatch = new CountDownLatch(1);
            CountDownLatch asyncStartLatch = new CountDownLatch(1);
            TestRewriteable rewrite = new TestRewriteableWithLatchAndCapture(1, asyncStartLatch, completionThreadName);
            Rewriteable.rewriteAndFetch(rewrite, new QueryRewriteContext(null, null, null), searchExecutor, new ActionListener<>() {
                @Override
                public void onResponse(TestRewriteable result) {
                    completionThreadName.set(Thread.currentThread().getName());
                    completionLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    completionThreadName.set(Thread.currentThread().getName());
                    completionLatch.countDown();
                }
            });
            asyncStartLatch.countDown();

            assertTrue("Timed out waiting for rewrite to complete", completionLatch.await(10, TimeUnit.SECONDS));
            assertThat("Should complete on search thread pool", completionThreadName.get(), containsString("search"));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testRewriteAndFetchWithExecutorOnFailure() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            Executor searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
            AtomicReference<String> completionThreadName = new AtomicReference<>();
            AtomicReference<Exception> caughtException = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            TestRewriteable rewrite = new FailingTestRewriteable(randomIntBetween(1, 3));
            Rewriteable.rewriteAndFetch(rewrite, new QueryRewriteContext(null, null, null), searchExecutor, new ActionListener<>() {
                @Override
                public void onResponse(TestRewriteable result) {
                    completionThreadName.set(Thread.currentThread().getName());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    completionThreadName.set(Thread.currentThread().getName());
                    caughtException.set(e);
                    latch.countDown();
                }
            });

            assertTrue("Timed out waiting for rewrite to complete", latch.await(10, TimeUnit.SECONDS));
            assertNotNull("Exception should be caught", caughtException.get());
            assertThat(caughtException.get().getMessage(), containsString("Simulated async failure"));
            assertThat("Should complete on search thread pool", completionThreadName.get(), containsString("search"));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testRewriteAndFetchWithExecutorNoAsyncActions() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            Executor searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
            Thread callingThread = Thread.currentThread();
            AtomicReference<Thread> completionThread = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            TestRewriteable rewrite = new TestRewriteable(randomIntBetween(0, 5), false);
            Rewriteable.rewriteAndFetch(rewrite, new QueryRewriteContext(null, null, null), searchExecutor, new ActionListener<>() {
                @Override
                public void onResponse(TestRewriteable result) {
                    completionThread.set(Thread.currentThread());
                    assertEquals(0, result.numRewrites);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not fail: " + e.getMessage());
                    latch.countDown();
                }
            });

            assertTrue("Timed out waiting for rewrite to complete", latch.await(10, TimeUnit.SECONDS));
            assertSame("Should complete on calling thread when no async actions", callingThread, completionThread.get());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testRewriteAndFetchWithExecutorAsyncOnDifferentThread() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            Executor searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
            AtomicReference<String> asyncThreadName = new AtomicReference<>();
            AtomicReference<String> completionThreadName = new AtomicReference<>();
            CountDownLatch completionLatch = new CountDownLatch(1);
            CountDownLatch asyncStartLatch = new CountDownLatch(1);
            TestRewriteable rewrite = new TestRewriteableWithLatchAndCapture(1, asyncStartLatch, asyncThreadName);
            Rewriteable.rewriteAndFetch(rewrite, new QueryRewriteContext(null, null, null), searchExecutor, new ActionListener<>() {
                @Override
                public void onResponse(TestRewriteable result) {
                    completionThreadName.set(Thread.currentThread().getName());
                    completionLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    completionThreadName.set(Thread.currentThread().getName());
                    completionLatch.countDown();
                }
            });
            asyncStartLatch.countDown();

            assertTrue("Timed out waiting for rewrite to complete", completionLatch.await(10, TimeUnit.SECONDS));
            String asyncThread = asyncThreadName.get();
            assertNotNull("Async thread name should be captured", asyncThread);

            String completionThread = completionThreadName.get();
            assertNotNull("Completion thread name should be captured", completionThread);
            assertThat("Should complete on search thread pool", completionThread, containsString("search"));
            assertThat("Async and completion threads should differ", completionThread, not(asyncThread));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static class TestRewriteable implements Rewriteable<TestRewriteable> {

        final int numRewrites;
        final boolean fetch;
        final Supplier<Boolean> supplier;

        TestRewriteable(int numRewrites) {
            this(numRewrites, false, null);
        }

        TestRewriteable(int numRewrites, boolean fetch) {
            this(numRewrites, fetch, null);
        }

        TestRewriteable(int numRewrites, boolean fetch, Supplier<Boolean> supplier) {
            this.numRewrites = numRewrites;
            this.fetch = fetch;
            this.supplier = supplier;
        }

        @Override
        public TestRewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            if (numRewrites == 0) {
                return this;
            }
            if (supplier != null && supplier.get() == null) {
                return this;
            }
            if (supplier != null) {
                assertTrue(supplier.get());
            }
            if (fetch) {
                SetOnce<Boolean> setOnce = new SetOnce<>();
                ctx.registerAsyncAction((c, l) -> {
                    Runnable r = () -> {
                        setOnce.set(Boolean.TRUE);
                        l.onResponse(null);
                    };
                    if (randomBoolean()) {
                        new Thread(r).start();
                    } else {
                        r.run();
                    }
                });
                return new TestRewriteable(numRewrites - 1, fetch, setOnce::get);
            }
            return new TestRewriteable(numRewrites - 1, fetch, null);
        }
    }

    private static class FailingTestRewriteable extends TestRewriteable {

        FailingTestRewriteable(int numRewrites) {
            super(numRewrites, true, null);
        }

        @Override
        public TestRewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            if (numRewrites == 0) {
                return this;
            }
            SetOnce<Boolean> setOnce = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> {
                // Simulate failure on a separate thread (like transport thread)
                new Thread(() -> { l.onFailure(new RuntimeException("Simulated async failure")); }).start();
            });
            return new FailingTestRewriteable(numRewrites - 1);
        }
    }

    private static class TestRewriteableWithLatchAndCapture extends TestRewriteable {

        private final CountDownLatch asyncStartLatch;
        private final AtomicReference<String> asyncThreadCapture;

        TestRewriteableWithLatchAndCapture(int numRewrites, CountDownLatch asyncStartLatch, AtomicReference<String> asyncThreadCapture) {
            super(numRewrites, true, null);
            this.asyncStartLatch = asyncStartLatch;
            this.asyncThreadCapture = asyncThreadCapture;
        }

        @Override
        public TestRewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            if (numRewrites == 0) {
                return this;
            }
            SetOnce<Boolean> setOnce = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> {
                new Thread(() -> {
                    try {
                        asyncStartLatch.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    asyncThreadCapture.set(Thread.currentThread().getName());
                    setOnce.set(Boolean.TRUE);
                    l.onResponse(null);
                }, "simulated-transport-thread").start();
            });
            return new TestRewriteable(numRewrites - 1, false, setOnce::get);
        }
    }
}
