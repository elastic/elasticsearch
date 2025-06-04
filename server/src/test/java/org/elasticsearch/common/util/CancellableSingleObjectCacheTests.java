/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class CancellableSingleObjectCacheTests extends ESTestCase {

    public void testNoPendingRefreshIfAlreadyCancelled() {
        final TestCache testCache = new TestCache();
        final TestFuture future = new TestFuture();
        testCache.get("foo", () -> true, future);
        testCache.assertPendingRefreshes(0);
        assertTrue(future.isDone());
        expectThrows(ExecutionException.class, TaskCancelledException.class, future::get);
    }

    public void testListenersCompletedByRefresh() throws ExecutionException {
        final TestCache testCache = new TestCache();

        // The first get() calls the refresh function
        final TestFuture future0 = new TestFuture();
        testCache.get("foo", () -> false, future0);
        testCache.assertPendingRefreshes(1);

        // The second get() with a matching key does not refresh again
        final TestFuture future1 = new TestFuture();
        testCache.get("foo", () -> false, future1);
        assertFalse(future0.isDone());
        assertFalse(future1.isDone());
        testCache.assertPendingRefreshes(1);
        testCache.completeNextRefresh("foo", 1);
        assertThat(future0.result(), equalTo(1));
        assertThat(future0.result(), sameInstance(future1.result()));

        // A further get() call with a matching key re-uses the cached value
        final TestFuture future2 = new TestFuture();
        testCache.get("foo", () -> false, future2);
        testCache.assertNoPendingRefreshes();
        assertThat(future2.result(), sameInstance(future1.result()));

        // A call with a different key triggers another refresh
        final TestFuture future3 = new TestFuture();
        testCache.get("bar", () -> false, future3);
        assertFalse(future3.isDone());
        testCache.assertPendingRefreshes(1);
        testCache.completeNextRefresh("bar", 2);
        assertThat(future3.result(), equalTo(2));
    }

    public void testListenerCompletedByRefreshEvenIfDiscarded() throws ExecutionException {
        final TestCache testCache = new TestCache();

        // This computation is discarded before it completes.
        final TestFuture future1 = new TestFuture();
        final AtomicBoolean future1Cancelled = new AtomicBoolean();
        testCache.get("foo", future1Cancelled::get, future1);
        future1Cancelled.set(true);
        testCache.assertPendingRefreshes(1);
        assertFalse(future1.isDone());

        // However the refresh continues and makes its result available to a later get() call for the same value.
        final TestFuture future2 = new TestFuture();
        testCache.get("foo", () -> false, future2);
        testCache.assertPendingRefreshes(1);
        testCache.completeNextRefresh("foo", 1);
        assertThat(future2.result(), equalTo(1));

        // We expect the first listener to have been completed with a cancellation exception when detected in the ensureNotCancelled() call.
        assertTrue(future1.isDone());
        expectThrows(ExecutionException.class, TaskCancelledException.class, future1::result);
    }

    public void testBothListenersReceiveTaskCancelledExceptionWhenBothSupersededAndNewTasksAreCancelled() {
        final TestCache testCache = new TestCache();

        // This computation is superseded and then cancelled.
        final AtomicBoolean isCancelled = new AtomicBoolean();
        final TestFuture future1 = new TestFuture();
        testCache.get("foo", isCancelled::get, future1);
        testCache.assertPendingRefreshes(1);

        // A second get() call that supersedes the original refresh and starts another one, but will be cancelled as well.
        final TestFuture future2 = new TestFuture();
        testCache.get("bar", isCancelled::get, future2);
        testCache.assertPendingRefreshes(2);

        testCache.assertNextRefreshCancelled();
        assertFalse(future1.isDone());
        testCache.assertPendingRefreshes(1);
        assertFalse(future2.isDone());

        isCancelled.set(true);
        // This next refresh should also fail with a cancellation exception.
        testCache.completeNextRefresh("bar", 1);
        expectThrows(ExecutionException.class, TaskCancelledException.class, future1::result);
        expectThrows(ExecutionException.class, TaskCancelledException.class, future2::result);
    }

    public void testListenerCompletedWithCancellationExceptionIfRefreshCancelled() throws ExecutionException {
        final TestCache testCache = new TestCache();

        // This computation is discarded before it completes.
        final TestFuture future1 = new TestFuture();
        final AtomicBoolean future1Cancelled = new AtomicBoolean();
        testCache.get("foo", future1Cancelled::get, future1);
        future1Cancelled.set(true);
        testCache.assertPendingRefreshes(1);

        assertFalse(future1.isDone());

        // A second get() call with a non-matching key cancels the original refresh and starts another one
        final TestFuture future2 = new TestFuture();
        testCache.get("bar", () -> false, future2);
        testCache.assertPendingRefreshes(2);
        testCache.assertNextRefreshCancelled();
        expectThrows(ExecutionException.class, TaskCancelledException.class, future1::result);
        testCache.completeNextRefresh("bar", 2);
        assertThat(future2.result(), equalTo(2));
    }

    public void testListenerCompletedWithFresherInputIfSuperseded() throws ExecutionException {
        final TestCache testCache = new TestCache();

        // This computation is superseded before it completes.
        final TestFuture future1 = new TestFuture();
        testCache.get("foo", () -> false, future1);
        testCache.assertPendingRefreshes(1);

        // A second get() call with a non-matching key supersedes the original refresh and starts another one
        final TestFuture future2 = new TestFuture();
        testCache.get("bar", () -> false, future2);
        testCache.assertPendingRefreshes(2);
        testCache.assertNextRefreshCancelled();
        assertFalse(future1.isDone());

        testCache.completeNextRefresh("bar", 2);
        assertThat(future2.result(), equalTo(2));
        assertThat(future1.result(), equalTo(2));
    }

    public void testRunsCancellationChecksEvenWhenSuperseded() {
        final TestCache testCache = new TestCache();

        // This computation is superseded and then cancelled.
        final AtomicBoolean isCancelled = new AtomicBoolean();
        final TestFuture future1 = new TestFuture();
        testCache.get("foo", isCancelled::get, future1);
        testCache.assertPendingRefreshes(1);

        // A second get() call with a non-matching key supersedes the original refresh and starts another one
        final TestFuture future2 = new TestFuture();
        testCache.get("bar", () -> false, future2);
        testCache.assertPendingRefreshes(2);

        testCache.assertNextRefreshCancelled();
        assertFalse(future1.isDone());

        isCancelled.set(true);
        testCache.completeNextRefresh("bar", 1);
        expectThrows(ExecutionException.class, TaskCancelledException.class, future1::result);
    }

    public void testExceptionCompletesListenersButIsNotCached() throws ExecutionException {
        final TestCache testCache = new TestCache();

        // If a refresh results in an exception then all the pending get() calls complete exceptionally
        final TestFuture future0 = new TestFuture();
        final TestFuture future1 = new TestFuture();
        testCache.get("foo", () -> false, future0);
        testCache.get("foo", () -> false, future1);
        testCache.assertPendingRefreshes(1);
        final ElasticsearchException exception = new ElasticsearchException("simulated");
        testCache.completeNextRefresh(exception);
        assertSame(exception, expectThrows(ExecutionException.class, ElasticsearchException.class, future0::result));
        assertSame(exception, expectThrows(ExecutionException.class, ElasticsearchException.class, future1::result));

        testCache.assertNoPendingRefreshes();
        // The exception is not cached, however, so a subsequent get() call with a matching key performs another refresh
        final TestFuture future2 = new TestFuture();
        testCache.get("foo", () -> false, future2);
        testCache.assertPendingRefreshes(1);
        testCache.completeNextRefresh("foo", 1);
        assertThat(future2.result(), equalTo(1));
    }

    public void testConcurrentRefreshesAndCancellation() throws InterruptedException {
        final ThreadPool threadPool = new TestThreadPool("test");
        try {
            final ThreadContext threadContext = threadPool.getThreadContext();
            final CancellableSingleObjectCache<String, String, Integer> testCache = new CancellableSingleObjectCache<>(threadContext) {
                @Override
                protected void refresh(
                    String s,
                    Runnable ensureNotCancelled,
                    BooleanSupplier supersedeIfStale,
                    ActionListener<Integer> listener
                ) {
                    threadPool.generic().execute(ActionRunnable.supply(listener, () -> {
                        ensureNotCancelled.run();
                        if (s.equals("FAIL")) {
                            throw new ElasticsearchException("simulated");
                        }
                        return s.length();
                    }));
                }

                @Override
                protected String getKey(String s) {
                    return s;
                }
            };
            final int count = 1000;
            final CountDownLatch startLatch = new CountDownLatch(1);
            final CountDownLatch finishLatch = new CountDownLatch(count);
            final BlockingQueue<Runnable> queue = ConcurrentCollections.newBlockingQueue();
            final String contextHeader = "test-context-header";

            for (int i = 0; i < count; i++) {
                final boolean cancel = randomBoolean();
                final String input = randomFrom("FAIL", "foo", "barbaz", "quux", "gruly");
                queue.offer(() -> {
                    try {
                        assertTrue(startLatch.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }

                    final ListenableFuture<Integer> stepListener = new ListenableFuture<>();
                    final AtomicBoolean isComplete = new AtomicBoolean();
                    final AtomicBoolean isCancelled = new AtomicBoolean();
                    try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                        final String contextValue = randomAlphaOfLength(10);
                        threadContext.putHeader(contextHeader, contextValue);
                        testCache.get(input, isCancelled::get, ActionListener.runBefore(stepListener, () -> {
                            assertTrue(isComplete.compareAndSet(false, true));
                            assertThat(threadContext.getHeader(contextHeader), equalTo(contextValue));
                        }));
                    }

                    final Runnable next = queue.poll();
                    if (next != null) {
                        threadPool.generic().execute(next);
                    }

                    if (cancel) {
                        isCancelled.set(true);
                    }

                    stepListener.addListener(ActionListener.wrap(len -> {
                        finishLatch.countDown();
                        assertThat(len, equalTo(input.length()));
                        assertNotEquals("FAIL", input);
                    }, e -> {
                        finishLatch.countDown();
                        if (e instanceof TaskCancelledException) {
                            assertTrue(cancel);
                        } else {
                            assertEquals("FAIL", input);
                        }
                    }));
                });
            }

            for (int i = 0; i < 10; i++) {
                threadPool.generic().execute(Objects.requireNonNull(queue.poll()));
            }

            startLatch.countDown();
            assertTrue(finishLatch.await(10, TimeUnit.SECONDS));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testConcurrentRefreshesWithFreshnessCheck() throws InterruptedException {
        final ThreadPool threadPool = new TestThreadPool("test");
        try {
            final CancellableSingleObjectCache<String, String, Integer> testCache = new CancellableSingleObjectCache<>(
                threadPool.getThreadContext()
            ) {
                @Override
                protected void refresh(
                    String s,
                    Runnable ensureNotCancelled,
                    BooleanSupplier supersedeIfStale,
                    ActionListener<Integer> listener
                ) {
                    threadPool.generic().execute(() -> {
                        if (supersedeIfStale.getAsBoolean()) {
                            return;
                        }

                        ActionListener.completeWith(listener, () -> {
                            ensureNotCancelled.run();
                            return s.length();
                        });
                    });
                }

                @Override
                protected String getKey(String s) {
                    return s;
                }

                @Override
                protected boolean isFresh(String currentKey, String newKey) {
                    return newKey.length() <= currentKey.length();
                }
            };
            final int count = 1000;
            final CountDownLatch startLatch = new CountDownLatch(1);
            final CountDownLatch finishLatch = new CountDownLatch(count);
            final BlockingQueue<Runnable> queue = ConcurrentCollections.newBlockingQueue();

            for (int i = 0; i < count; i++) {
                final boolean cancel = randomBoolean();
                final String input = randomFrom("foo", "barbaz", "quux", "gruly");
                queue.offer(() -> {
                    try {
                        assertTrue(startLatch.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }

                    final ListenableFuture<Integer> stepListener = new ListenableFuture<>();
                    final AtomicBoolean isComplete = new AtomicBoolean();
                    final AtomicBoolean isCancelled = new AtomicBoolean();
                    testCache.get(
                        input,
                        isCancelled::get,
                        ActionListener.runBefore(stepListener, () -> assertTrue(isComplete.compareAndSet(false, true)))
                    );

                    final Runnable next = queue.poll();
                    if (next != null) {
                        threadPool.generic().execute(next);
                    }

                    if (cancel) {
                        isCancelled.set(true);
                    }

                    stepListener.addListener(ActionListener.wrap(len -> {
                        finishLatch.countDown();
                        assertThat(len, greaterThanOrEqualTo(input.length()));
                    }, e -> {
                        finishLatch.countDown();
                        if (e instanceof TaskCancelledException) {
                            assertTrue(cancel);
                        } else {
                            fail(e);
                        }
                    }));
                });
            }

            for (int i = 0; i < 10; i++) {
                threadPool.generic().execute(Objects.requireNonNull(queue.poll()));
            }

            startLatch.countDown();
            assertTrue(finishLatch.await(10, TimeUnit.SECONDS));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testForegroundRefreshCanBeCancelled() throws InterruptedException {
        final CyclicBarrier barrier = new CyclicBarrier(2);

        final CancellableSingleObjectCache<String, String, Integer> testCache = new CancellableSingleObjectCache<>(testThreadContext) {
            @Override
            protected void refresh(
                String s,
                Runnable ensureNotCancelled,
                BooleanSupplier supersedeIfStale,
                ActionListener<Integer> listener
            ) {
                ActionListener.completeWith(listener, () -> {
                    safeAwait(barrier); // main-thread barrier 2; cancelled-thread barrier 1
                    safeAwait(barrier); // main-thread barrier 3; cancelled-thread barrier 2
                    ensureNotCancelled.run();
                    if (s.equals("cancelled")) {
                        throw new AssertionError("should have been cancelled");
                    } else {
                        return s.length();
                    }
                });
            }

            @Override
            protected String getKey(String s) {
                return s;
            }
        };

        final TestFuture cancelledFuture = new TestFuture();
        final TestFuture successfulFuture = new TestFuture();

        final AtomicBoolean isCancelled = new AtomicBoolean();
        final Thread cancelledThread = new Thread(() -> {
            testCache.get("cancelled", isCancelled::get, cancelledFuture);
            safeAwait(barrier); // cancelled-thread barrier 3
        }, "cancelled-thread");

        cancelledThread.start();
        safeAwait(barrier); // main-thread barrier 1
        isCancelled.set(true);
        testCache.get("successful", () -> false, successfulFuture);
        cancelledThread.join();

        expectThrows(ExecutionException.class, TaskCancelledException.class, cancelledFuture::result);
    }

    public void testClearCurrentCachedItem() throws ExecutionException {
        final TestCache testCache = new TestCache();

        // The first get() calls the refresh function.
        final TestFuture future0 = new TestFuture();
        testCache.get("foo", () -> false, future0);
        testCache.assertPendingRefreshes(1);
        testCache.completeNextRefresh("foo", 1);
        assertThat(future0.result(), equalTo(1));

        testCache.clearCurrentCachedItem();

        // The second get() with a matching key will execute a refresh since the cached item was cleared.
        final TestFuture future1 = new TestFuture();
        testCache.get("foo", () -> false, future1);
        testCache.assertPendingRefreshes(1);
        testCache.completeNextRefresh("foo", 2);
        assertThat(future1.result(), equalTo(2));
    }

    private static final ThreadContext testThreadContext = new ThreadContext(Settings.EMPTY);

    private static class TestCache extends CancellableSingleObjectCache<String, String, Integer> {

        private final LinkedList<ListenableFuture<Function<String, Integer>>> pendingRefreshes = new LinkedList<>();

        private TestCache() {
            super(testThreadContext);
        }

        @Override
        protected void refresh(
            String input,
            Runnable ensureNotCancelled,
            BooleanSupplier supersedeIfStale,
            ActionListener<Integer> listener
        ) {
            final ListenableFuture<Function<String, Integer>> stepListener = new ListenableFuture<>();
            pendingRefreshes.offer(stepListener);
            stepListener.addListener(listener.delegateFailureAndWrap((l, f) -> {
                if (supersedeIfStale.getAsBoolean()) {
                    return;
                }
                ActionListener.completeWith(l, () -> {
                    ensureNotCancelled.run();
                    return f.apply(input);
                });
            }));
        }

        @Override
        protected String getKey(String s) {
            return s;
        }

        void assertPendingRefreshes(int expected) {
            assertThat(pendingRefreshes.size(), equalTo(expected));
        }

        void assertNoPendingRefreshes() {
            assertThat(pendingRefreshes, empty());
        }

        void completeNextRefresh(String key, int value) {
            nextRefresh().onResponse(k -> {
                assertThat(k, equalTo(key));
                return value;
            });
        }

        void completeNextRefresh(Exception e) {
            nextRefresh().onFailure(e);
        }

        void assertNextRefreshCancelled() {
            nextRefresh().onResponse(k -> { throw new AssertionError("should not be called"); });
        }

        private ListenableFuture<Function<String, Integer>> nextRefresh() {
            assertThat(pendingRefreshes, not(empty()));
            final ListenableFuture<Function<String, Integer>> nextRefresh = pendingRefreshes.poll();
            assertNotNull(nextRefresh);
            return nextRefresh;
        }

    }

    private static class TestFuture extends PlainActionFuture<Integer> {

        private final AtomicBoolean isCompleted = new AtomicBoolean();

        @Override
        public void onResponse(Integer result) {
            assertTrue(isCompleted.compareAndSet(false, true));
            super.onResponse(result);
        }

        @Override
        public void onFailure(Exception e) {
            assertTrue(isCompleted.compareAndSet(false, true));
            super.onFailure(e);
        }
    }

}
