/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SubscribableListenerTests extends ESTestCase {

    private static class OrderAssertingRunnable implements Runnable {
        private final int index;
        private final AtomicInteger order;

        OrderAssertingRunnable(int index, AtomicInteger order) {
            this.index = index;
            this.order = order;
        }

        @Override
        public void run() {
            assertTrue(order.compareAndSet(index, index + 1));
        }
    }

    public void testSubscriptionOrder() {
        var listener = new SubscribableListener<>();
        var order = new AtomicInteger();

        var subscriberCount = between(0, 4);
        for (int i = 0; i < subscriberCount; i++) {
            listener.addListener(ActionListener.running(new OrderAssertingRunnable(i, order)));
        }

        assertEquals(0, order.get());

        if (randomBoolean()) {
            listener.onResponse(new Object());
        } else {
            listener.onFailure(new ElasticsearchException("test"));
        }

        assertEquals(subscriberCount, order.get());
        listener.addListener(ActionListener.running(new OrderAssertingRunnable(subscriberCount, order)));
        assertEquals(subscriberCount + 1, order.get());
        listener.addListener(ActionListener.running(new OrderAssertingRunnable(subscriberCount + 1, order)));
        assertEquals(subscriberCount + 2, order.get());
    }

    public void testOnResponse() {
        var listener = new SubscribableListener<>();
        var order = new AtomicInteger();
        var expectedResponse = new Object();

        IntFunction<ActionListener<Object>> listenerFactory = i -> ActionListener.runAfter(
            ActionTestUtils.assertNoFailureListener(o -> assertSame(o, expectedResponse)),
            new OrderAssertingRunnable(i, order)
        );

        var subscriberCount = between(0, 4);
        for (int i = 0; i < subscriberCount; i++) {
            listener.addListener(listenerFactory.apply(i));
        }

        assertEquals(0, order.get());
        listener.onResponse(expectedResponse);
        assertEquals(subscriberCount, order.get());

        assertEquals(subscriberCount, order.get());
        listener.addListener(ActionListener.running(new OrderAssertingRunnable(subscriberCount, order)));
        assertEquals(subscriberCount + 1, order.get());

        if (randomBoolean()) {
            listener.onResponse(new Object());
        } else {
            listener.onFailure(new ElasticsearchException("test"));
        }

        listener.addListener(ActionListener.running(new OrderAssertingRunnable(subscriberCount + 1, order)));
        assertEquals(subscriberCount + 2, order.get());
    }

    public void testOnFailure() {
        var listener = new SubscribableListener<>();
        var order = new AtomicInteger();
        var expectedException = new ElasticsearchException("test");

        IntFunction<ActionListener<Object>> listenerFactory = i -> ActionListener.runAfter(
            ActionListener.wrap(o -> fail(), e -> assertSame(e, expectedException)),
            new OrderAssertingRunnable(i, order)
        );

        var subscriberCount = between(0, 4);
        for (int i = 0; i < subscriberCount; i++) {
            listener.addListener(listenerFactory.apply(i));
        }

        assertEquals(0, order.get());
        listener.onFailure(expectedException);
        assertEquals(subscriberCount, order.get());

        assertEquals(subscriberCount, order.get());
        listener.addListener(ActionListener.running(new OrderAssertingRunnable(subscriberCount, order)));
        assertEquals(subscriberCount + 1, order.get());

        if (randomBoolean()) {
            listener.onResponse(new Object());
        } else {
            listener.onFailure(new ElasticsearchException("test"));
        }

        listener.addListener(ActionListener.running(new OrderAssertingRunnable(subscriberCount + 1, order)));
        assertEquals(subscriberCount + 2, order.get());
    }

    public void testThreadContext() {
        final var listener = new SubscribableListener<>();
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var subscriberCount = between(1, 5);
        final var completedListeners = new AtomicInteger();

        for (int i = 0; i < subscriberCount; i++) {
            try (var ignored = threadContext.stashContext()) {
                final var headerValue = randomAlphaOfLength(5);
                threadContext.putHeader("test-header", headerValue);
                listener.addListener(ActionListener.running(() -> {
                    assertEquals(headerValue, threadContext.getHeader("test-header"));
                    completedListeners.incrementAndGet();
                }), EsExecutors.DIRECT_EXECUTOR_SERVICE, threadContext);
            }
        }

        assertEquals(0, completedListeners.get());
        listener.onResponse(null);
        assertEquals(subscriberCount, completedListeners.get());
    }

    public void testConcurrency() throws InterruptedException {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var listener = new SubscribableListener<>();
        final var subscriberThreads = between(0, 10);
        final var completerThreads = between(1, 10);
        final var barrier = new CyclicBarrier(subscriberThreads + completerThreads);

        final var completerThread = new AtomicReference<String>();
        final var winningValue = new AtomicReference<>();
        final var threads = new ArrayList<Thread>();
        final var responses = new HashMap<String, Object>();
        for (int i = 0; i < subscriberThreads; i++) {
            final var threadName = "subscriber-" + i;
            threads.add(new Thread(() -> {
                safeAwait(barrier);
                try (var ignored = threadContext.stashContext()) {
                    final var headerValue = randomAlphaOfLength(5);
                    threadContext.putHeader("test-header", headerValue);
                    listener.addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(Object o) {
                            assertEquals(headerValue, threadContext.getHeader("test-header"));

                            winningValue.compareAndSet(null, o);
                            assertSame(winningValue.get(), o);

                            var currentThreadName = Thread.currentThread().getName();
                            if (currentThreadName.equals(threadName) == false) {
                                completerThread.compareAndSet(null, currentThreadName);
                                assertEquals(completerThread.get(), currentThreadName);
                                assertSame(responses.get(currentThreadName), o);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onResponse(e);
                        }
                    }, EsExecutors.DIRECT_EXECUTOR_SERVICE, threadContext);
                }
            }, threadName));
        }
        for (int i = 0; i < completerThreads; i++) {
            final var threadName = "completer-" + i;
            final var thisResponse = randomFrom(new Object(), new ElasticsearchException("test"));
            responses.put(threadName, thisResponse);
            threads.add(new Thread(() -> {
                safeAwait(barrier);
                if (thisResponse instanceof Exception e) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(thisResponse);
                }
            }, threadName));
        }

        for (final var thread : threads) {
            thread.start();
        }
        for (final var thread : threads) {
            thread.join();
        }
    }

    public void testForkingAndInterrupts() throws Exception {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var testThread = Thread.currentThread();

        final var interrupt = randomBoolean();
        if (interrupt) {
            testThread.interrupt();
        }

        final var completion = new AtomicBoolean();
        final var executorThreadPrefix = randomAlphaOfLength(10);
        final var executor = EsExecutors.newScaling(
            executorThreadPrefix,
            1,
            1,
            10,
            TimeUnit.SECONDS,
            true,
            EsExecutors.daemonThreadFactory(executorThreadPrefix),
            threadContext
        );

        try (var refs = new RefCountingRunnable(() -> completion.set(true))) {
            final var expectedResult = new Object();
            final var assertingListener = ActionTestUtils.assertNoFailureListener(result -> assertSame(expectedResult, result));

            final var listener = new SubscribableListener<>();
            final var headerName = "test-header";

            try (var ignored = threadContext.stashContext()) {
                final var headerValue = randomAlphaOfLength(10);
                threadContext.putHeader(headerName, headerValue);
                listener.addListener(ActionListener.releaseAfter(assertingListener.map(result -> {
                    assertNotSame(testThread, Thread.currentThread());
                    assertThat(Thread.currentThread().getName(), containsString(executorThreadPrefix));
                    assertEquals(headerValue, threadContext.getHeader(headerName));
                    return result;
                }), refs.acquire()), executor, threadContext);
            }

            assertFalse(listener.isDone());
            listener.onResponse(expectedResult);
            assertTrue(listener.isDone());
            assertSame(expectedResult, listener.rawResult());

            try (var ignored = threadContext.stashContext()) {
                final var headerValue = randomAlphaOfLength(10);
                threadContext.putHeader(headerName, headerValue);
                listener.addListener(ActionListener.releaseAfter(assertingListener.map(result -> {
                    assertSame(testThread, Thread.currentThread());
                    assertEquals(headerValue, threadContext.getHeader(headerName));
                    return result;
                }), refs.acquire()), executor, threadContext);
            }

            assertEquals(interrupt, Thread.interrupted());
        } finally {
            ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        }

        assertTrue(completion.get());
    }

    public void testTimeoutBeforeCompletion() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var headerName = "test-header-name";
        final var headerValue = randomAlphaOfLength(10);

        final var timedOut = new AtomicBoolean();
        final var listener = new SubscribableListener<Void>();
        listener.addListener(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("should not execute");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(ElasticsearchTimeoutException.class));
                assertEquals("timed out after [30s/30000ms]", e.getMessage());
                assertEquals(headerValue, threadPool.getThreadContext().getHeader(headerName));
                assertTrue(timedOut.compareAndSet(false, true));
            }
        });
        try (var ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(headerName, headerValue);
            listener.addTimeout(
                TimeValue.timeValueSeconds(30),
                threadPool,
                randomFrom(EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.generic())
            );
        }

        if (randomBoolean()) {
            deterministicTaskQueue.scheduleAt(
                deterministicTaskQueue.getCurrentTimeMillis() + randomLongBetween(
                    TimeValue.timeValueSeconds(30).millis() + 1,
                    TimeValue.timeValueSeconds(60).millis()
                ),
                () -> listener.onResponse(null)
            );
        }

        assertFalse(timedOut.get());
        assertFalse(listener.isDone());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(timedOut.get());
        assertTrue(listener.isDone());
    }

    public void testCompletionBeforeTimeout() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var complete = new AtomicBoolean();
        final var listener = new SubscribableListener<Void>();
        listener.addListener(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                assertTrue(complete.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                fail("should not fail");
            }
        });
        listener.addTimeout(
            TimeValue.timeValueSeconds(30),
            threadPool,
            randomFrom(EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.generic())
        );

        deterministicTaskQueue.scheduleAt(
            deterministicTaskQueue.getCurrentTimeMillis() + randomLongBetween(0, TimeValue.timeValueSeconds(30).millis() - 1),
            () -> listener.onResponse(null)
        );

        assertFalse(complete.get());
        assertFalse(listener.isDone());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(complete.get());
        assertTrue(listener.isDone());
    }

    public void testCreateUtils() throws Exception {
        final var succeeded = SubscribableListener.newSucceeded("result");
        assertTrue(succeeded.isDone());
        assertEquals("result", succeeded.rawResult());

        final var failed = SubscribableListener.newFailed(new ElasticsearchException("simulated"));
        assertTrue(failed.isDone());
        assertEquals("simulated", expectThrows(ElasticsearchException.class, failed::rawResult).getMessage());

        final var forkedListenerRef = new AtomicReference<ActionListener<Object>>();
        final var forked = SubscribableListener.newForked(forkedListenerRef::set);
        assertSame(forked, forkedListenerRef.get());

        final var forkFailed = SubscribableListener.newForked(l -> { throw new ElasticsearchException("simulated fork failure"); });
        assertTrue(forkFailed.isDone());
        assertEquals("simulated fork failure", expectThrows(ElasticsearchException.class, forkFailed::rawResult).getMessage());
    }

    public void testAndThenSuccess() {
        final var initialListener = new SubscribableListener<>();
        final var forked = new AtomicReference<ActionListener<Object>>();
        final var result = new AtomicReference<>();

        final var chainedListener = initialListener.andThen((l, o) -> {
            forked.set(l);
            result.set(o);
        });
        assertNull(forked.get());
        assertNull(result.get());

        final var o1 = new Object();
        initialListener.onResponse(o1);
        assertSame(o1, result.get());
        assertSame(chainedListener, forked.get());
        assertFalse(chainedListener.isDone());
    }

    public void testAndThenFailure() {
        final var initialListener = new SubscribableListener<>();

        final var chainedListener = initialListener.andThen((l, o) -> fail("should not be called"));
        assertFalse(chainedListener.isDone());

        initialListener.onFailure(new ElasticsearchException("simulated"));
        assertComplete(chainedListener, "simulated");
    }

    public void testAndThenThreading() {
        runAndThenThreadingTest(true);
        runAndThenThreadingTest(false);
    }

    private static void runAndThenThreadingTest(boolean testSuccess) {
        final var completeListener = testSuccess
            ? SubscribableListener.newSucceeded(new Object())
            : SubscribableListener.newFailed(new ElasticsearchException("immediate failure"));

        assertComplete(
            completeListener.andThen(r -> fail("should not fork"), null, ActionListener::onResponse),
            testSuccess ? null : "immediate failure"
        );

        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var headerName = "test-header";
        final var expectedHeader = randomAlphaOfLength(10);

        final SubscribableListener<Object> deferredListener = new SubscribableListener<>();
        final SubscribableListener<Object> chainedListener;
        final AtomicReference<Runnable> forkedRunnable = new AtomicReference<>();

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(headerName, expectedHeader);
            chainedListener = deferredListener.andThen(
                r -> assertTrue(forkedRunnable.compareAndSet(null, r)),
                threadContext,
                (l, response) -> {
                    assertEquals(expectedHeader, threadContext.getHeader(headerName));
                    l.onResponse(response);
                }
            );
        }

        assertFalse(chainedListener.isDone());
        assertNull(forkedRunnable.get());

        final AtomicBoolean isComplete = new AtomicBoolean();

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(headerName, randomAlphaOfLength(10));
            chainedListener.addListener(ActionListener.running(() -> {
                assertEquals(expectedHeader, threadContext.getHeader(headerName));
                assertTrue(isComplete.compareAndSet(false, true));
            }));
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(headerName, randomAlphaOfLength(10));
            if (testSuccess) {
                deferredListener.onResponse(new Object());
            } else {
                deferredListener.onFailure(new ElasticsearchException("simulated failure"));
            }
        }

        assertFalse(chainedListener.isDone());
        assertFalse(isComplete.get());

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(headerName, randomAlphaOfLength(10));
            forkedRunnable.get().run();
        }

        assertComplete(chainedListener, testSuccess ? null : "simulated failure");
        assertTrue(isComplete.get());
    }

    private static void assertComplete(SubscribableListener<Object> listener, @Nullable String expectedFailureMessage) {
        assertTrue(listener.isDone());
        if (expectedFailureMessage == null) {
            try {
                listener.rawResult();
            } catch (Exception e) {
                fail(e);
            }
        } else {
            assertEquals(expectedFailureMessage, expectThrows(ElasticsearchException.class, listener::rawResult).getMessage());
        }
    }
}
