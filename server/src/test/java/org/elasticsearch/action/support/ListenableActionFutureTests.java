/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transports;
import org.junit.Assert;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class ListenableActionFutureTests extends ESTestCase {

    public void testListenerIsCallableFromNetworkThreads() throws Exception {
        ThreadPool threadPool = new TestThreadPool("testListenerIsCallableFromNetworkThreads");
        try {
            final ListenableActionFuture<Object> future = new ListenableActionFuture<>();
            final CountDownLatch listenerCalled = new CountDownLatch(1);
            final AtomicReference<Exception> error = new AtomicReference<>();
            final Object response = new Object();
            future.addListener(new ActionListener<Object>() {
                @Override
                public void onResponse(Object o) {
                    listenerCalled.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    error.set(e);
                    listenerCalled.countDown();
                }
            });
            Thread networkThread = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    error.set(e);
                    listenerCalled.countDown();
                }

                @Override
                protected void doRun() {
                    future.onResponse(response);
                }
            }, Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX + "_testListenerIsCallableFromNetworkThread");
            networkThread.start();
            networkThread.join();
            listenerCalled.await();
            if (error.get() != null) {
                throw error.get();
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testListenersNotifiedOnCorrectThreads() throws InterruptedException {

        final int adderThreads = between(1, 5);
        final int completerThreads = between(1, 3);

        final ListenableActionFuture<Void> future = new ListenableActionFuture<>();

        final AtomicBoolean preComplete = new AtomicBoolean();
        final AtomicBoolean postComplete = new AtomicBoolean();
        final String ADDER_THREAD_NAME_PREFIX = "adder-";
        final String COMPLETER_THREAD_NAME_PREFIX = "completer-";

        final CyclicBarrier barrier = new CyclicBarrier(adderThreads + completerThreads + 1);
        final Thread[] threads = new Thread[adderThreads + completerThreads];
        for (int i = 0; i < adderThreads + completerThreads; i++) {
            if (i < adderThreads) {
                final String threadName = ADDER_THREAD_NAME_PREFIX + i;
                threads[i] = new Thread(() -> {
                    safeAwait(barrier);

                    final AtomicBoolean isComplete = new AtomicBoolean();
                    if (completerThreads == 1 && postComplete.get()) {
                        // If there are multiple completer threads then onResponse might return on one thread, and hence postComplete is
                        // set, before the other completer thread notifies all the listeners. OTOH with one completer thread we know that
                        // postComplete indicates that the listeners were already notified.
                        future.addListener(ActionTestUtils.assertNoFailureListener(ignored -> {
                            assertTrue(isComplete.compareAndSet(false, true));
                            assertThat(Thread.currentThread().getName(), equalTo(threadName));
                        }));
                        assertTrue(isComplete.get());
                    } else {
                        final PlainActionFuture<String> completingThreadNameFuture = new PlainActionFuture<>();
                        future.addListener(ActionTestUtils.assertNoFailureListener(ignored -> {
                            assertTrue(isComplete.compareAndSet(false, true));
                            completingThreadNameFuture.onResponse(Thread.currentThread().getName());
                        }));

                        final boolean incompleteAfterAdd = preComplete.get() == false;
                        final String completingThreadName = completingThreadNameFuture.actionGet(10L, TimeUnit.SECONDS);
                        if (incompleteAfterAdd) {
                            assertThat(completingThreadName, startsWith(COMPLETER_THREAD_NAME_PREFIX));
                        } else {
                            assertThat(completingThreadName, anyOf(equalTo(threadName), startsWith(COMPLETER_THREAD_NAME_PREFIX)));
                        }
                    }
                }, threadName);
            } else {
                final String threadName = COMPLETER_THREAD_NAME_PREFIX + i;
                threads[i] = new Thread(() -> {
                    safeAwait(barrier);

                    preComplete.set(true);
                    future.onResponse(null);
                    postComplete.set(true);

                }, threadName);
            }

            threads[i].start();
        }

        safeAwait(barrier);
        for (final Thread thread : threads) {
            thread.join();
        }

    }

    public void testAddedListenersReleasedOnCompletion() {
        final ListenableActionFuture<Void> future = new ListenableActionFuture<>();
        final ReachabilityChecker reachabilityChecker = new ReachabilityChecker();

        for (int i = between(1, 3); i > 0; i--) {
            future.addListener(reachabilityChecker.register(ActionListener.running(() -> {})));
        }
        reachabilityChecker.checkReachable();
        if (randomBoolean()) {
            future.onResponse(null);
        } else {
            future.onFailure(new ElasticsearchException("simulated"));
        }
        reachabilityChecker.ensureUnreachable();

        future.addListener(reachabilityChecker.register(ActionListener.running(() -> {})));
        reachabilityChecker.ensureUnreachable();
    }

    public void testExceptionWrapping() {
        assertExceptionWrapping(new ElasticsearchException("simulated"), e -> {
            assertEquals(ElasticsearchException.class, e.getClass());
            assertEquals("simulated", e.getMessage());
        });

        assertExceptionWrapping(new RuntimeException("simulated"), e -> {
            assertEquals(RuntimeException.class, e.getClass());
            assertEquals("simulated", e.getMessage());
        });

        assertExceptionWrapping(new IOException("simulated"), e -> {
            assertEquals(UncategorizedExecutionException.class, e.getClass());
            assertEquals(ExecutionException.class, e.getCause().getClass());
            assertEquals(IOException.class, e.getCause().getCause().getClass());
            assertEquals("simulated", e.getCause().getCause().getMessage());
        });

        assertExceptionWrapping(new ElasticsearchException("outer", new IOException("inner")), e -> {
            assertEquals(ElasticsearchException.class, e.getClass());
            assertEquals(IOException.class, e.getCause().getClass());
            assertEquals("outer", e.getMessage());
            assertEquals("inner", e.getCause().getMessage());
        });

        assertExceptionWrapping(new RemoteTransportException("outer", new IOException("inner")), e -> {
            assertEquals(UncategorizedExecutionException.class, e.getClass());
            assertEquals(IOException.class, e.getCause().getClass());
            assertEquals("Failed execution", e.getMessage());
            assertEquals("inner", e.getCause().getMessage());
        });
    }

    private void assertExceptionWrapping(Exception exception, Consumer<Exception> assertException) {
        final AtomicInteger assertCount = new AtomicInteger();
        final BiConsumer<ActionListener<Void>, Exception> exceptionCheck = (l, e) -> {
            assertException.accept(e);
            assertCount.incrementAndGet();
        };
        final var future = new ListenableActionFuture<Void>();

        future.addListener(ActionListener.<Void>running(Assert::fail).delegateResponse(exceptionCheck));

        future.onFailure(exception);
        assertEquals(1, assertCount.get());

        exceptionCheck.accept(null, expectThrows(Exception.class, future::actionResult));
        assertEquals(2, assertCount.get());

        future.addListener(ActionListener.<Void>running(Assert::fail).delegateResponse(exceptionCheck));
        assertEquals(3, assertCount.get());
    }
}
