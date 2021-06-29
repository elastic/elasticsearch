/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
                    awaitSafe(barrier);

                    final AtomicBoolean isComplete = new AtomicBoolean();
                    if (completerThreads == 1 && postComplete.get()) {
                        // If there are multiple completer threads then onResponse might return on one thread, and hence postComplete is
                        // set, before the other completer thread notifies all the listeners. OTOH with one completer thread we know that
                        // postComplete indicates that the listeners were already notified.
                        future.addListener(new ActionListener<>() {
                            @Override
                            public void onResponse(Void response) {
                                assertTrue(isComplete.compareAndSet(false, true));
                                assertThat(Thread.currentThread().getName(), equalTo(threadName));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                throw new AssertionError("unexpected", e);
                            }
                        });
                        assertTrue(isComplete.get());
                    } else {
                        final PlainActionFuture<String> completingThreadNameFuture = new PlainActionFuture<>();
                        future.addListener(new ActionListener<>() {
                            @Override
                            public void onResponse(Void response) {
                                assertTrue(isComplete.compareAndSet(false, true));
                                completingThreadNameFuture.onResponse(Thread.currentThread().getName());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                throw new AssertionError("unexpected", e);
                            }
                        });

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
                    awaitSafe(barrier);

                    preComplete.set(true);
                    future.onResponse(null);
                    postComplete.set(true);

                }, threadName);
            }

            threads[i].start();
        }

        awaitSafe(barrier);
        for (final Thread thread : threads) {
            thread.join();
        }

    }

    private static void awaitSafe(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }
}
