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
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RefCountingListenerTests extends ESTestCase {

    public void testBasicOperation() throws InterruptedException {
        final var executed = new AtomicBoolean();
        final var exceptionCount = new AtomicInteger();
        final var threads = new Thread[between(0, 3)];
        final var exceptionLimit = Math.max(1, between(0, threads.length + 2));

        boolean async = false;
        final var startLatch = new CountDownLatch(1);

        try (var refs = new RefCountingListener(exceptionLimit, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                assertTrue(executed.compareAndSet(false, true));
                assertEquals(0, exceptionCount.get());
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(executed.compareAndSet(false, true));
                assertThat(exceptionCount.get(), greaterThan(0));
                Throwable[] suppressed = e.getSuppressed();
                if (exceptionCount.get() > exceptionLimit) {
                    assertEquals(exceptionLimit, suppressed.length);
                    for (int i = 0; i < suppressed.length; i++) {
                        Throwable throwable = suppressed[i];
                        if (i == suppressed.length - 1) {
                            assertThat(
                                throwable.getMessage(),
                                equalTo((exceptionCount.get() - exceptionLimit) + " further exceptions were dropped")
                            );
                        } else {
                            assertThat(throwable.getMessage(), equalTo("simulated"));
                        }
                    }
                } else {
                    assertEquals(exceptionCount.get() - 1, suppressed.length);
                    for (Throwable throwable : suppressed) {
                        assertThat(throwable.getMessage(), equalTo("simulated"));
                    }
                }
            }

            @Override
            public String toString() {
                return "test listener";
            }
        })) {
            assertEquals("refCounting[test listener]", refs.toString());

            var listener = refs.acquire();
            assertThat(listener.toString(), containsString("refCounting[test listener]"));
            assertFalse(refs.isFailing());
            if (randomBoolean()) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new ElasticsearchException("simulated"));
                exceptionCount.incrementAndGet();
                assertTrue(refs.isFailing());
            }

            var reachChecker = new ReachabilityChecker();
            var consumed = new AtomicBoolean();
            var consumingListener = refs.acquire(reachChecker.register(new CheckedConsumer<String, Exception>() {
                @Override
                public void accept(String s) {
                    assertEquals("test response", s);
                    consumed.set(true);
                }

                @Override
                public String toString() {
                    return "test consumer";
                }
            }));
            assertFalse(consumed.get());
            assertThat(consumingListener.toString(), containsString("refCounting[test listener][test consumer]"));
            if (randomBoolean()) {
                consumingListener.onResponse("test response");
                assertTrue(consumed.get());
            } else {
                consumingListener.onFailure(new ElasticsearchException("simulated"));
                assertFalse(consumed.get());
                exceptionCount.incrementAndGet();
            }

            assertEquals(exceptionCount.get() > 0, refs.isFailing());

            reachChecker.ensureUnreachable();
            assertThat(consumingListener.toString(), containsString("refCounting[test listener][null]"));

            for (int i = 0; i < threads.length; i++) {
                if (randomBoolean()) {
                    async = true;
                    var ref = refs.acquire();
                    threads[i] = new Thread(() -> {
                        safeAwait(startLatch);
                        assertFalse(executed.get());
                        if (randomBoolean()) {
                            ref.onResponse(null);
                        } else {
                            exceptionCount.incrementAndGet();
                            ref.onFailure(new ElasticsearchException("simulated"));
                        }
                    });
                }
            }

            assertEquals(exceptionCount.get() > 0, refs.isFailing());
            assertFalse(executed.get());
        }

        assertNotEquals(async, executed.get());

        for (Thread thread : threads) {
            if (thread != null) {
                thread.start();
            }
        }

        startLatch.countDown();

        for (Thread thread : threads) {
            if (thread != null) {
                thread.join();
            }
        }

        assertTrue(executed.get());
    }

    @SuppressWarnings("resource")
    public void testNullCheck() {
        expectThrows(NullPointerException.class, () -> new RefCountingListener(between(1, 10), null));
    }

    public void testValidation() {
        final var callCount = new AtomicInteger();
        final var refs = new RefCountingListener(Integer.MAX_VALUE, ActionListener.running(callCount::incrementAndGet));
        refs.close();
        assertEquals(1, callCount.get());

        for (int i = between(1, 5); i > 0; i--) {
            final ThrowingRunnable throwingRunnable;
            final String expectedMessage;
            if (randomBoolean()) {
                throwingRunnable = refs::acquire;
                expectedMessage = AbstractRefCounted.ALREADY_CLOSED_MESSAGE;
            } else {
                throwingRunnable = refs::close;
                expectedMessage = AbstractRefCounted.INVALID_DECREF_MESSAGE;
            }

            assertEquals(expectedMessage, expectThrows(AssertionError.class, throwingRunnable).getMessage());
            assertEquals(1, callCount.get());
        }
    }

    public void testConsumerFailure() {
        final var executed = new AtomicBoolean();
        final Runnable completeAcquiredRunOnce;
        try (var refs = new RefCountingListener(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                fail("unexpected success");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e.getMessage(), equalTo("simulated"));
                executed.set(true);
            }
        })) {
            final var listener = refs.acquire(ignored -> {
                if (randomBoolean()) {
                    throw new ElasticsearchException("simulated");
                } else {
                    throw new IOException("simulated");
                }
            });
            completeAcquiredRunOnce = new RunOnce(() -> listener.onResponse(null));
            if (randomBoolean()) {
                completeAcquiredRunOnce.run();
            }
        }
        completeAcquiredRunOnce.run();
        assertTrue(executed.get());
    }

    public void testJavaDocExample() {
        final var flag = new AtomicBoolean();
        runExample(ActionListener.running(() -> assertTrue(flag.compareAndSet(false, true))));
        assertTrue(flag.get());
    }

    private void runExample(ActionListener<Void> finalListener) {
        final var collection = randomList(10, Object::new);
        final var otherCollection = randomList(10, Object::new);
        final var flag = randomBoolean();
        @SuppressWarnings("UnnecessaryLocalVariable")
        final var executorService = DIRECT_EXECUTOR_SERVICE;
        final var results = new ArrayList<>();

        try (var refs = new RefCountingListener(finalListener)) {
            for (var item : collection) {
                if (condition(item)) {
                    runAsyncAction(item, refs.acquire(results::add));
                }
            }
            if (flag) {
                runOneOffAsyncAction(refs.acquire(results::add));
                return;
            }
            for (var item : otherCollection) {
                var itemRef = refs.acquire(); // delays completion while the background action is pending
                executorService.execute(() -> {
                    try {
                        if (condition(item)) {
                            runOtherAsyncAction(item, refs.acquire(results::add));
                        }
                    } finally {
                        itemRef.onResponse(null);
                    }
                });
            }
        }
    }

    @SuppressWarnings("unused")
    private boolean condition(Object item) {
        return randomBoolean();
    }

    @SuppressWarnings("unused")
    private void runAsyncAction(Object item, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    @SuppressWarnings("unused")
    private void runOtherAsyncAction(Object item, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    private void runOneOffAsyncAction(ActionListener<Void> listener) {
        listener.onResponse(null);
    }
}
