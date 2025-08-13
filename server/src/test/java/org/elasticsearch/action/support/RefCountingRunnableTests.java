/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.hamcrest.Matchers.containsString;

public class RefCountingRunnableTests extends ESTestCase {

    public void testBasicOperation() throws InterruptedException {
        final var executed = new AtomicBoolean();
        final var threads = new Thread[between(0, 3)];
        boolean async = false;
        final var startLatch = new CountDownLatch(1);

        try (var refs = new RefCountingRunnable(new Runnable() {
            @Override
            public void run() {
                assertTrue(executed.compareAndSet(false, true));
            }

            @Override
            public String toString() {
                return "test runnable";
            }
        })) {
            assertEquals("refCounted[test runnable]", refs.toString());
            try (var ref = refs.acquire()) {
                assertEquals("refCounted[test runnable]", ref.toString());
            }
            var listener = refs.acquireListener();
            assertThat(listener.toString(), containsString("release[refCounted[test runnable]]"));
            listener.onResponse(null);

            for (int i = 0; i < threads.length; i++) {
                if (randomBoolean()) {
                    async = true;
                    var ref = refs.acquire();
                    threads[i] = new Thread(() -> {
                        try (var ignored = ref) {
                            assertTrue(startLatch.await(10, TimeUnit.SECONDS));
                            assertFalse(executed.get());
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    });
                }
            }

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
        expectThrows(NullPointerException.class, () -> new RefCountingRunnable(null));
    }

    public void testAsyncAcquire() throws InterruptedException {
        final var completionLatch = new CountDownLatch(1);
        final var executorService = EsExecutors.newScaling(
            "test",
            1,
            between(1, 10),
            10,
            TimeUnit.SECONDS,
            true,
            EsExecutors.daemonThreadFactory("test"),
            new ThreadContext(Settings.EMPTY)
        );
        final var asyncPermits = new Semaphore(between(0, 1000));

        try (var refs = new RefCountingRunnable(() -> {
            assertEquals(1, completionLatch.getCount());
            completionLatch.countDown();
        })) {
            class AsyncAction extends AbstractRunnable {
                private final Releasable ref;

                AsyncAction(Releasable ref) {
                    this.ref = ref;
                }

                @Override
                protected void doRun() throws Exception {
                    if (asyncPermits.tryAcquire()) {
                        executorService.execute(new AsyncAction(refs.acquire()));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
                }

                @Override
                public void onAfter() {
                    ref.close();
                }
            }

            for (int i = between(0, 5); i >= 0; i--) {
                executorService.execute(new AsyncAction(refs.acquire()));
            }

            assertEquals(1, completionLatch.getCount());
        }

        if (randomBoolean()) {
            assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
            assertFalse(asyncPermits.tryAcquire());
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
    }

    public void testValidation() {
        final var callCount = new AtomicInteger();
        final var refs = new RefCountingRunnable(callCount::incrementAndGet);
        refs.close();
        assertEquals(1, callCount.get());

        for (int i = between(1, 5); i > 0; i--) {
            final ThrowingRunnable throwingRunnable;
            final String expectedMessage;
            if (randomBoolean()) {
                throwingRunnable = randomBoolean() ? refs::acquire : refs::acquireListener;
                expectedMessage = AbstractRefCounted.ALREADY_CLOSED_MESSAGE;
            } else {
                throwingRunnable = refs::close;
                expectedMessage = AbstractRefCounted.INVALID_DECREF_MESSAGE;
            }

            assertEquals(expectedMessage, expectThrows(AssertionError.class, throwingRunnable).getMessage());
            assertEquals(1, callCount.get());
        }
    }

    public void testJavaDocExample() {
        final var flag = new AtomicBoolean();
        runExample(() -> assertTrue(flag.compareAndSet(false, true)));
        assertTrue(flag.get());
    }

    private void runExample(Runnable finalRunnable) {
        final var collection = randomList(10, Object::new);
        final var otherCollection = randomList(10, Object::new);
        final var flag = randomBoolean();
        @SuppressWarnings("UnnecessaryLocalVariable")
        final var executorService = DIRECT_EXECUTOR_SERVICE;

        try (var refs = new RefCountingRunnable(finalRunnable)) {
            for (var item : collection) {
                if (condition(item)) {
                    runAsyncAction(item, refs.acquire());
                }
            }
            if (flag) {
                runOneOffAsyncAction(refs.acquire());
                return;
            }
            for (var item : otherCollection) {
                var itemRef = refs.acquire(); // delays completion while the background action is pending
                executorService.execute(() -> {
                    try (var ignored = itemRef) {
                        if (condition(item)) {
                            runOtherAsyncAction(item, refs.acquire());
                        }
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
    private void runAsyncAction(Object item, Releasable releasable) {
        releasable.close();
    }

    @SuppressWarnings("unused")
    private void runOtherAsyncAction(Object item, Releasable releasable) {
        releasable.close();
    }

    private void runOneOffAsyncAction(Releasable releasable) {
        releasable.close();
    }
}
