/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PlainActionFutureTests extends ESTestCase {

    public void testInterruption() throws Exception {
        final PlainActionFuture<Object> future = new PlainActionFuture<>() {
            @Override
            public void onResponse(Object value) {
                throw new AssertionError("should not be called");
            }
        };

        // test all possible methods that can be interrupted
        final Runnable runnable = () -> {
            final int method = randomIntBetween(0, 2);
            switch (method) {
                case 0 -> future.actionGet();
                case 1 -> future.actionGet(TimeValue.timeValueSeconds(30));
                case 2 -> future.actionGet(30, TimeUnit.SECONDS);
                default -> throw new AssertionError(method);
            }
        };

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread main = Thread.currentThread();
        final Thread thread = new Thread(() -> {
            safeAwait(barrier);
            main.interrupt();
        });
        thread.start();

        final AtomicBoolean interrupted = new AtomicBoolean();

        safeAwait(barrier);

        try {
            runnable.run();
        } catch (final IllegalStateException e) {
            interrupted.set(Thread.interrupted());
        }
        // we check this here instead of in the catch block to ensure that the catch block executed
        assertTrue(interrupted.get());

        thread.join();
    }

    public void testNoResult() {
        assumeTrue("assertions required for this test", Assertions.ENABLED);
        final var future = new PlainActionFuture<>();
        expectThrows(AssertionError.class, future::result);
    }

    public void testUnwrapException() {
        checkUnwrap(new RemoteTransportException("test", new RuntimeException()), RuntimeException.class, RemoteTransportException.class);
        checkUnwrap(
            new RemoteTransportException("test", new Exception()),
            UncategorizedExecutionException.class,
            RemoteTransportException.class
        );
        checkUnwrap(new Exception(), UncategorizedExecutionException.class, Exception.class);
        checkUnwrap(new ElasticsearchException("test", new Exception()), ElasticsearchException.class, ElasticsearchException.class);
    }

    private void checkUnwrap(Exception exception, Class<? extends Exception> actionGetException, Class<? extends Exception> getException) {
        final var future = new PlainActionFuture<>();
        future.onFailure(exception);

        assertEquals(actionGetException, expectThrows(RuntimeException.class, future::actionGet).getClass());
        assertEquals(actionGetException, expectThrows(RuntimeException.class, () -> future.actionGet(10, TimeUnit.SECONDS)).getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, future::get).getCause().getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS)).getCause().getClass());

        if (exception instanceof RuntimeException) {
            expectThrows(ExecutionException.class, getException, future::result);
            expectThrows(ExecutionException.class, getException, expectIgnoresInterrupt(future::result));
            assertEquals(getException, expectThrows(Exception.class, () -> FutureUtils.get(future)).getClass());
            assertEquals(getException, expectThrows(Exception.class, () -> FutureUtils.get(future, 10, TimeUnit.SECONDS)).getClass());
        } else {
            expectThrows(ExecutionException.class, getException, future::result);
            expectThrows(ExecutionException.class, getException, expectIgnoresInterrupt(future::result));
            assertEquals(getException, expectThrowsWrapped(() -> FutureUtils.get(future)).getClass());
            assertEquals(getException, expectThrowsWrapped(() -> FutureUtils.get(future, 10, TimeUnit.SECONDS)).getClass());
        }

        assertCapturesInterrupt(future::get);
        assertCapturesInterrupt(() -> future.get(10, TimeUnit.SECONDS));
        assertPropagatesInterrupt(future::actionGet);
        assertPropagatesInterrupt(() -> future.actionGet(10, TimeUnit.SECONDS));
    }

    private static Throwable expectThrowsWrapped(ThrowingRunnable runnable) {
        return expectThrows(UncategorizedExecutionException.class, ExecutionException.class, runnable).getCause();
    }

    public void testCancelException() {
        final var future = new PlainActionFuture<>();
        future.cancel(randomBoolean());

        assertCancellation(future::get);
        assertCancellation(future::actionGet);
        assertCancellation(() -> future.get(10, TimeUnit.SECONDS));
        assertCancellation(() -> future.actionGet(10, TimeUnit.SECONDS));
        assertCancellation(future::result);

        try {
            Thread.currentThread().interrupt();
            assertCancellation(future::result);
        } finally {
            assertTrue(Thread.interrupted());
        }

        assertCapturesInterrupt(future::get);
        assertCapturesInterrupt(() -> future.get(10, TimeUnit.SECONDS));
        assertPropagatesInterrupt(future::actionGet);
        assertPropagatesInterrupt(() -> future.actionGet(10, TimeUnit.SECONDS));
    }

    public void testAssertCompleteAllowedAllowsConcurrentCompletesFromSamePool() {
        final AtomicReference<PlainActionFuture<?>> futureReference = new AtomicReference<>(new PlainActionFuture<>());
        final var executorName = randomFrom(ThreadPool.Names.GENERIC, ThreadPool.Names.MANAGEMENT);
        final var running = new AtomicBoolean(true);
        try (TestThreadPool threadPool = new TestThreadPool(getTestName())) {
            // We only need 4 threads to reproduce this issue reliably, using more threads
            // just increases the run time due to the additional synchronisation
            final var threadCount = Math.min(threadPool.info(executorName).getMax(), 4);
            final var startBarrier = new CyclicBarrier(threadCount + 1);
            // N threads competing to complete the futures
            for (int i = 0; i < threadCount; i++) {
                threadPool.executor(executorName).execute(() -> {
                    safeAwait(startBarrier);
                    while (running.get()) {
                        futureReference.get().onResponse(null);
                    }
                });
            }
            // The race can only occur once per completion, so we provide
            // a stream of new futures to the competing threads to
            // maximise the probability it occurs. Providing them
            // with new futures while they spin proved to be much
            // more reliable at reproducing the issue than releasing
            // them all from a barrier to complete a single future.
            safeAwait(startBarrier);
            for (int i = 0; i < 20; i++) {
                futureReference.set(new PlainActionFuture<>());
                safeSleep(1);
            }
            running.set(false);
        }
    }

    private static void assertCancellation(ThrowingRunnable runnable) {
        final var cancellationException = expectThrows(CancellationException.class, runnable);
        assertEquals("Task was cancelled.", cancellationException.getMessage());
        assertNull(cancellationException.getCause());
    }

    private static void assertCapturesInterrupt(ThrowingRunnable runnable) {
        try {
            Thread.currentThread().interrupt();
            final var interruptedException = expectThrows(InterruptedException.class, runnable);
            assertNull(interruptedException.getMessage());
            assertNull(interruptedException.getCause());
        } finally {
            assertFalse(Thread.interrupted());
        }
    }

    private static void assertPropagatesInterrupt(ThrowingRunnable runnable) {
        try {
            Thread.currentThread().interrupt();
            final var interruptedException = expectThrows(IllegalStateException.class, InterruptedException.class, runnable);
            assertNull(interruptedException.getMessage());
            assertNull(interruptedException.getCause());
        } finally {
            assertTrue(Thread.interrupted());
        }
    }

    private static ThrowingRunnable expectIgnoresInterrupt(ThrowingRunnable runnable) {
        return () -> {
            try {
                Thread.currentThread().interrupt();
                runnable.run();
            } finally {
                assertTrue(Thread.interrupted());
            }
        };
    }
}
