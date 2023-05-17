/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
            final int method = randomIntBetween(0, 4);
            switch (method) {
                case 0 -> future.actionGet();
                case 1 -> future.actionGet("30s");
                case 2 -> future.actionGet(30000);
                case 3 -> future.actionGet(TimeValue.timeValueSeconds(30));
                case 4 -> future.actionGet(30, TimeUnit.SECONDS);
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
        expectThrows(AssertionError.class, future::actionResult);
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
        assertEquals(actionGetException, expectThrows(RuntimeException.class, future::actionResult).getClass());
        assertEquals(actionGetException, expectThrows(RuntimeException.class, expectIgnoresInterrupt(future::actionResult)).getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, future::get).getCause().getClass());
        assertEquals(getException, expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS)).getCause().getClass());

        if (exception instanceof RuntimeException) {
            assertEquals(getException, expectThrows(Exception.class, future::result).getClass());
            assertEquals(getException, expectThrows(Exception.class, expectIgnoresInterrupt(future::result)).getClass());
            assertEquals(getException, expectThrows(Exception.class, () -> FutureUtils.get(future)).getClass());
            assertEquals(getException, expectThrows(Exception.class, () -> FutureUtils.get(future, 10, TimeUnit.SECONDS)).getClass());
        } else {
            assertEquals(getException, expectThrowsWrapped(future::result).getClass());
            assertEquals(getException, expectThrowsWrapped(expectIgnoresInterrupt(future::result)).getClass());
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
        assertCancellation(future::actionResult);

        try {
            Thread.currentThread().interrupt();
            assertCancellation(future::result);
            assertCancellation(future::actionResult);
        } finally {
            assertTrue(Thread.interrupted());
        }

        assertCapturesInterrupt(future::get);
        assertCapturesInterrupt(() -> future.get(10, TimeUnit.SECONDS));
        assertPropagatesInterrupt(future::actionGet);
        assertPropagatesInterrupt(() -> future.actionGet(10, TimeUnit.SECONDS));
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
