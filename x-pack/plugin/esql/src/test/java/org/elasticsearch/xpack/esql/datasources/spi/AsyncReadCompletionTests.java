/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class AsyncReadCompletionTests extends ESTestCase {

    /** A dedicated {@link Error} type so the test can match it unambiguously. */
    private static final class TestError extends Error {
        TestError(String message) {
            super(message);
        }
    }

    public void testForwardsResultAndFailureToDelegate() {
        AtomicReference<Object> seenResult = new AtomicReference<>();
        AtomicReference<Throwable> seenFailure = new AtomicReference<>();
        BiConsumer<Object, Throwable> wrapped = AsyncReadCompletion.errorSafe((result, failure) -> {
            seenResult.set(result);
            seenFailure.set(failure);
        });

        Object result = new Object();
        wrapped.accept(result, null);
        assertSame(result, seenResult.get());
        assertNull(seenFailure.get());

        RuntimeException failure = new RuntimeException("upstream");
        wrapped.accept(null, failure);
        assertNull(seenResult.get());
        assertSame(failure, seenFailure.get());
    }

    public void testNonFatalThrowableIsRethrownToCaller() {
        RuntimeException boom = new RuntimeException("boom");
        BiConsumer<Object, Throwable> wrapped = AsyncReadCompletion.errorSafe((result, failure) -> { throw boom; });
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> wrapped.accept(null, null));
        assertSame(boom, thrown);
    }

    /**
     * The point of the wrapper: a fatal {@link Error} thrown while delivering a completion must not be
     * swallowed by the enclosing {@code CompletableFuture}. {@code ExceptionsHelper.maybeDieOnAnotherThread}
     * forks an "elasticsearch-error-rethrower" thread that rethrows it to the uncaught-exception handler.
     * We intercept the default handler to observe that, suppressing it so the test framework does not see it.
     */
    public void testFatalErrorReachesUncaughtExceptionHandler() throws Exception {
        Thread.UncaughtExceptionHandler original = Thread.getDefaultUncaughtExceptionHandler();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> captured = new AtomicReference<>();
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            if ("elasticsearch-error-rethrower".equals(thread.getName())) {
                captured.set(throwable);
                latch.countDown();
                return; // suppress: do not let the rethrown error fail the test framework
            }
            if (original != null) {
                original.uncaughtException(thread, throwable);
            }
        });
        try {
            TestError error = new TestError("boom");
            BiConsumer<Object, Throwable> wrapped = AsyncReadCompletion.errorSafe((result, failure) -> { throw error; });

            // The wrapper still rethrows to its immediate caller (the CompletableFuture machinery)...
            TestError thrown = expectThrows(TestError.class, () -> wrapped.accept(null, null));
            assertSame(error, thrown);

            // ...and, crucially, dispatches it to the uncaught-exception handler on a forked thread.
            assertTrue("fatal error was not rethrown on another thread", latch.await(10, TimeUnit.SECONDS));
            assertSame(error, captured.get());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(original);
        }
    }
}
