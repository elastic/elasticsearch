/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.test.ESTestCase;

import java.io.IOError;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;

public class ElasticsearchUncaughtExceptionHandlerTests extends ESTestCase {

    private static Map<Class<? extends Error>, Integer> EXPECTED_STATUS = Map.of(
        InternalError.class,
        128,
        OutOfMemoryError.class,
        127,
        StackOverflowError.class,
        126,
        UnknownError.class,
        125,
        IOError.class,
        124
    );

    public void testUncaughtError() throws InterruptedException {
        final Error error = randomFrom(
            new InternalError(),
            new OutOfMemoryError(),
            new StackOverflowError(),
            new UnknownError(),
            new IOError(new IOException("fatal")),
            new Error() {
            }
        );
        final Thread thread = new Thread(() -> { throw error; });
        final String name = randomAlphaOfLength(10);
        thread.setName(name);
        final AtomicBoolean halt = new AtomicBoolean();
        final AtomicInteger observedStatus = new AtomicInteger();
        final AtomicReference<String> threadNameReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        thread.setUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler() {

            @Override
            void halt(int status) {
                halt.set(true);
                observedStatus.set(status);
            }

            @Override
            void onFatalUncaught(String threadName, Throwable t) {
                threadNameReference.set(threadName);
                throwableReference.set(t);
            }

            @Override
            void onNonFatalUncaught(String threadName, Throwable t) {
                fail();
            }

        });
        thread.start();
        thread.join();
        assertTrue(halt.get());
        final int status;
        if (EXPECTED_STATUS.containsKey(error.getClass())) {
            status = EXPECTED_STATUS.get(error.getClass());
        } else {
            status = 1;
        }
        assertThat(observedStatus.get(), equalTo(status));
        assertThat(threadNameReference.get(), equalTo(name));
        assertThat(throwableReference.get(), equalTo(error));
    }

    public void testUncaughtException() throws InterruptedException {
        final RuntimeException e = new RuntimeException("boom");
        final Thread thread = new Thread(() -> { throw e; });
        final String name = randomAlphaOfLength(10);
        thread.setName(name);
        final AtomicReference<String> threadNameReference = new AtomicReference<>();
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        thread.setUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler() {
            @Override
            void halt(int status) {
                fail();
            }

            @Override
            void onFatalUncaught(String threadName, Throwable t) {
                fail();
            }

            @Override
            void onNonFatalUncaught(String threadName, Throwable t) {
                threadNameReference.set(threadName);
                throwableReference.set(t);
            }
        });
        thread.start();
        thread.join();
        assertThat(threadNameReference.get(), equalTo(name));
        assertThat(throwableReference.get(), equalTo(e));
    }

    public void testIsFatalCause() {
        assertFatal(new OutOfMemoryError());
        assertFatal(new StackOverflowError());
        assertFatal(new InternalError());
        assertFatal(new UnknownError());
        assertFatal(new IOError(new IOException()));
        assertNonFatal(new RuntimeException());
        assertNonFatal(new UncheckedIOException(new IOException()));
    }

    private void assertFatal(Throwable cause) {
        assertTrue(ElasticsearchUncaughtExceptionHandler.isFatalUncaught(cause));
    }

    private void assertNonFatal(Throwable cause) {
        assertFalse(ElasticsearchUncaughtExceptionHandler.isFatalUncaught(cause));
    }

}
