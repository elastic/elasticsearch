/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class AbstractMeteredStorageObjectTests extends ESTestCase {

    /** Concrete leaf with the read methods stubbed; only the metering scaffolding is under test. */
    private static final class TestStorageObject extends AbstractMeteredStorageObject {
        @Override
        public InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant lastModified() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoragePath path() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestError extends Error {
        TestError(String message) {
            super(message);
        }
    }

    public void testOnReadCompleteForwardsResultAndReturnsCompletingFuture() {
        CompletableFuture<String> future = new CompletableFuture<>();
        AtomicReference<String> seenResult = new AtomicReference<>();
        AtomicReference<Throwable> seenFailure = new AtomicReference<>();
        CompletableFuture<String> returned = AbstractMeteredStorageObject.onReadComplete(future, (result, throwable) -> {
            seenResult.set(result);
            seenFailure.set(throwable);
        });

        future.complete("ok");
        assertEquals("ok", seenResult.get());
        assertNull(seenFailure.get());
        assertEquals("ok", returned.join()); // the returned future is usable, not swallowed internally
    }

    public void testOnReadCompleteForwardsFailure() {
        CompletableFuture<String> future = new CompletableFuture<>();
        AtomicReference<Throwable> seenFailure = new AtomicReference<>();
        AbstractMeteredStorageObject.onReadComplete(future, (result, throwable) -> seenFailure.set(throwable));

        RuntimeException boom = new RuntimeException("upstream");
        future.completeExceptionally(boom);
        assertSame(boom, seenFailure.get());
    }

    public void testOnReadCompleteSurfacesFatalErrorToUncaughtHandler() throws Exception {
        Thread.UncaughtExceptionHandler original = Thread.getDefaultUncaughtExceptionHandler();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> captured = new AtomicReference<>();
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            if ("elasticsearch-error-rethrower".equals(thread.getName())) {
                captured.set(throwable);
                latch.countDown();
                return;
            }
            if (original != null) {
                original.uncaughtException(thread, throwable);
            }
        });
        try {
            TestError error = new TestError("boom");
            CompletableFuture<String> future = new CompletableFuture<>();
            AbstractMeteredStorageObject.onReadComplete(future, (result, throwable) -> { throw error; });

            future.complete("x"); // runs the handler, which throws the fatal error
            assertTrue("fatal error was not rethrown on another thread", latch.await(10, TimeUnit.SECONDS));
            assertSame(error, captured.get());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(original);
        }
    }

    public void testDeliverReadHandsBufferToListener() {
        TestStorageObject obj = new TestStorageObject();
        DirectReadBuffer buffer = new DirectReadBuffer(ByteBuffer.allocate(16), () -> {});
        AtomicReference<DirectReadBuffer> delivered = new AtomicReference<>();

        obj.deliverRead(ActionListener.wrap(delivered::set, e -> fail("unexpected failure: " + e)), buffer, System.nanoTime());
        assertSame(buffer, delivered.get());
    }

    public void testDeliverReadClosesBufferWhenDeliveryThrows() {
        TestStorageObject obj = new TestStorageObject();
        AtomicBoolean closed = new AtomicBoolean();
        DirectReadBuffer buffer = new DirectReadBuffer(ByteBuffer.allocate(16), () -> closed.set(true));
        RuntimeException boom = new RuntimeException("listener boom");

        ActionListener<DirectReadBuffer> throwing = new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer b) {
                throw boom;
            }

            @Override
            public void onFailure(Exception e) {
                fail("onFailure must not be called when onResponse threw");
            }
        };

        assertSame(boom, expectThrows(RuntimeException.class, () -> obj.deliverRead(throwing, buffer, System.nanoTime())));
        assertTrue("buffer must be closed when delivery throws", closed.get());
    }
}
