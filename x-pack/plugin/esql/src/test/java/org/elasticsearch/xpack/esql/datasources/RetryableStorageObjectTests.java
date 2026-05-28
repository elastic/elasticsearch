/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryableStorageObjectTests extends ESTestCase {

    /**
     * Test fixture instead of {@code mock(StorageObject.class)} per AGENTS.md "real classes over mocks":
     * carries a configurable {@link StorageObjectMetrics} snapshot and a single-shot {@code newStream()}
     * that fails {@code failuresBeforeSuccess} times with the configured exception then returns the
     * configured payload. Other {@link StorageObject} methods throw — they're not exercised by the
     * metrics tests below and a real failure beats a silent mocked default.
     */
    private static final class FakeStorageObject implements StorageObject {
        private final StoragePath path;
        private final StorageObjectMetrics metrics;
        private final IOException failure;
        private final byte[] successPayload;
        private final int failuresBeforeSuccess;
        private int callsObserved = 0;

        FakeStorageObject(
            StoragePath path,
            StorageObjectMetrics metrics,
            IOException failure,
            byte[] successPayload,
            int failuresBeforeSuccess
        ) {
            this.path = path;
            this.metrics = metrics;
            this.failure = failure;
            this.successPayload = successPayload;
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public InputStream newStream() throws IOException {
            int call = callsObserved++;
            if (call < failuresBeforeSuccess) {
                throw failure;
            }
            return new ByteArrayInputStream(successPayload);
        }

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
            // SPI contract: "or null if not available" — null is a real value, not unsupported.
            return null;
        }

        @Override
        public boolean exists() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObjectMetrics metrics() {
            return metrics;
        }
    }

    public void testMetricsForwardsDelegateCountersWhenNoRetries() {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        StorageObjectMetrics snapshot = new StorageObjectMetrics(13, 4321, 16384, 4);
        FakeStorageObject delegate = new FakeStorageObject(StoragePath.of("s3://bucket/key"), snapshot, null, new byte[0], 0);

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        // No retries observed at this decorator boundary, so the merged snapshot equals the delegate.
        assertEquals(snapshot, obj.metrics());
    }

    public void testMetricsAddsRetryCountObservedByDecorator() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        // Underlying provider counts a request and bytes; SDK-internal retries (would-be retryCount)
        // are not yet wired at the provider layer, so retryCount on the delegate is zero.
        StorageObjectMetrics delegateMetrics = new StorageObjectMetrics(1, 100, 4096, 0);
        // Two SocketTimeoutException failures classify as transient (per RetryPolicy.isTransientSingleCause),
        // triggering two retries before the third newStream() call returns a real ByteArrayInputStream.
        FakeStorageObject delegate = new FakeStorageObject(
            StoragePath.of("s3://bucket/key"),
            delegateMetrics,
            new SocketTimeoutException("read timed out"),
            new byte[] { 1, 2, 3 },
            2
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        try (InputStream returned = obj.newStream()) {
            assertEquals(1, returned.read());
            assertEquals(2, returned.read());
            assertEquals(3, returned.read());
            assertEquals(-1, returned.read());
        }

        StorageObjectMetrics merged = obj.metrics();
        // Delegate's request/byte counters pass through; retry counter accumulates at the decorator.
        assertEquals(1L, merged.requestCount());
        assertEquals(100L, merged.requestNanos());
        assertEquals(4096L, merged.bytesRead());
        assertEquals(2L, merged.retryCount());
    }

    /**
     * Regression guard: {@code abortStream} must forward directly to the delegate, not fall
     * through to the SPI default (which is a draining {@code stream.close()} on providers like
     * S3). The original bug was a missing override on a decorator silently swallowing the abort.
     */
    public void testAbortStreamDelegates() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(3, 1, 10));
        obj.abortStream(stream);

        verify(delegate).abortStream(stream);
    }

    /**
     * Regression guard: abort is a best-effort connection-discard, not a retryable I/O. If the
     * delegate's {@code abortStream} throws, the exception must propagate unchanged — wrapping
     * abort in retry logic would defeat the bounded-latency contract the abort path provides.
     */
    public void testAbortStreamDoesNotRetryOnFailure() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));
        doThrow(new IOException("abort failed")).when(delegate).abortStream(any(InputStream.class));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(5, 1, 10));
        IOException thrown = expectThrows(IOException.class, () -> obj.abortStream(stream));
        assertEquals("abort failed", thrown.getMessage());

        verify(delegate, times(1)).abortStream(stream);
    }

    /**
     * Regression guard: the wrapper must not unwrap or alter the stream instance — the SPI
     * contract requires the delegate to receive the exact instance it returned from
     * {@code newStream()}, otherwise provider-specific abort dispatch (e.g. the S3
     * {@code Abortable} cast) silently falls back to a draining close.
     */
    public void testAbortStreamForwardsSameInstance() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));
        InputStream[] captured = new InputStream[1];
        doAnswer(inv -> {
            captured[0] = inv.getArgument(0);
            return null;
        }).when(delegate).abortStream(any(InputStream.class));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(3, 1, 10));
        obj.abortStream(stream);

        assertSame(stream, captured[0]);
    }
}
