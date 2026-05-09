/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

public class RetryableStorageObjectTests extends ESTestCase {

    /**
     * Test fixture instead of {@code mock(StorageObject.class)} per AGENTS.md "real classes over mocks":
     * carries a configurable {@link StorageObjectMetrics} snapshot and a single-shot {@code newStream()}
     * that fails {@code failuresBeforeSuccess} times with the configured exception then returns the
     * configured payload. Other {@link StorageObject} methods throw — they're not exercised by the
     * tests below and a real failure beats a silent mocked default.
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
        public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
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
}
