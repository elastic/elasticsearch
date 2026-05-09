/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StorageObjectMetricsTests extends ESTestCase {

    public void testEmptyIsZero() {
        StorageObjectMetrics m = StorageObjectMetrics.ZERO;
        assertEquals(0L, m.requestCount());
        assertEquals(0L, m.requestNanos());
        assertEquals(0L, m.bytesRead());
        assertEquals(0L, m.retryCount());
        assertTrue(m.isZero());
    }

    public void testAdd() {
        StorageObjectMetrics a = new StorageObjectMetrics(1, 100, 1024, 0);
        StorageObjectMetrics b = new StorageObjectMetrics(2, 200, 2048, 1);
        StorageObjectMetrics sum = a.add(b);
        assertEquals(3L, sum.requestCount());
        assertEquals(300L, sum.requestNanos());
        assertEquals(3072L, sum.bytesRead());
        assertEquals(1L, sum.retryCount());
    }

    public void testSerializationRoundTrip() throws Exception {
        StorageObjectMetrics original = new StorageObjectMetrics(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
            StorageObjectMetrics roundTripped = new StorageObjectMetrics(in);
            assertEquals(original, roundTripped);
        }
    }

    public void testCountersIncrementUnderConcurrency() throws Exception {
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();

        int threads = 8;
        int iterationsPerThread = 1_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        counters.addRequest(123L, 4096L);
                        if (i % 7 == 0) {
                            counters.addRetry();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        long expectedRequests = (long) threads * iterationsPerThread;
        long expectedNanos = expectedRequests * 123L;
        long expectedBytes = expectedRequests * 4096L;
        long expectedRetries = (long) threads * (iterationsPerThread / 7 + (iterationsPerThread % 7 == 0 ? 0 : 1));

        StorageObjectMetrics snap = counters.snapshot();
        assertEquals(expectedRequests, snap.requestCount());
        assertEquals(expectedNanos, snap.requestNanos());
        assertEquals(expectedBytes, snap.bytesRead());
        assertEquals(expectedRetries, snap.retryCount());
    }

    public void testStorageObjectDefaultsToEmpty() {
        StorageObject obj = new MinimalStorageObject();
        assertSame(StorageObjectMetrics.ZERO, obj.metrics());
    }

    /** Minimal StorageObject impl that exists purely to assert the default {@link StorageObject#metrics()} accessor. */
    private static final class MinimalStorageObject implements StorageObject {
        @Override
        public java.io.InputStream newStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.io.InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public java.time.Instant lastModified() {
            return null;
        }

        @Override
        public boolean exists() {
            return false;
        }

        @Override
        public StoragePath path() {
            return null;
        }
    }
}
