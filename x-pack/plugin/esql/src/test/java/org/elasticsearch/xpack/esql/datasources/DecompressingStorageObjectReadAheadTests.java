/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.DecompressingStorageObject.ReadAhead;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A gzip or zstd text object is one serial stream, but its compressed bytes must not arrive over one
 * whole-object GET. These tests pin that {@link DecompressingStorageObject} feeds the codec from a window
 * of concurrent ranged reads, delivered in order, with every buffer and outstanding range released.
 */
public class DecompressingStorageObjectReadAheadTests extends ESTestCase {

    private static final Executor DIRECT = Runnable::run;

    public void testStreamOnlyCodecReadsThroughConcurrentRangesInOrder() throws Exception {
        byte[] original = randomByteArrayOfLength(between(4 << 20, 8 << 20));
        byte[] compressed = gzip(original);
        CountingStorageObject raw = new CountingStorageObject(compressed, true, 0);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(64));
        StorageObject decompressing = decompressing(raw, breaker, new ReadAhead(4, 1024 * 1024));

        InputStream in = decompressing.newStream();
        assertArrayEquals(original, in.readAllBytes());

        assertThat("no whole-object stream is opened", raw.wholeObjectOpens.get(), equalTo(0));
        assertThat("ranges are in flight together", raw.maxInFlight.get(), greaterThanOrEqualTo(2));
        decompressing.abortStream(in);
        assertThat("every range buffer is released", breaker.getUsed(), equalTo(0L));
    }

    public void testWindowOfZeroKeepsSingleStream() throws Exception {
        byte[] original = randomByteArrayOfLength(between(1000, 100_000));
        CountingStorageObject raw = new CountingStorageObject(gzip(original), false, 0);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(64));
        StorageObject decompressing = decompressing(raw, breaker, ReadAhead.NONE);

        InputStream in = decompressing.newStream();
        assertArrayEquals(original, in.readAllBytes());
        in.close();

        assertThat(raw.wholeObjectOpens.get(), equalTo(1));
        assertThat(raw.rangedReads.get(), equalTo(0));
    }

    public void testAbortEarlyCancelsOutstandingRanges() throws Exception {
        byte[] original = randomByteArrayOfLength(between(4 << 20, 6 << 20));
        CountingStorageObject raw = new CountingStorageObject(gzip(original), true, 0);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(64));
        StorageObject decompressing = decompressing(raw, breaker, new ReadAhead(4, 256 * 1024));

        InputStream in = decompressing.newStream();
        assertThat(in.read(new byte[4096]), greaterThanOrEqualTo(1));
        decompressing.abortStream(in);
        raw.awaitAllCompleted();

        assertThat("outstanding ranges are cancelled", raw.cancelled.get(), greaterThanOrEqualTo(1));
        assertThat("every buffer, including late completions, is released", breaker.getUsed(), equalTo(0L));
    }

    public void testRangeFailingOnceIsRetriedWithoutLosingBytes() throws Exception {
        byte[] original = randomByteArrayOfLength(between(1 << 20, 3 << 20));
        CountingStorageObject raw = new CountingStorageObject(gzip(original), true, 1);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(64));
        StorageObject chain = new RetryableStorageObject(
            decompressingOver(new RetryableStorageObject(raw, new RetryPolicy(3, 1, 10)), breaker, new ReadAhead(3, 128 * 1024)),
            new RetryPolicy(1, 1, 10)
        );

        InputStream in = chain.newStream();
        assertArrayEquals(original, in.readAllBytes());
        chain.abortStream(in);
        assertThat(raw.failuresInjected.get(), equalTo(1));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testPersistentRangeFailurePropagatesAndReleasesBuffers() throws Exception {
        byte[] original = randomByteArrayOfLength(between(1 << 20, 2 << 20));
        CountingStorageObject raw = new CountingStorageObject(gzip(original), true, Integer.MAX_VALUE);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(64));
        StorageObject decompressing = decompressing(raw, breaker, new ReadAhead(3, 128 * 1024));

        IOException e = expectThrows(IOException.class, () -> {
            InputStream in = decompressing.newStream();
            try {
                in.readAllBytes();
            } finally {
                decompressing.abortStream(in);
            }
        });
        assertThat(e.getCause() == null ? e : e.getCause(), instanceOf(IOException.class));
        raw.awaitAllCompleted();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReadAheadStreamHandlesLengthsAroundRangeBoundaries() throws Exception {
        for (int length : new int[] { 0, 1, 99, 100, 101, 299, 300, 301 }) {
            byte[] data = randomByteArrayOfLength(length);
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
            CountingStorageObject raw = new CountingStorageObject(data, true, 0);
            try (
                RangeReadAheadInputStream in = new RangeReadAheadInputStream(
                    raw,
                    length,
                    100,
                    3,
                    DirectBufferFactory.forBreaker(breaker),
                    DIRECT
                )
            ) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                if (randomBoolean()) {
                    int b;
                    while ((b = in.read()) != -1) {
                        out.write(b);
                    }
                } else {
                    out.write(in.readAllBytes());
                }
                assertArrayEquals("length " + length, data, out.toByteArray());
            }
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    private static StorageObject decompressing(StorageObject raw, CircuitBreaker breaker, ReadAhead readAhead) {
        return decompressingOver(raw, breaker, readAhead);
    }

    private static DecompressingStorageObject decompressingOver(StorageObject raw, CircuitBreaker breaker, ReadAhead readAhead) {
        return new DecompressingStorageObject(raw, new GzipDecompressionCodec(), breaker, 0, DIRECT, readAhead);
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(data);
        }
        return bos.toByteArray();
    }

    /**
     * In-memory object whose ranged reads complete on their own threads after a random delay, so ranges finish out of
     * order. A real storage provider is not usable here because the test must observe concurrency, cancellation and
     * injected failures, which no real provider exposes.
     */
    private static final class CountingStorageObject implements StorageObject {
        private final byte[] data;
        private final boolean randomDelay;
        private final int failuresToInject;

        final AtomicInteger wholeObjectOpens = new AtomicInteger();
        final AtomicInteger rangedReads = new AtomicInteger();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final AtomicInteger cancelled = new AtomicInteger();
        final AtomicInteger failuresInjected = new AtomicInteger();
        private final Set<Thread> threads = ConcurrentHashMap.newKeySet();

        CountingStorageObject(byte[] data, boolean randomDelay, int failuresToInject) {
            this.data = data;
            this.randomDelay = randomDelay;
            this.failuresToInject = failuresToInject;
        }

        void awaitAllCompleted() throws InterruptedException {
            for (Thread t : threads) {
                t.join(TimeValue.timeValueSeconds(10).millis());
            }
        }

        @Override
        public InputStream newStream() {
            wholeObjectOpens.incrementAndGet();
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        @Override
        public Releasable startReadBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            rangedReads.incrementAndGet();
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            DirectReadBuffer drb;
            try {
                drb = factory.allocateWritableWindow((int) length);
            } catch (Exception e) {
                inFlight.decrementAndGet();
                listener.onFailure(e);
                return () -> {};
            }
            CountDownLatch cancelLatch = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                try {
                    if (randomDelay) {
                        // a cancelled range wakes immediately rather than finishing the delay
                        cancelLatch.await(between(0, 30), java.util.concurrent.TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                inFlight.decrementAndGet();
                if (cancelLatch.getCount() == 0) {
                    // Cancelled: a real client fails the future; the buffer must still be released.
                    drb.close();
                    listener.onFailure(new IOException("cancelled"));
                    return;
                }
                if (failuresInjected.get() < failuresToInject && failuresInjected.incrementAndGet() > 0) {
                    drb.close();
                    listener.onFailure(new java.net.ConnectException("injected failure at " + position));
                    return;
                }
                ByteBuffer buf = drb.buffer();
                buf.put(data, (int) position, (int) length);
                buf.position(0).limit((int) length);
                listener.onResponse(drb);
            });
            threads.add(t);
            t.start();
            return () -> {
                if (cancelLatch.getCount() > 0) {
                    cancelled.incrementAndGet();
                    cancelLatch.countDown();
                }
            };
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public long knownLength() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("s3://bucket/data.csv.gz");
        }

        @Override
        public StorageIdentity storageIdentity() {
            return StorageIdentity.unique();
        }
    }
}
