/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MeteredInputStreamTests extends ESTestCase {

    public void testReadAndSkipCountBytes() throws IOException {
        byte[] payload = randomByteArrayOfLength(64);
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        try (MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters)) {
            assertEquals(payload[0] & 0xFF, in.read());
            byte[] buf = new byte[10];
            assertEquals(10, in.read(buf));
            assertEquals(5, in.skip(5));
        }
        assertEquals(16L, counters.snapshot().bytesRead());
        assertEquals(0L, counters.snapshot().requestCount());
    }

    public void testShortReadAccountsReturnedCountNotBufferLength() throws IOException {
        byte[] payload = randomByteArrayOfLength(8);
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        try (MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters)) {
            assertEquals(8, in.read(new byte[32]));
            assertEquals(0, in.skip(16));
        }
        assertEquals(8L, counters.snapshot().bytesRead());
    }

    public void testSingleReadLargerThanChunkPublishesFullCount() throws IOException {
        int extra = between(1, 1024);
        byte[] payload = new byte[MeteredInputStream.PUBLISH_CHUNK_BYTES + extra];
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters);
        assertEquals(payload.length, in.read(payload));
        assertEquals(payload.length, counters.snapshot().bytesRead());
        in.close();
        assertEquals(payload.length, counters.snapshot().bytesRead());
    }

    public void testCloseWithoutReadBooksZero() throws IOException {
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        new MeteredInputStream(new ByteArrayInputStream(randomByteArrayOfLength(64)), counters).close();
        assertEquals(0L, counters.snapshot().bytesRead());
    }

    public void testMarkNotSupported() {
        MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(new byte[1]), new StorageObjectMetricsCounters());
        assertFalse(in.markSupported());
        expectThrows(IOException.class, in::reset);
    }

    public void testChunkPublishAt256KiB() throws IOException {
        int chunk = MeteredInputStream.PUBLISH_CHUNK_BYTES;
        byte[] payload = new byte[chunk + 100];
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters);
        assertEquals(chunk - 1, in.read(new byte[chunk - 1]));
        assertEquals(0L, counters.snapshot().bytesRead());
        assertEquals(payload[chunk - 1] & 0xFF, in.read());
        assertEquals("live snapshot publishes at the 256 KiB boundary", chunk, counters.snapshot().bytesRead());
        assertEquals(100, in.read(new byte[100]));
        assertEquals(chunk, counters.snapshot().bytesRead());
        in.close();
        assertEquals(payload.length, counters.snapshot().bytesRead());
    }

    public void testCloseFlushesRemainderOnce() throws IOException {
        byte[] payload = randomByteArrayOfLength(between(1, 1024));
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters);
        assertEquals(payload.length, in.read(payload));
        assertEquals(0L, counters.snapshot().bytesRead());
        in.close();
        in.close();
        assertEquals(payload.length, counters.snapshot().bytesRead());
    }

    public void testAbortDoesNotCallCloseWhenOnAbortSet() throws IOException {
        AtomicBoolean abortCalled = new AtomicBoolean();
        AtomicBoolean closeCalled = new AtomicBoolean();
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        InputStream inner = new FilterInputStream(new ByteArrayInputStream(new byte[32])) {
            @Override
            public void close() throws IOException {
                closeCalled.set(true);
                super.close();
            }
        };
        MeteredInputStream in = new MeteredInputStream(inner, counters, () -> abortCalled.set(true));
        assertEquals(8, in.read(new byte[8]));
        in.abort();
        in.abort();
        in.close();
        assertEquals(8L, counters.snapshot().bytesRead());
        assertTrue(abortCalled.get());
        assertFalse(closeCalled.get());
    }

    public void testAbortWithoutCallbackClosesInner() throws IOException {
        AtomicBoolean closeCalled = new AtomicBoolean();
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        InputStream inner = new FilterInputStream(new ByteArrayInputStream(new byte[8])) {
            @Override
            public void close() throws IOException {
                closeCalled.set(true);
                super.close();
            }
        };
        MeteredInputStream in = new MeteredInputStream(inner, counters);
        assertEquals(3, in.read(new byte[3]));
        in.abort();
        assertEquals(3L, counters.snapshot().bytesRead());
        assertTrue(closeCalled.get());
    }

    public void testPublishStreamBytesOnceOnClose() throws IOException {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "s3");
        counters.addRequest(1L, 0L);
        byte[] payload = randomByteArrayOfLength(between(1, 64));
        try (MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters)) {
            assertEquals(payload.length, in.read(payload));
        }
        assertEquals(1L, counters.snapshot().requestCount());
        assertEquals(payload.length, counters.snapshot().bytesRead());
        assertEquals(
            1,
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL).size()
        );
        assertEquals(
            1,
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL).size()
        );
        assertEquals(
            payload.length,
            registry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL)
                .get(0)
                .getLong()
        );
    }

    public void testChunkPublishDoesNotEmitApmUntilClose() throws IOException {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "s3");
        counters.addRequest(1L, 0L);
        byte[] payload = new byte[MeteredInputStream.PUBLISH_CHUNK_BYTES + 8];
        MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(payload), counters);
        assertEquals(payload.length, in.read(payload));
        assertEquals(payload.length, counters.snapshot().bytesRead());
        assertEquals(
            0,
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL).size()
        );
        in.close();
        assertEquals(
            1,
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL).size()
        );
        assertEquals(
            payload.length,
            registry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL)
                .get(0)
                .getLong()
        );
    }

    public void testZeroReadPublishesNothing() throws IOException {
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        try (MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(new byte[0]), counters)) {
            assertEquals(-1, in.read());
        }
        assertEquals(0L, counters.snapshot().bytesRead());
    }

    public void testReadAfterPartialThenClose() throws IOException {
        AtomicInteger reads = new AtomicInteger();
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        try (MeteredInputStream in = new MeteredInputStream(new ByteArrayInputStream(new byte[20]), counters)) {
            reads.set(in.read(new byte[7]));
        }
        assertEquals(7, reads.get());
        assertEquals(7L, counters.snapshot().bytesRead());
    }

    /**
     * Abort while {@code in.read()} is parked, then deliver {@code n} bytes. Those bytes reached
     * the caller after finish already flushed, and must still land in profile and APM.
     */
    public void testAbortDuringReadCountsDeliveredBytes() throws Exception {
        int delivered = between(1, 4096);
        byte[] payload = randomByteArrayOfLength(delivered);
        assertAbortDuringRead(payload, 0, payload.length, true);
    }

    public void testAbortDuringReadWithoutOnAbortCountsDeliveredBytes() throws Exception {
        int delivered = between(1, 4096);
        byte[] payload = randomByteArrayOfLength(delivered);
        assertAbortDuringRead(payload, 0, payload.length, false);
    }

    public void testAbortDuringReadEofKeepsPriorBytes() throws Exception {
        byte[] prefix = randomByteArrayOfLength(between(1, 1024));
        assertAbortDuringRead(prefix, prefix.length, -1, randomBoolean());
    }

    public void testAbortDuringReadEofWithNoPriorBytesPublishesNothing() throws Exception {
        assertAbortDuringRead(new byte[0], 0, -1, randomBoolean());
    }

    /**
     * Parked read returns a full publish chunk after abort already flushed the prefix remainder.
     * Chunk publish after finish would double-count; skipping it would drop the chunk.
     */
    public void testAbortDuringChunkReadDoesNotDoubleCount() throws Exception {
        int prefix = MeteredInputStream.PUBLISH_CHUNK_BYTES - between(1, 64);
        int tail = MeteredInputStream.PUBLISH_CHUNK_BYTES + between(0, 64);
        byte[] payload = randomByteArrayOfLength(prefix + tail);
        assertAbortDuringRead(payload, prefix, tail, randomBoolean());
    }

    public void testAbortTwiceDuringReadPublishesOnce() throws Exception {
        int delivered = between(1, 256);
        byte[] payload = randomByteArrayOfLength(delivered);
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        long profile = assertAbortDuringRead(payload, 0, payload.length, true, true, registry);
        assertEquals(delivered, profile);
        List<Measurement> events = apmEvents(registry);
        assertEquals("abort twice still one APM publish when finish flushed zero", 1, events.size());
        assertEquals(delivered, events.get(0).getLong());
    }

    /**
     * Tight abort-vs-read loop: in-flight {@code account} may overlap {@code finish} without a
     * parked read. Delivered bytes, profile, and APM sum must stay equal.
     */
    public void testConcurrentAbortDuringStreamingReads() throws Exception {
        int iters = scaledRandomIntBetween(20, 40);
        for (int i = 0; i < iters; i++) {
            byte[] payload = randomByteArrayOfLength(between(1, MeteredInputStream.PUBLISH_CHUNK_BYTES + 1024));
            RecordingMeterRegistry registry = new RecordingMeterRegistry();
            ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
            StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
            counters.attach(metrics, "s3");
            CloseReturnsEofStream inner = new CloseReturnsEofStream(payload);
            MeteredInputStream in = new MeteredInputStream(inner, counters, inner::close);

            AtomicInteger delivered = new AtomicInteger();
            AtomicReference<Throwable> readerError = new AtomicReference<>();
            AtomicReference<Throwable> abortError = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            byte[] buf = new byte[between(1, 2048)];
            Thread reader = new Thread(() -> {
                try {
                    started.countDown();
                    int n;
                    while ((n = in.read(buf)) != -1) {
                        delivered.addAndGet(n);
                    }
                } catch (Throwable e) {
                    readerError.set(e);
                }
            }, "metered-reader");
            Thread abort = new Thread(() -> {
                try {
                    assertTrue(started.await(10, TimeUnit.SECONDS));
                    Thread.yield();
                    in.abort();
                    in.abort();
                } catch (Throwable e) {
                    abortError.set(e);
                }
            }, "metered-abort");
            reader.start();
            abort.start();
            joinThread(reader);
            joinThread(abort);
            throwIfPresent(readerError.get());
            throwIfPresent(abortError.get());
            long profile = counters.snapshot().bytesRead();
            assertEquals("delivered vs profile on iter " + i, delivered.get(), profile);
            assertEquals("profile vs APM on iter " + i, profile, apmSum(registry));
            assertTrue("abort must not over-count on iter " + i, profile <= payload.length);
        }
    }

    private static void assertAbortDuringRead(byte[] payload, int prefixLen, int parkedResult, boolean withOnAbort) throws Exception {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        assertAbortDuringRead(payload, prefixLen, parkedResult, withOnAbort, true, registry);
    }

    /**
     * @param parkedResult bytes the parked {@code read} returns, or {@code -1} for EOF
     */
    private static long assertAbortDuringRead(
        byte[] payload,
        int prefixLen,
        int parkedResult,
        boolean withOnAbort,
        boolean abortTwice,
        RecordingMeterRegistry registry
    ) throws Exception {
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "s3");
        ParkingInputStream inner = new ParkingInputStream(payload, prefixLen);
        MeteredInputStream in = withOnAbort
            ? new MeteredInputStream(inner, counters, inner::close)
            : new MeteredInputStream(inner, counters);

        if (prefixLen > 0) {
            byte[] prefixBuf = new byte[prefixLen];
            assertEquals(prefixLen, in.read(prefixBuf));
        }

        AtomicReference<Throwable> abortError = new AtomicReference<>();
        Thread abortThread = new Thread(() -> {
            try {
                assertTrue("reader parked", inner.parked.await(10, TimeUnit.SECONDS));
                in.abort();
                if (abortTwice) {
                    in.abort();
                }
            } catch (Throwable e) {
                abortError.set(e);
            }
        }, "metered-abort");
        abortThread.start();
        byte[] buf = parkedResult > 0 ? new byte[parkedResult] : new byte[1];
        int n;
        try {
            n = in.read(buf);
        } finally {
            joinThread(abortThread);
        }
        throwIfPresent(abortError.get());
        assertEquals(parkedResult, n);
        in.close();

        long delivered = prefixLen + Math.max(n, 0);
        long profile = counters.snapshot().bytesRead();
        assertEquals(delivered, profile);
        assertEquals(profile, apmSum(registry));
        return profile;
    }

    private static long apmSum(RecordingMeterRegistry registry) {
        long sum = 0L;
        for (Measurement m : apmEvents(registry)) {
            sum += m.getLong();
        }
        return sum;
    }

    private static List<Measurement> apmEvents(RecordingMeterRegistry registry) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL);
    }

    private static void throwIfPresent(Throwable t) {
        if (t != null) {
            throw new AssertionError(t);
        }
    }

    private static void joinThread(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(10));
        if (thread.isAlive()) {
            thread.interrupt();
            fail(thread.getName() + " did not finish");
        }
    }

    /**
     * Serves {@code prefixLen} bytes immediately, then parks the next {@code read} until
     * {@link #close()}, then returns the remainder (or {@code -1} when the prefix was the whole
     * payload).
     */
    private static final class ParkingInputStream extends InputStream {
        private final byte[] payload;
        private final int prefixLen;
        final CountDownLatch parked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private int pos;
        private boolean parkedReadDone;

        ParkingInputStream(byte[] payload, int prefixLen) {
            this.payload = payload;
            this.prefixLen = prefixLen;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n == -1 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (pos < prefixLen) {
                int n = Math.min(len, prefixLen - pos);
                System.arraycopy(payload, pos, b, off, n);
                pos += n;
                return n;
            }
            if (parkedReadDone) {
                return -1;
            }
            parked.countDown();
            try {
                if (release.await(10, TimeUnit.SECONDS) == false) {
                    throw new IOException("parked read was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            parkedReadDone = true;
            if (pos >= payload.length) {
                return -1;
            }
            int n = Math.min(len, payload.length - pos);
            System.arraycopy(payload, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void close() {
            release.countDown();
        }
    }

    /** After {@code close()}, further reads return EOF; an in-flight {@code super.read} may still deliver. */
    private static final class CloseReturnsEofStream extends FilterInputStream {
        private final AtomicBoolean closed = new AtomicBoolean();

        CloseReturnsEofStream(byte[] data) {
            super(new ByteArrayInputStream(data));
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed.get()) {
                return -1;
            }
            return super.read(b, off, len);
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
