/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
}
