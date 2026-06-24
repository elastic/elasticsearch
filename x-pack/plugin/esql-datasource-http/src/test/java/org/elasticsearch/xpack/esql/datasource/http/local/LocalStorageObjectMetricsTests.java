/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests that {@link LocalStorageObject} increments its
 * {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetricsCounters}
 * exactly once per I/O entry point. Local FS is the easiest provider to drive
 * deterministically with real I/O, so the counters are exercised end-to-end
 * (no mocks).
 */
public class LocalStorageObjectMetricsTests extends ESTestCase {

    private static final byte[] PAYLOAD = "Hello, metrics! This payload is exactly 64 bytes long for testing!".getBytes(
        StandardCharsets.UTF_8
    );

    public void testRangeReadIncrementsCounters() throws IOException {
        Path tempFile = createTempFile("metrics", ".bin");
        Files.write(tempFile, PAYLOAD);

        LocalStorageObject object = new LocalStorageObject(tempFile);
        assertEquals(StorageObjectMetrics.ZERO, object.metrics());

        int rangeLen = 16;
        try (InputStream stream = object.newStream(0, rangeLen)) {
            byte[] buf = stream.readAllBytes();
            assertEquals(rangeLen, buf.length);
        }

        StorageObjectMetrics snapshot = object.metrics();
        assertEquals("expected exactly one request after newStream(pos, len)", 1L, snapshot.requestCount());
        assertEquals("range request records the requested length", (long) rangeLen, snapshot.bytesRead());
        assertEquals("local FS has no SDK retries", 0L, snapshot.retryCount());
    }

    public void testFullStreamIncrementsCounters() throws IOException {
        Path tempFile = createTempFile("metrics", ".bin");
        Files.write(tempFile, PAYLOAD);

        LocalStorageObject object = new LocalStorageObject(tempFile);

        try (InputStream stream = object.newStream()) {
            byte[] buf = stream.readAllBytes();
            assertEquals(PAYLOAD.length, buf.length);
        }

        StorageObjectMetrics afterFull = object.metrics();
        assertEquals("expected exactly one request after newStream()", 1L, afterFull.requestCount());
        assertEquals("newStream() records the file size", (long) PAYLOAD.length, afterFull.bytesRead());
        assertEquals(0L, afterFull.retryCount());
    }

    public void testRangeAndFullCombinedIncrement() throws IOException {
        Path tempFile = createTempFile("metrics", ".bin");
        Files.write(tempFile, PAYLOAD);

        LocalStorageObject object = new LocalStorageObject(tempFile);

        int rangeLen = 8;
        try (InputStream rangeStream = object.newStream(4, rangeLen)) {
            assertEquals(rangeLen, rangeStream.readAllBytes().length);
        }
        try (InputStream fullStream = object.newStream()) {
            assertEquals(PAYLOAD.length, fullStream.readAllBytes().length);
        }

        StorageObjectMetrics snapshot = object.metrics();
        assertEquals("two requests after one range + one full read", 2L, snapshot.requestCount());
        assertEquals("bytesRead = range length + full file size", (long) rangeLen + PAYLOAD.length, snapshot.bytesRead());
    }

    public void testReadBytesIncrementsCounters() throws IOException {
        Path tempFile = createTempFile("metrics", ".bin");
        Files.write(tempFile, PAYLOAD);

        LocalStorageObject object = new LocalStorageObject(tempFile);

        ByteBuffer buf = ByteBuffer.allocate(12);
        int read = object.readBytes(2, buf);
        assertEquals(12, read);

        StorageObjectMetrics snapshot = object.metrics();
        assertEquals("readBytes increments request count by one", 1L, snapshot.requestCount());
        assertEquals("readBytes records the bytes actually read", 12L, snapshot.bytesRead());
        assertEquals(0L, snapshot.retryCount());
    }

    public void testMetadataProbesAreNotCounted() throws IOException {
        Path tempFile = createTempFile("metrics", ".bin");
        Files.write(tempFile, PAYLOAD);

        LocalStorageObject object = new LocalStorageObject(tempFile);

        // Pure metadata calls — these must not bump the I/O counters.
        assertTrue(object.exists());
        assertEquals(PAYLOAD.length, object.length());
        assertNotNull(object.lastModified());

        assertEquals("metadata probes do not count as data reads", StorageObjectMetrics.ZERO, object.metrics());
    }
}
