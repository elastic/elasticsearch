/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class RangeStorageObjectTests extends ESTestCase {

    private static final byte[] FILE_BYTES = "Hello, World! This is test data for range reads.".getBytes(StandardCharsets.UTF_8);

    public void testNewStreamReadsFromOffset() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        try (InputStream stream = range.newStream()) {
            byte[] result = stream.readAllBytes();
            assertEquals("World!", new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testNewStreamWithPositionAddsToOffset() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 20);

        try (InputStream stream = range.newStream(7, 4)) {
            byte[] result = stream.readAllBytes();
            assertEquals("This", new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testLengthReturnsRangeLength() {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 10, 25);
        assertEquals(25, range.length());
    }

    public void testPathDelegates() {
        StoragePath path = StoragePath.of("s3://bucket/file.csv");
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES, path);
        RangeStorageObject range = new RangeStorageObject(delegate, 0, 10);
        assertEquals(path, range.path());
    }

    public void testExistsDelegates() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 0, 10);
        assertTrue(range.exists());
    }

    public void testLastModifiedDelegates() throws IOException {
        Instant now = Instant.now();
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES, StoragePath.of("s3://bucket/test"), now);
        RangeStorageObject range = new RangeStorageObject(delegate, 0, 10);
        assertEquals(now, range.lastModified());
    }

    public void testZeroOffsetFullLength() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 0, FILE_BYTES.length);

        try (InputStream stream = range.newStream()) {
            byte[] result = stream.readAllBytes();
            assertArrayEquals(FILE_BYTES, result);
        }
    }

    public void testReadBytesAddsOffset() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        ByteBuffer buf = ByteBuffer.allocate(6);
        int bytesRead = range.readBytes(0, buf);
        assertEquals(6, bytesRead);
        buf.flip();
        byte[] result = new byte[6];
        buf.get(result);
        assertEquals("World!", new String(result, StandardCharsets.UTF_8));
    }

    public void testReadBytesWithDirectBuffer() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        ByteBuffer buf = ByteBuffer.allocateDirect(6);
        int bytesRead = range.readBytes(0, buf);
        assertEquals(6, bytesRead);
        buf.flip();
        byte[] result = new byte[6];
        buf.get(result);
        assertEquals("World!", new String(result, StandardCharsets.UTF_8));
    }

    public void testReadBytesPastRangeReturnsMinusOne() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        ByteBuffer buf = ByteBuffer.allocate(10);
        int bytesRead = range.readBytes(6, buf);
        assertEquals(-1, bytesRead);
        assertEquals(0, buf.position());
    }

    public void testNegativeOffsetThrows() {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RangeStorageObject(delegate, -1, 10));
        assertTrue(e.getMessage().contains("offset must be >= 0"));
    }

    public void testNegativeLengthThrows() {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RangeStorageObject(delegate, 0, -1));
        assertTrue(e.getMessage().contains("length must be >= 0"));
    }

    private static class InMemoryStorageObject implements StorageObject {
        private final byte[] data;
        private final StoragePath path;
        private final Instant lastModified;

        InMemoryStorageObject(byte[] data) {
            this(data, StoragePath.of("s3://bucket/test"), Instant.EPOCH);
        }

        InMemoryStorageObject(byte[] data, StoragePath path) {
            this(data, path, Instant.EPOCH);
        }

        InMemoryStorageObject(byte[] data, StoragePath path, Instant lastModified) {
            this.data = data;
            this.path = path;
            this.lastModified = lastModified;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return lastModified;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
