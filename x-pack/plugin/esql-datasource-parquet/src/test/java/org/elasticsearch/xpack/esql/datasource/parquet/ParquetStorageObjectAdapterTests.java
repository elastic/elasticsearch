/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.io.SeekableInputStream;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;

public class ParquetStorageObjectAdapterTests extends ESTestCase {

    public void testNullStorageObjectThrowsException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ParquetStorageObjectAdapter(null));
        assertEquals("storageObject cannot be null", e.getMessage());
    }

    public void testGetLength() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        assertEquals(1024, adapter.getLength());
    }

    public void testNewStreamReturnsSeekableInputStream() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertNotNull(stream);
            assertEquals(0, stream.getPos());
        }
    }

    public void testSeekableInputStreamRead() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(1, stream.read());
            assertEquals(1, stream.getPos());
            assertEquals(2, stream.read());
            assertEquals(2, stream.getPos());
        }
    }

    public void testSeekableInputStreamReadArray() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buffer = new byte[5];
            int bytesRead = stream.read(buffer);
            assertEquals(5, bytesRead);
            assertEquals(5, stream.getPos());
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, buffer);
        }
    }

    public void testSeekableInputStreamSeekForward() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(5);
            assertEquals(5, stream.getPos());
            assertEquals(6, stream.read());
            assertEquals(6, stream.getPos());
        }
    }

    public void testSeekableInputStreamSeekBackward() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            // Read some bytes to advance position
            stream.read();
            stream.read();
            stream.read();
            assertEquals(3, stream.getPos());

            // Seek backward
            stream.seek(1);
            assertEquals(1, stream.getPos());
            assertEquals(2, stream.read());
        }
    }

    public void testSeekableInputStreamSeekToNegativePositionThrows() throws IOException {
        byte[] data = new byte[100];
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            IOException e = expectThrows(IOException.class, () -> stream.seek(-1));
            assertTrue(e.getMessage().contains("Cannot seek to negative position"));
        }
    }

    public void testSeekableInputStreamSeekBeyondEndThrows() throws IOException {
        byte[] data = new byte[100];
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            IOException e = expectThrows(IOException.class, () -> stream.seek(200));
            assertTrue(e.getMessage().contains("Cannot seek beyond end of file"));
        }
    }

    public void testSeekableInputStreamReadFully() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buffer = new byte[5];
            stream.readFully(buffer);
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, buffer);
            assertEquals(5, stream.getPos());
        }
    }

    public void testSeekableInputStreamReadFullyWithOffset() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buffer = new byte[10];
            stream.readFully(buffer, 2, 5);
            assertArrayEquals(new byte[] { 0, 0, 1, 2, 3, 4, 5, 0, 0, 0 }, buffer);
            assertEquals(5, stream.getPos());
        }
    }

    public void testSeekableInputStreamReadByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(5);
            int bytesRead = stream.read(buffer);
            assertEquals(5, bytesRead);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
        }
    }

    public void testSeekableInputStreamReadFullyByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(5);
            stream.readFully(buffer);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
            assertEquals(4, buffer.get());
            assertEquals(5, buffer.get());
        }
    }

    public void testSeekableInputStreamSkip() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            long skipped = stream.skip(3);
            assertEquals(3, skipped);
            assertEquals(3, stream.getPos());
            assertEquals(4, stream.read());
        }
    }

    private void randomBytes(byte[] data) {
        random().nextBytes(data);
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                // Simple implementation that doesn't support range reads
                throw new UnsupportedOperationException("Range reads not supported in basic test");
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };
    }

    private StorageObject createRangeReadStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };
    }
}
