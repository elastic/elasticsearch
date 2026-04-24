/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.io.SeekableInputStream;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ParquetStorageObjectAdapterTests extends ESTestCase {

    public void testNullStorageObjectThrowsException() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class, () -> new ParquetStorageObjectAdapter(null));
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
        StorageObject storageObject = createRangeReadStorageObject(data);

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
        StorageObject storageObject = createRangeReadStorageObject(data);

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
        StorageObject storageObject = createRangeReadStorageObject(data);

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
        StorageObject storageObject = createRangeReadStorageObject(data);

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
        StorageObject storageObject = createRangeReadStorageObject(data);

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
        StorageObject storageObject = createRangeReadStorageObject(data);

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
        StorageObject storageObject = createRangeReadStorageObject(data);

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

    public void testReadByteBufferWithNonZeroPosition() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(10);
            buffer.position(3);
            int bytesRead = stream.read(buffer);
            assertEquals(7, bytesRead);
            assertEquals(10, buffer.position());
            buffer.flip();
            buffer.position(3);
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
        }
    }

    public void testReadFullyByteBufferWithNonZeroPosition() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.position(3);
            stream.readFully(buffer);
            assertEquals(8, buffer.position());
            buffer.flip();
            buffer.position(3);
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
            assertEquals(4, buffer.get());
            assertEquals(5, buffer.get());
        }
    }

    public void testReadDirectByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(5);
            assertFalse(buffer.hasArray());
            int bytesRead = stream.read(buffer);
            assertEquals(5, bytesRead);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
        }
    }

    public void testReadFullyDirectByteBuffer() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(5);
            assertFalse(buffer.hasArray());
            stream.readFully(buffer);
            buffer.flip();
            assertEquals(1, buffer.get());
            assertEquals(2, buffer.get());
            assertEquals(3, buffer.get());
            assertEquals(4, buffer.get());
            assertEquals(5, buffer.get());
        }
    }

    public void testByteBufferReadThenByteArrayRead() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buf = ByteBuffer.allocate(3);
            stream.readFully(buf);
            buf.flip();
            assertEquals(1, buf.get());
            assertEquals(2, buf.get());
            assertEquals(3, buf.get());
            assertEquals(3, stream.getPos());

            // Byte-array read continues from same stream position
            byte[] arr = new byte[3];
            stream.readFully(arr);
            assertArrayEquals(new byte[] { 4, 5, 6 }, arr);
            assertEquals(6, stream.getPos());
        }
    }

    public void testSeekThenByteBufferRead() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(5);
            ByteBuffer buf = ByteBuffer.allocate(3);
            stream.readFully(buf);
            buf.flip();
            assertEquals(6, buf.get());
            assertEquals(7, buf.get());
            assertEquals(8, buf.get());
            assertEquals(8, stream.getPos());
        }
    }

    public void testReadByteBufferAtEofReturnsMinusOne() throws IOException {
        byte[] data = new byte[] { 1, 2, 3 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(3);
            ByteBuffer buf = ByteBuffer.allocate(5);
            int bytesRead = stream.read(buf);
            assertEquals(-1, bytesRead);
            assertEquals(0, buf.position());
        }
    }

    public void testReadFullyByteBufferAtEofThrows() throws IOException {
        byte[] data = new byte[] { 1, 2, 3 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(3);
            ByteBuffer buf = ByteBuffer.allocate(5);
            expectThrows(IOException.class, () -> stream.readFully(buf));
        }
    }

    public void testDirectByteBufferLargerThanTransferBuffer() throws IOException {
        byte[] data = new byte[StorageObject.TRANSFER_BUFFER_SIZE * 3];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
            assertFalse(buf.hasArray());
            stream.readFully(buf);
            buf.flip();
            for (int i = 0; i < data.length; i++) {
                assertEquals("Mismatch at position " + i, data[i], buf.get());
            }
        }
    }

    public void testSeekableInputStreamSkip() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            long skipped = stream.skip(3);
            assertEquals(3, skipped);
            assertEquals(3, stream.getPos());
            assertEquals(4, stream.read());
        }
    }

    /**
     * Verifies that the adapter never calls {@link StorageObject#newStream()} (full GET).
     * A stub that throws on newStream() is used; all operations succeed via newStream(pos, len) only.
     */
    public void testNoFullGetUsesOnlyRangeReads() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject rangeOnlyStorageObject = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported; use range reads only");
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

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(rangeOnlyStorageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(0, stream.getPos());
            assertEquals(1, stream.read());
            assertEquals(2, stream.read());
            stream.seek(5);
            assertEquals(6, stream.read());
            stream.seek(0);
            byte[] buf = new byte[5];
            stream.readFully(buf);
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, buf);
        }
    }

    /**
     * Verifies that repeated reads within the same window do not trigger additional range requests.
     * The sliding window should serve multiple reads from the same cached range.
     */
    public void testSeekWindowBehaviorReusesCachedRange() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };
        StorageObject countingStorageObject = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
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

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(countingStorageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.read();
            stream.read();
            stream.read();
            stream.seek(0);
            stream.read();
            stream.seek(10);
            stream.read();
        }

        assertTrue("Expected few range reads (window reuse); got " + rangeReadCount[0], rangeReadCount[0] <= 2);
    }

    public void testFooterCacheServesTailReadsWithoutExtraRangeRequest() throws IOException {
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        byte[] data = new byte[1024 * 1024];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };
        Instant lastModified = Instant.ofEpochMilli(123);

        StorageObject countingStorageObject = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
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
                return lastModified;
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

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(countingStorageObject);
        long tailStart = data.length - 1024;

        byte[] first = new byte[1024];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(tailStart);
            stream.readFully(first);
        }
        assertEquals(1, rangeReadCount[0]);

        byte[] second = new byte[1024];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(tailStart);
            stream.readFully(second);
        }
        assertEquals(1, rangeReadCount[0]);
        assertArrayEquals(first, second);
    }

    public void testReadFullyCrossesWindowBoundaries() throws IOException {
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 123;
        byte[] data = new byte[size];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };

        StorageObject rangeOnlyCounting = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
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
                return Instant.ofEpochMilli(0);
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

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(rangeOnlyCounting);
        byte[] read = new byte[size];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(read);
        }
        assertArrayEquals(data, read);
        assertEquals(2, rangeReadCount[0]);
    }

    // --- Adaptive window tests ---

    public void testDefaultWindowUnchanged() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertNotNull(stream);
            assertEquals(0, stream.getPos());
        }
    }

    public void testAdaptiveWindowSmallHint() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        long rangeBytes = 8 * 1024 * 1024L; // 8 MiB
        ParquetStorageObjectAdapter adapter = ParquetStorageObjectAdapter.forRange(storageObject, rangeBytes);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertNotNull(stream);
            byte[] buf = new byte[data.length];
            stream.readFully(buf);
            assertArrayEquals(data, buf);
        }
    }

    public void testAdaptiveWindowCappedAtMax() throws IOException {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 123;
        byte[] data = new byte[size];
        randomBytes(data);
        final int[] maxRequestedLength = { 0 };

        StorageObject measuringStorageObject = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                maxRequestedLength[0] = Math.max(maxRequestedLength[0], (int) length);
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
                return Instant.ofEpochMilli(0);
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

        long hugeRange = 64 * 1024 * 1024L; // 64 MiB — should be capped to MAX_WINDOW_SIZE
        ParquetStorageObjectAdapter adapter = ParquetStorageObjectAdapter.forRange(measuringStorageObject, hugeRange);

        byte[] buf = new byte[size];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(buf);
        }
        assertArrayEquals(data, buf);
        assertTrue(
            "Requested range length should not exceed MAX_WINDOW_SIZE, got " + maxRequestedLength[0],
            maxRequestedLength[0] <= ParquetStorageObjectAdapter.MAX_WINDOW_SIZE
        );
    }

    public void testAdaptiveWindowFloorAtDefault() throws IOException {
        byte[] data = new byte[100];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        long tinyRange = 1024L; // 1 KiB — should be floored to DEFAULT_WINDOW_SIZE
        ParquetStorageObjectAdapter adapter = ParquetStorageObjectAdapter.forRange(storageObject, tinyRange);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buf = new byte[data.length];
            stream.readFully(buf);
            assertArrayEquals(data, buf);
        }
    }

    public void testAdaptiveWindowReducesRangeRequests() throws IOException {
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        int size = 6 * 1024 * 1024; // 6 MiB
        byte[] data = new byte[size];
        randomBytes(data);
        final int[] rangeReadCount = { 0 };

        StorageObject countingStorageObject = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount[0]++;
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
                return Instant.ofEpochMilli(42);
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test-adaptive.parquet");
            }
        };

        // With default 4 MiB window, reading 6 MiB requires 2 range requests
        ParquetStorageObjectAdapter defaultAdapter = new ParquetStorageObjectAdapter(countingStorageObject);
        byte[] buf = new byte[size];
        try (SeekableInputStream stream = defaultAdapter.newStream()) {
            stream.readFully(buf);
        }
        int defaultReads = rangeReadCount[0];

        // With 8 MiB adaptive window, reading 6 MiB requires 1 range request
        rangeReadCount[0] = 0;
        ParquetStorageObjectAdapter adaptiveAdapter = ParquetStorageObjectAdapter.forRange(countingStorageObject, 8 * 1024 * 1024L);
        try (SeekableInputStream stream = adaptiveAdapter.newStream()) {
            stream.readFully(buf);
        }
        int adaptiveReads = rangeReadCount[0];

        assertTrue(
            "Adaptive window should use fewer range reads (" + adaptiveReads + ") than default (" + defaultReads + ")",
            adaptiveReads < defaultReads
        );
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

    public void testPrefetchedDataServedFromMemory() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        AtomicInteger readBytesCalls = new AtomicInteger();
        StorageObject tracked = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) length);
            }

            @Override
            public int readBytes(long position, ByteBuffer target) throws IOException {
                readBytesCalls.incrementAndGet();
                return StorageObject.super.readBytes(position, target);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://prefetch-test.parquet");
            }
        };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(tracked);
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new TreeMap<>();
        int chunkLen = data.length - 100;
        ByteBuffer prefetchedBuf = ByteBuffer.wrap(data, 100, chunkLen);
        chunks.put(100L, new ColumnChunkPrefetcher.PrefetchedChunk(100, chunkLen, prefetchedBuf.slice()));
        adapter.installPrefetchedData(chunks);

        readBytesCalls.set(0);
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(100);
            byte[] result = new byte[100];
            stream.readFully(result);
            for (int i = 0; i < result.length; i++) {
                assertEquals(data[100 + i], result[i]);
            }
        }
        assertEquals(0, readBytesCalls.get());
    }

    public void testPrefetchedDataFallbackOnMiss() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        StorageObject storage = createRangeReadStorageObject(data);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new TreeMap<>();
        ByteBuffer prefetchedBuf = ByteBuffer.wrap(data, 100, 200);
        chunks.put(100L, new ColumnChunkPrefetcher.PrefetchedChunk(100, 200, prefetchedBuf.slice()));
        adapter.installPrefetchedData(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(500);
            byte[] result = new byte[100];
            stream.readFully(result);
            for (int i = 0; i < result.length; i++) {
                assertEquals(data[500 + i], result[i]);
            }
        }
    }

    public void testClearPrefetchedData() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        AtomicInteger readBytesCalls = new AtomicInteger();
        StorageObject tracked = new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) length);
            }

            @Override
            public int readBytes(long position, ByteBuffer target) throws IOException {
                readBytesCalls.incrementAndGet();
                return StorageObject.super.readBytes(position, target);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://clear-test.parquet");
            }
        };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(tracked);
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new TreeMap<>();
        ByteBuffer buf = ByteBuffer.wrap(data, 100, 500);
        chunks.put(100L, new ColumnChunkPrefetcher.PrefetchedChunk(100, 500, buf.slice()));
        adapter.installPrefetchedData(chunks);
        adapter.clearPrefetchedData();

        readBytesCalls.set(0);
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(100);
            byte[] result = new byte[100];
            stream.readFully(result);
        }
        assertTrue(readBytesCalls.get() > 0);
    }

    public void testNoPrefetchedDataDoesNotAffectBaseline() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        StorageObject storage = createRangeReadStorageObject(data);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] result = new byte[100];
            stream.readFully(result);
            for (int i = 0; i < result.length; i++) {
                assertEquals(data[i], result[i]);
            }
        }
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
