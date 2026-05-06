/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ParquetStorageObjectAdapterTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
    }

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

    /**
     * Each new stream fetches the tail independently; there is no cross-stream footer cache.
     */
    public void testSeparateStreamsEachFetchTailViaRangeRead() throws IOException {
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
        // Second tail read is served from the JVM-wide FooterByteCache — no additional range read
        assertEquals(1, rangeReadCount[0]);
        assertArrayEquals(first, second);
    }

    public void testReadFullyCrossesWindowBoundaries() throws IOException {
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
        // Exactly 2: one full 4 MiB window, one for the remaining 123-byte tail.
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

    /**
     * Verifies that seek+read sequences return correct data with the sliding window. This pattern
     * mimics Parquet's column chunk access: seek to a dictionary page, read it, seek to a data
     * page, read it, possibly seek back.
     */
    public void testDirectStreamSeekAndReadCorrectness() throws IOException {
        byte[] data = new byte[64 * 1024];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] buf = new byte[1024];
            stream.seek(4096);
            stream.readFully(buf);
            for (int i = 0; i < buf.length; i++) {
                assertEquals("Mismatch at first read offset " + i, data[4096 + i], buf[i]);
            }

            stream.seek(32768);
            stream.readFully(buf);
            for (int i = 0; i < buf.length; i++) {
                assertEquals("Mismatch at second read offset " + i, data[32768 + i], buf[i]);
            }

            stream.seek(4096);
            stream.readFully(buf);
            for (int i = 0; i < buf.length; i++) {
                assertEquals("Mismatch at re-read offset " + i, data[4096 + i], buf[i]);
            }
        }
    }

    public void testReadFullyMatchesSourceData() throws IOException {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 12345;
        byte[] data = new byte[size];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);
        byte[] result = new byte[size];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(result);
        }

        assertArrayEquals(data, result);
    }

    /**
     * Verifies that interleaved seek+read of non-overlapping regions returns correct bytes.
     * This simulates Parquet reading dictionary and data pages from different column chunks within a row group.
     */
    public void testInterleavedSeekReadParity() throws IOException {
        int size = 8 * 1024 * 1024;
        byte[] data = new byte[size];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        int[][] readRegions = new int[][] {
            { 0, 4096 },
            { 1_000_000, 8192 },
            { 4_000_000, 16384 },
            { 100, 2048 },
            { 7_000_000, 4096 },
            { 2_000_000, 32768 },
            { 0, 4096 } };

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            for (int[] region : readRegions) {
                int offset = region[0];
                int len = region[1];
                byte[] buf = new byte[len];
                stream.seek(offset);
                stream.readFully(buf);
                for (int i = 0; i < len; i++) {
                    assertEquals("Mismatch at offset " + (offset + i), data[offset + i], buf[i]);
                }
            }
        }
    }

    /**
     * Verifies single-byte reads, ByteBuffer reads, and skip operations on the windowed stream.
     */
    public void testDirectStreamOperations() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            assertEquals(1, stream.read());
            assertEquals(1, stream.getPos());

            long skipped = stream.skip(3);
            assertEquals(3, skipped);
            assertEquals(4, stream.getPos());
            assertEquals(5, stream.read());

            stream.seek(0);
            ByteBuffer buf = ByteBuffer.allocate(5);
            stream.readFully(buf);
            buf.flip();
            assertEquals(1, buf.get());
            assertEquals(2, buf.get());
            assertEquals(3, buf.get());
            assertEquals(4, buf.get());
            assertEquals(5, buf.get());

            stream.seek(8);
            ByteBuffer directBuf = ByteBuffer.allocateDirect(2);
            stream.readFully(directBuf);
            directBuf.flip();
            assertEquals(9, directBuf.get());
            assertEquals(10, directBuf.get());

            stream.seek(10);
            assertEquals(-1, stream.read());
        }
    }

    /**
     * End-to-end regression test for dictionary-encoded SUM/COUNT correctness. Writes a Parquet file with a
     * dictionary-encoded {@code int64} column spanning multiple data pages, then fully decodes
     * it via {@link ParquetFileReader} (which drives {@link ParquetStorageObjectAdapter}) across
     * multiple iterations. Any per-iteration drift in the sum or row count would be a correctness bug.
     */
    public void testDictionaryEncodedSumCountRegression() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("dict_regression");

        long[] dictionary = new long[] { 1L, 2L, 3L, 5L, 7L, 11L, 13L, 17L };
        int rowCount = 25_000;
        long expectedSum = 0;
        Random seeded = new Random(0xD1C77L);
        long[] expectedValues = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            expectedValues[i] = dictionary[seeded.nextInt(dictionary.length)];
            expectedSum += expectedValues[i];
        }

        byte[] parquetData = writeDictionaryEncodedInt64Parquet(schema, expectedValues);
        StorageObject storageObject = createRangeReadStorageObject(parquetData);

        int iterations = 20;
        // Use the Hadoop-free builder; the upstream ParquetReadOptions.builder() eagerly constructs
        // HadoopParquetConfiguration, which fails in tests without Woodstox on the classpath.
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        for (int iter = 0; iter < iterations; iter++) {
            ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

            long sum = 0;
            long count = 0;
            try (ParquetFileReader reader = ParquetFileReader.open(adapter, options)) {
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    long rgRows = pages.getRowCount();
                    for (long i = 0; i < rgRows; i++) {
                        Group g = recordReader.read();
                        sum += g.getLong(0, 0);
                        count++;
                    }
                }
            }

            assertEquals("COUNT mismatch on iter=" + iter, rowCount, count);
            assertEquals("SUM mismatch on iter=" + iter, expectedSum, sum);
        }
    }

    /**
     * Concurrency stress test for {@code WindowedSeekableInputStream}: one adapter shared, many streams opened from it.
     *
     * <p>Target runtime is capped at ~2s regardless of hardware to avoid flaking CI.
     */
    public void testConcurrentWindowedReadsProduceConsistentBytes() throws Exception {
        int size = ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE * 2 + 7777;
        byte[] data = new byte[size];
        randomBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);

        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        int threadCount = randomIntBetween(4, 8);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<AssertionError> firstFailure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>(threadCount);

        for (int t = 0; t < threadCount; t++) {
            long seed = randomLong();
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    Random rnd = new Random(seed);
                    try (SeekableInputStream stream = adapter.newStream()) {
                        while (System.nanoTime() < deadline && firstFailure.get() == null) {
                            int readLen = 1 + rnd.nextInt(4096);
                            int offset = rnd.nextInt(Math.max(1, size - readLen));
                            byte[] buf = new byte[readLen];
                            stream.seek(offset);
                            stream.readFully(buf);
                            for (int i = 0; i < readLen; i++) {
                                if (buf[i] != data[offset + i]) {
                                    firstFailure.compareAndSet(
                                        null,
                                        new AssertionError(
                                            "Stale byte at offset " + (offset + i) + ": expected " + data[offset + i] + " got " + buf[i]
                                        )
                                    );
                                    return;
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    firstFailure.compareAndSet(null, new AssertionError("Unexpected exception on stress thread", e));
                }
            }, "parquet-window-stress-" + t);
            thread.start();
            threads.add(thread);
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(10));
        }

        AssertionError failure = firstFailure.get();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Two adapters wrapping different {@link StorageObject} instances (same path and bytes, different
     * {@code lastModified}) each perform their own range reads; there is no cross-adapter cache.
     */
    public void testSeparateAdaptersDoNotShareCachedRanges() throws IOException {
        byte[] data = new byte[1024 * 1024];
        random().nextBytes(data);
        final int[] rangeReadCount = { 0 };
        final StoragePath path = StoragePath.of("memory://cache-key-test.parquet");

        StorageObject obj1 = createCountingStorageObject(data, path, Instant.ofEpochMilli(100), rangeReadCount);
        StorageObject obj2 = createCountingStorageObject(data, path, Instant.ofEpochMilli(500), rangeReadCount);

        int tailStart = data.length - 1024;
        byte[] expected = java.util.Arrays.copyOfRange(data, tailStart, data.length);

        ParquetStorageObjectAdapter first = new ParquetStorageObjectAdapter(obj1);
        try (SeekableInputStream s = first.newStream()) {
            s.seek(tailStart);
            byte[] w = new byte[1024];
            s.readFully(w);
            assertArrayEquals(expected, w);
        }
        assertEquals(1, rangeReadCount[0]);

        ParquetStorageObjectAdapter second = new ParquetStorageObjectAdapter(obj2);
        try (SeekableInputStream s = second.newStream()) {
            s.seek(tailStart);
            byte[] w = new byte[1024];
            s.readFully(w);
            assertArrayEquals(expected, w);
        }
        // Both adapters share the same (path, length) cache key, so the second read is a cache hit
        assertEquals("FooterByteCache coalesces reads for same path+length", 1, rangeReadCount[0]);
    }

    /**
     * Concurrent threads each open their own stream and read the tail; without a shared footer cache,
     * each stream performs its own range read(s).
     */
    public void testConcurrentTailReadsEachIssueRangeRequest() throws Exception {
        byte[] data = new byte[1024 * 1024];
        random().nextBytes(data);
        final AtomicInteger rangeReadCount = new AtomicInteger();
        final StoragePath path = StoragePath.of("memory://thundering-herd.parquet");
        final java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(8);

        StorageObject obj = new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount.incrementAndGet();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
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
                return Instant.ofEpochMilli(100);
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return path;
            }
        };

        int tailStart = data.length - 1024;
        byte[] expected = java.util.Arrays.copyOfRange(data, tailStart, data.length);
        Thread[] threads = new Thread[8];
        AtomicReference<AssertionError> firstFailure = new AtomicReference<>();

        for (int i = 0; i < 8; i++) {
            threads[i] = new Thread(() -> {
                try {
                    ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(obj);
                    try (SeekableInputStream s = adapter.newStream()) {
                        barrier.await(5, TimeUnit.SECONDS);
                        s.seek(tailStart);
                        byte[] w = new byte[1024];
                        s.readFully(w);
                        assertArrayEquals(expected, w);
                    }
                } catch (AssertionError e) {
                    firstFailure.compareAndSet(null, e);
                } catch (Exception e) {
                    firstFailure.compareAndSet(null, new AssertionError("Unexpected exception", e));
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
        }

        AssertionError failure = firstFailure.get();
        if (failure != null) {
            throw failure;
        }
        // FooterByteCache provides thundering-herd protection: concurrent tail reads for the same
        // (path, length) coalesce into a single I/O via Cache.computeIfAbsent
        assertEquals("FooterByteCache should coalesce concurrent tail reads into one range read", 1, rangeReadCount.get());
    }

    /**
     * Validates byte-for-byte correctness of the sliding-window adapter against raw data:
     * sequential reads, forward seeks, backward seeks, tail reads, and ByteBuffer reads.
     */
    public void testAdapterReadsIdenticalBytesToVanillaStream() throws IOException {
        int n = 2 * ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 500;
        byte[] data = new byte[n];
        random().nextBytes(data);
        StorageObject storageObject = createRangeReadStorageObject(data);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storageObject);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] got = new byte[100];
            assertEquals(100, stream.read(got));
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 0, 100), got);

            stream.seek(1000);
            got = new byte[200];
            assertEquals(200, stream.read(got, 0, 200));
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 1000, 1200), got);

            stream.seek(500);
            got = new byte[300];
            assertEquals(300, stream.read(got, 0, 300));
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 500, 800), got);

            long endPos = data.length - 100L;
            stream.seek(endPos);
            got = new byte[100];
            stream.readFully(got);
            assertArrayEquals(java.util.Arrays.copyOfRange(data, (int) endPos, data.length), got);

            stream.seek(2048);
            ByteBuffer buf = ByteBuffer.allocate(64);
            stream.readFully(buf);
            buf.flip();
            byte[] bufBytes = new byte[64];
            buf.get(bufBytes);
            assertArrayEquals(java.util.Arrays.copyOfRange(data, 2048, 2048 + 64), bufBytes);
        }
    }

    /**
     * Validates that the {@code forRange} adaptive-window adapter reads the same bytes as the
     * default-window adapter for identical seek/read sequences.
     */
    public void testForRangeAdapterReadsIdenticalData() throws IOException {
        int n = 2 * ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 500;
        byte[] data = new byte[n];
        random().nextBytes(data);
        StorageObject storage = createRangeReadStorageObject(data);
        ParquetStorageObjectAdapter defaultAdapter = new ParquetStorageObjectAdapter(storage);
        ParquetStorageObjectAdapter rangeAdapter = ParquetStorageObjectAdapter.forRange(storage, 8L * 1024 * 1024);

        try (SeekableInputStream a = defaultAdapter.newStream(); SeekableInputStream b = rangeAdapter.newStream()) {
            byte[] g1 = new byte[100];
            byte[] g2 = new byte[100];
            assertEquals(a.read(g1), b.read(g2));
            assertArrayEquals(g1, g2);

            a.seek(1000);
            b.seek(1000);
            g1 = new byte[200];
            g2 = new byte[200];
            assertEquals(a.read(g1, 0, 200), b.read(g2, 0, 200));
            assertArrayEquals(g1, g2);

            a.seek(500);
            b.seek(500);
            g1 = new byte[300];
            g2 = new byte[300];
            assertEquals(a.read(g1, 0, 300), b.read(g2, 0, 300));
            assertArrayEquals(g1, g2);

            long endPos = data.length - 100L;
            a.seek(endPos);
            b.seek(endPos);
            g1 = new byte[100];
            g2 = new byte[100];
            a.readFully(g1);
            b.readFully(g2);
            assertArrayEquals(g1, g2);
        }
    }

    private StorageObject createCountingStorageObject(byte[] data, StoragePath path, Instant lastModified, int[] counter) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                throw new UnsupportedOperationException("Full GET not supported");
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                counter[0]++;
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
                return path;
            }
        };
    }

    /**
     * Writes a Parquet file with a single {@code int64} column that is dictionary-encoded and
     * spans multiple data pages (small page size + few distinct values forces this). Used by
     * {@link #testDictionaryEncodedSumCountRegression()}.
     */
    private static byte[] writeDictionaryEncodedInt64Parquet(MessageType schema, long[] values) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(out);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.set("parquet.enable.dictionary", "true");

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(conf)
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .withRowGroupSize(64 * 1024L)
                .build()
        ) {
            for (long v : values) {
                Group g = groupFactory.newGroup();
                g.add("v", v);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    // --- Pre-warmed chunk cache tests ---

    /**
     * Reads that fall entirely inside a pre-warmed chunk must be served from memory; the
     * StorageObject must observe zero range GETs for those bytes.
     */
    public void testPreWarmedChunkServesReadFromMemory() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        // Pre-warm the [200, 400) range.
        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 200, 200).slice();
        chunks.put(200L, new ColumnChunkPrefetcher.PrefetchedChunk(200L, 200L, warm));
        adapter.installPreWarmedChunks(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            stream.seek(200);
            byte[] result = new byte[200];
            stream.readFully(result);
            for (int i = 0; i < 200; i++) {
                assertEquals("Mismatch at " + i, data[200 + i], result[i]);
            }
        }
        assertEquals("Pre-warmed read must not issue any range GETs", 0, rangeReadCount.get());
    }

    /**
     * When a position falls outside any pre-warmed chunk, the stream must fall back to the normal
     * range read path so correctness is preserved even with a pre-warm cache installed.
     */
    public void testReadOutsidePreWarmedChunkFallsBackToIO() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        // Pre-warm the [100, 200) range only.
        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 100, 100).slice();
        chunks.put(100L, new ColumnChunkPrefetcher.PrefetchedChunk(100L, 100L, warm));
        adapter.installPreWarmedChunks(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            // Read inside pre-warm.
            stream.seek(120);
            byte[] inside = new byte[40];
            stream.readFully(inside);
            for (int i = 0; i < 40; i++) {
                assertEquals(data[120 + i], inside[i]);
            }
            // Read outside pre-warm — must hit storage.
            stream.seek(1500);
            byte[] outside = new byte[100];
            stream.readFully(outside);
            for (int i = 0; i < 100; i++) {
                assertEquals(data[1500 + i], outside[i]);
            }
        }
        assertEquals("Read outside the pre-warmed range must trigger a single range GET", 1, rangeReadCount.get());
    }

    /**
     * Already-open streams must observe a pre-warm install that happens after their construction.
     * This matches the production wiring: parquet-mr opens the file's {@code SeekableInputStream}
     * during {@code ParquetFileReader.open}, before the caller has had a chance to install the
     * pre-warm map; without this property the optimization would be silently bypassed.
     */
    public void testInstallAfterStreamOpenAffectsAlreadyOpenStream() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        try (SeekableInputStream stream = adapter.newStream()) {
            // Install the pre-warm map AFTER the stream was opened — same shape as production.
            java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
            ByteBuffer warm = ByteBuffer.wrap(data, 0, 256).slice();
            chunks.put(0L, new ColumnChunkPrefetcher.PrefetchedChunk(0L, 256L, warm));
            adapter.installPreWarmedChunks(chunks);

            byte[] result = new byte[256];
            stream.readFully(result);
            for (int i = 0; i < 256; i++) {
                assertEquals(data[i], result[i]);
            }
        }
        assertEquals("Already-open stream must observe the post-construction install", 0, rangeReadCount.get());
    }

    /**
     * A clear that happens after the open stream consumed the pre-warmed bytes must still allow
     * subsequent reads to hit storage when needed. This guards the production sequence of
     * install → row-group filter → clear.
     */
    public void testClearAfterUseFallsBackToStorage() throws IOException {
        byte[] data = new byte[2048];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 0, 256).slice();
        chunks.put(0L, new ColumnChunkPrefetcher.PrefetchedChunk(0L, 256L, warm));
        adapter.installPreWarmedChunks(chunks);

        try (SeekableInputStream stream = adapter.newStream()) {
            byte[] warmBuf = new byte[256];
            stream.readFully(warmBuf);
            assertEquals(0, rangeReadCount.get());

            adapter.installPreWarmedChunks(null);

            stream.seek(1024);
            byte[] coldBuf = new byte[256];
            stream.readFully(coldBuf);
            for (int i = 0; i < 256; i++) {
                assertEquals(data[1024 + i], coldBuf[i]);
            }
            assertEquals("After clearing, reads outside the (now-detached) cache must hit storage", 1, rangeReadCount.get());
        }
    }

    /**
     * Streams created after an install observe the new map; streams created after a clear go
     * straight to the storage backend.
     */
    public void testInstallNullDisablesPreWarmForNewStreams() throws IOException {
        byte[] data = new byte[1024];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        java.util.NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = new java.util.TreeMap<>();
        ByteBuffer warm = ByteBuffer.wrap(data, 0, 256).slice();
        chunks.put(0L, new ColumnChunkPrefetcher.PrefetchedChunk(0L, 256L, warm));
        adapter.installPreWarmedChunks(chunks);
        adapter.installPreWarmedChunks(null);

        byte[] result = new byte[256];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(result);
            for (int i = 0; i < 256; i++) {
                assertEquals(data[i], result[i]);
            }
        }
        assertEquals("Stream created after clear must use real I/O", 1, rangeReadCount.get());
    }

    /**
     * Empty maps are treated identically to {@code null}: the cache is left disabled. This
     * matches the production wiring where {@link PreloadedRowGroupMetadata} returns an empty map
     * when no predicate columns were supplied.
     */
    public void testInstallEmptyMapTreatedAsCleared() throws IOException {
        byte[] data = new byte[256];
        randomBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();
        StorageObject storage = createCountingRangeReadStorageObject(data, rangeReadCount);
        ParquetStorageObjectAdapter adapter = new ParquetStorageObjectAdapter(storage);

        adapter.installPreWarmedChunks(new java.util.TreeMap<>());

        byte[] result = new byte[256];
        try (SeekableInputStream stream = adapter.newStream()) {
            stream.readFully(result);
        }
        assertEquals("Empty map must not enable the pre-warm fast path", 1, rangeReadCount.get());
    }

    private StorageObject createCountingRangeReadStorageObject(byte[] data, AtomicInteger counter) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException("Full GET not supported in counting harness");
            }

            @Override
            public InputStream newStream(long position, long length) {
                counter.incrementAndGet();
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.ofEpochMilli(0);
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://prewarm-test.parquet");
            }
        };
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return out.size();
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://dict-regression.parquet";
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
