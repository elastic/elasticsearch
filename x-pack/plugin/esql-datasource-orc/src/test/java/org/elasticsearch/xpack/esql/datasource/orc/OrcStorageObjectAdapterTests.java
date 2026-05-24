/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class OrcStorageObjectAdapterTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        OrcStorageObjectAdapter.clearCacheForTests();
    }

    public void testNullStorageObjectThrows() {
        expectThrows(QlIllegalArgumentException.class, () -> new OrcStorageObjectAdapter(null));
    }

    public void testGetFileStatusReturnsCorrectLength() throws IOException {
        byte[] data = new byte[1024];
        StorageObject storageObject = createStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        FileStatus status = adapter.getFileStatus(new Path("memory://test.orc"));
        assertEquals(1024L, status.getLen());
        assertFalse(status.isDirectory());
    }

    public void testOpenReturnsReadableStream() throws IOException {
        byte[] data = new byte[] { 1, 2, 3, 4, 5 };
        StorageObject storageObject = createStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            assertNotNull(stream);
            assertEquals(1, stream.read());
            assertEquals(2, stream.read());
        }
    }

    public void testStreamSeekForward() throws IOException {
        byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        StorageObject storageObject = createStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            stream.seek(3);
            assertEquals(3, stream.getPos());
            assertEquals(40, stream.read());
        }
    }

    public void testStreamSeekBackward() throws IOException {
        byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        StorageObject storageObject = createRangeReadStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            stream.read();
            stream.read();
            stream.read();
            assertEquals(3, stream.getPos());
            stream.seek(1);
            assertEquals(1, stream.getPos());
            assertEquals(20, stream.read());
        }
    }

    public void testStreamSeekNegativeThrows() throws IOException {
        byte[] data = new byte[] { 1, 2, 3 };
        StorageObject storageObject = createStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            expectThrows(IOException.class, () -> stream.seek(-1));
        }
    }

    public void testStreamSeekBeyondEndThrows() throws IOException {
        byte[] data = new byte[] { 1, 2, 3 };
        StorageObject storageObject = createStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            expectThrows(IOException.class, () -> stream.seek(100));
        }
    }

    public void testStreamReadArray() throws IOException {
        byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        StorageObject storageObject = createStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            byte[] buf = new byte[3];
            int read = stream.read(buf, 0, 3);
            assertEquals(3, read);
            assertEquals(10, buf[0]);
            assertEquals(20, buf[1]);
            assertEquals(30, buf[2]);
        }
    }

    public void testPositionedRead() throws IOException {
        byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        StorageObject storageObject = createRangeReadStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            byte[] buf = new byte[2];
            int read = stream.read(2, buf, 0, 2);
            assertEquals(2, read);
            assertEquals(30, buf[0]);
            assertEquals(40, buf[1]);
            // Position should not change after positioned read
            assertEquals(0, stream.getPos());
        }
    }

    public void testReadFullyPositioned() throws IOException {
        byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        StorageObject storageObject = createRangeReadStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            byte[] buf = new byte[3];
            stream.readFully(1, buf);
            assertEquals(20, buf[0]);
            assertEquals(30, buf[1]);
            assertEquals(40, buf[2]);
        }
    }

    public void testSkip() throws IOException {
        byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        StorageObject storageObject = createStorageObject(data);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(storageObject);

        try (FSDataInputStream stream = adapter.open(new Path("memory://test.orc"))) {
            long skipped = stream.skip(2);
            assertEquals(2, skipped);
            assertEquals(2, stream.getPos());
            assertEquals(30, stream.read());
        }
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://test.orc");
            }
        };
    }

    /**
     * Verifies that two readFully calls for the same tail region result in only one actual
     * storage read, with the second served from FooterByteCache.
     */
    public void testTailCacheCoalescesRepeatedReadFully() throws IOException {
        byte[] data = new byte[4096];
        random().nextBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();

        StorageObject countingStorage = createCountingRangeReadStorageObject(data, rangeReadCount);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(countingStorage);

        int tailLen = 512;
        long tailPos = data.length - tailLen;

        byte[] first = new byte[tailLen];
        try (FSDataInputStream stream = adapter.open(new Path("memory://cache-test.orc"))) {
            stream.readFully(tailPos, first);
        }
        assertEquals(1, rangeReadCount.get());

        byte[] second = new byte[tailLen];
        try (FSDataInputStream stream = adapter.open(new Path("memory://cache-test.orc"))) {
            stream.readFully(tailPos, second);
        }
        assertEquals("Second tail read should be served from cache", 1, rangeReadCount.get());
        assertArrayEquals(first, second);

        byte[] expected = new byte[tailLen];
        System.arraycopy(data, (int) tailPos, expected, 0, tailLen);
        assertArrayEquals(expected, first);
    }

    /**
     * Verifies that positioned reads that hit the cache still return correct bytes.
     */
    public void testPositionedReadFromTailCache() throws IOException {
        byte[] data = new byte[2048];
        random().nextBytes(data);
        AtomicInteger rangeReadCount = new AtomicInteger();

        StorageObject countingStorage = createCountingRangeReadStorageObject(data, rangeReadCount);
        OrcStorageObjectAdapter adapter = new OrcStorageObjectAdapter(countingStorage);

        int tailLen = 1024;
        long tailPos = data.length - tailLen;

        byte[] fullTail = new byte[tailLen];
        try (FSDataInputStream stream = adapter.open(new Path("memory://pos-test.orc"))) {
            stream.readFully(tailPos, fullTail);
        }
        assertEquals(1, rangeReadCount.get());

        byte[] subRange = new byte[256];
        long subPos = data.length - 256;
        try (FSDataInputStream stream = adapter.open(new Path("memory://pos-test.orc"))) {
            int read = stream.read(subPos, subRange, 0, 256);
            assertEquals(256, read);
        }
        assertEquals("Sub-range read within cached tail should not cause extra storage reads", 1, rangeReadCount.get());

        byte[] expected = new byte[256];
        System.arraycopy(data, (int) subPos, expected, 0, 256);
        assertArrayEquals(expected, subRange);
    }

    private StorageObject createRangeReadStorageObject(byte[] data) {
        return createCountingRangeReadStorageObject(data, new AtomicInteger());
    }

    private StorageObject createCountingRangeReadStorageObject(byte[] data, AtomicInteger rangeReadCount) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                rangeReadCount.incrementAndGet();
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
                return StoragePath.of("memory://test.orc");
            }
        };
    }
}
