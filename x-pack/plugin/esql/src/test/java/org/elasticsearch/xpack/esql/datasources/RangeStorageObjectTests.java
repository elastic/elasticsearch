/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RangeStorageObjectTests extends ESTestCase {

    // Hold a strong reference to the BlockFactory so the JVM Cleaner does not close the
    // arrow root allocator mid-test (BlockFactory.arrowAllocator() registers a cleaner action
    // on its own BlockFactory instance, which is otherwise unreachable from ALLOCATOR alone).
    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();
    private static final BufferAllocator ALLOCATOR = BLOCK_FACTORY.arrowAllocator();
    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forAllocator(ALLOCATOR);

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

    public void testNewStreamReadToEndReadsToViewEnd() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6); // view = "World!"
        // READ_TO_END within the view stops at the view's end (offset+length), not the underlying object's end.
        try (InputStream stream = range.newStream(2, StorageObject.READ_TO_END)) {
            byte[] result = stream.readAllBytes();
            assertEquals("rld!", new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testNewStreamReadToEndAtViewEndIsEmpty() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);
        // position at the view's end: nothing left -> an empty stream, never a negative length to the delegate.
        try (InputStream stream = range.newStream(6, StorageObject.READ_TO_END)) {
            assertEquals(-1, stream.read());
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

    public void testReadBytesNonZeroPosition() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        ByteBuffer buf = ByteBuffer.allocate(4);
        int bytesRead = range.readBytes(2, buf);
        assertEquals(4, bytesRead);
        buf.flip();
        byte[] result = new byte[4];
        buf.get(result);
        assertEquals("rld!", new String(result, StandardCharsets.UTF_8));
    }

    public void testReadBytesBufferLargerThanRemaining() throws IOException {
        // The view is "World!" (6 bytes) with real bytes (" This is test data…") right after it in FILE_BYTES,
        // so an oversized buffer would overrun the view if the read were not capped.
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        ByteBuffer buf = ByteBuffer.allocate(20);
        int bytesRead = range.readBytes(0, buf);
        assertEquals("read must be capped to the view length, not overrun into the bytes after the view", 6, bytesRead);
        assertEquals("the caller's buffer limit must be restored after the capped read", 20, buf.limit());
        buf.flip();
        byte[] result = new byte[bytesRead];
        buf.get(result);
        assertEquals("only the view's bytes are delivered", "World!", new String(result, StandardCharsets.UTF_8));
    }

    public void testNewStreamClosedRangeLargerThanViewIsCappedToView() throws IOException {
        // A closed-range newStream asking for more than the view holds must stop at the view's end, not read the
        // bytes that follow the view in the underlying object.
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6); // view = "World!"

        byte[] read;
        try (InputStream stream = range.newStream(0, 20)) {
            read = stream.readAllBytes();
        }
        assertEquals("closed-range read must be capped to the view", "World!", new String(read, StandardCharsets.UTF_8));
    }

    public void testReadBytesBufferExactSize() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        ByteBuffer buf = ByteBuffer.allocate(3);
        int bytesRead = range.readBytes(0, buf);
        assertEquals(3, bytesRead);
        buf.flip();
        byte[] result = new byte[3];
        buf.get(result);
        assertEquals("Wor", new String(result, StandardCharsets.UTF_8));
    }

    public void testReadBytesPartialReadFromMiddle() throws IOException {
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        ByteBuffer buf = ByteBuffer.allocate(2);
        int bytesRead = range.readBytes(4, buf);
        assertEquals(2, bytesRead);
        buf.flip();
        byte[] result = new byte[2];
        buf.get(result);
        assertEquals("d!", new String(result, StandardCharsets.UTF_8));
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

    public void testSupportsNativeAsyncDelegatesToDelegate() {
        StorageObject syncDelegate = new InMemoryStorageObject(FILE_BYTES);
        RangeStorageObject syncRange = new RangeStorageObject(syncDelegate, 0, 10);
        assertFalse(syncRange.supportsNativeAsync());

        StorageObject asyncDelegate = new AsyncCapableStorageObject(FILE_BYTES);
        RangeStorageObject asyncRange = new RangeStorageObject(asyncDelegate, 0, 10);
        assertTrue(asyncRange.supportsNativeAsync());
    }

    public void testReadBytesAsyncAdjustsPosition() throws Exception {
        StorageObject delegate = new AsyncCapableStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
        Executor directExecutor = Runnable::run;

        range.readBytesAsync(0, 6, FACTORY, directExecutor, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer byteBuffer) {
                result.set(byteBuffer);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        try (DirectReadBuffer drb = result.get()) {
            byte[] bytes = new byte[drb.buffer().remaining()];
            drb.buffer().get(bytes);
            assertEquals("World!", new String(bytes, StandardCharsets.UTF_8));
        }
    }

    public void testReadBytesAsyncWithTargetBufferAdjustsPosition() throws Exception {
        StorageObject delegate = new AsyncCapableStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Integer> bytesRead = new AtomicReference<>();
        ByteBuffer target = ByteBuffer.allocate(6);
        Executor directExecutor = Runnable::run;

        range.readBytesAsync(0, target, directExecutor, new ActionListener<>() {
            @Override
            public void onResponse(Integer count) {
                bytesRead.set(count);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        target.flip();
        byte[] bytes = new byte[target.remaining()];
        target.get(bytes);
        assertEquals("World!", new String(bytes, StandardCharsets.UTF_8));
    }

    public void testMetricsDelegatesToWrapped() {
        StorageObjectMetrics snapshot = new StorageObjectMetrics(7, 1234, 4096, 2);
        StorageObject delegate = new InMemoryStorageObject(FILE_BYTES) {
            @Override
            public StorageObjectMetrics metrics() {
                return snapshot;
            }
        };
        RangeStorageObject range = new RangeStorageObject(delegate, 0, FILE_BYTES.length);
        assertSame(snapshot, range.metrics());
    }

    /**
     * Regression guard: {@code abortStream} must forward directly to the delegate, not fall
     * through to the SPI default (which is a draining {@code stream.close()} on providers like
     * S3). {@code RangeStorageObject} is a thin view over its delegate's stream, so the abort
     * must reach the underlying provider unchanged.
     */
    public void testAbortStreamDelegates() throws IOException {
        AbortTrackingStorageObject delegate = new AbortTrackingStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        InputStream stream = range.newStream();
        range.abortStream(stream);

        assertSame("delegate must receive the exact stream instance", stream, delegate.lastAborted);
        assertEquals("delegate.abortStream must be invoked exactly once", 1, delegate.abortCount);
    }

    public void testReadBytesAsyncPastRangeReturnsMinusOne() throws Exception {
        StorageObject delegate = new AsyncCapableStorageObject(FILE_BYTES);
        RangeStorageObject range = new RangeStorageObject(delegate, 7, 6);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Integer> bytesRead = new AtomicReference<>();
        ByteBuffer target = ByteBuffer.allocate(10);
        Executor directExecutor = Runnable::run;

        range.readBytesAsync(6, target, directExecutor, new ActionListener<>() {
            @Override
            public void onResponse(Integer count) {
                bytesRead.set(count);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(-1), bytesRead.get());
    }

    /**
     * StorageObject that records {@code abortStream} invocations so a wrapper test can
     * confirm the call reached the underlying delegate.
     */
    private static class AbortTrackingStorageObject extends InMemoryStorageObject {
        int abortCount;
        InputStream lastAborted;

        AbortTrackingStorageObject(byte[] data) {
            super(data);
        }

        @Override
        public void abortStream(InputStream stream) {
            abortCount++;
            lastAborted = stream;
        }
    }

    /**
     * StorageObject that reports native async support for testing delegation.
     */
    private static class AsyncCapableStorageObject extends InMemoryStorageObject {
        AsyncCapableStorageObject(byte[] data) {
            super(data);
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }
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
