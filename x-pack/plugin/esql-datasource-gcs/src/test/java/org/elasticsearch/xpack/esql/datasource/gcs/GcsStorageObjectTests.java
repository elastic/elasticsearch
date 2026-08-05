/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for GcsStorageObject.
 * Tests constructor validation, accessor methods, cached metadata behavior,
 * stream operations, readBytes, readBytesAsync, and error handling.
 */
public class GcsStorageObjectTests extends ESTestCase {

    // Hold a strong reference to the BlockFactory so the JVM Cleaner does not close the
    // arrow root allocator mid-test (BlockFactory.arrowAllocator() registers a cleaner action
    // on its own BlockFactory instance, which is otherwise unreachable from ALLOCATOR alone).
    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();
    private static final BufferAllocator ALLOCATOR = BLOCK_FACTORY.arrowAllocator();
    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forAllocator(ALLOCATOR);

    private final Storage mockStorage = mock(Storage.class);

    public void testConstructorNullStorageThrows() {
        StoragePath path = StoragePath.of("gs://bucket/key");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GcsStorageObject(null, "bucket", "key", path));
        assertEquals("storage cannot be null", e.getMessage());
    }

    public void testConstructorNullBucketThrows() {
        StoragePath path = StoragePath.of("gs://bucket/key");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GcsStorageObject(mockStorage, null, "key", path)
        );
        assertEquals("bucket cannot be null or empty", e.getMessage());
    }

    public void testConstructorEmptyBucketThrows() {
        StoragePath path = StoragePath.of("gs://bucket/key");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GcsStorageObject(mockStorage, "", "key", path));
        assertEquals("bucket cannot be null or empty", e.getMessage());
    }

    public void testConstructorNullObjectNameThrows() {
        StoragePath path = StoragePath.of("gs://bucket/key");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GcsStorageObject(mockStorage, "bucket", null, path)
        );
        assertEquals("objectName cannot be null", e.getMessage());
    }

    public void testConstructorNullPathThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GcsStorageObject(mockStorage, "bucket", "key", null)
        );
        assertEquals("path cannot be null", e.getMessage());
    }

    public void testPathReturnsConstructorPath() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);
        assertEquals(path, obj.path());
    }

    public void testBucketAndObjectNameAccessors() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);
        assertEquals("my-bucket", obj.bucket());
        assertEquals("data/file.parquet", obj.objectName());
    }

    public void testConstructorWithLength() throws IOException {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path, 1024L);
        assertEquals(1024L, obj.length());
    }

    public void testConstructorWithLengthAndLastModified() throws IOException {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        Instant now = Instant.now();
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path, 1024L, now);
        assertEquals(1024L, obj.length());
        assertEquals(now, obj.lastModified());
    }

    public void testToString() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);
        String str = obj.toString();
        assertTrue(str.contains("my-bucket"));
        assertTrue(str.contains("data/file.parquet"));
    }

    public void testNewStreamWithNegativePositionThrows() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> obj.newStream(-1, 100));
        assertEquals("position must be non-negative, got: -1", e.getMessage());
    }

    public void testNewStreamWithNegativeLengthThrows() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);
        // -1 is now the READ_TO_END open-ended sentinel; a different non-positive length is still invalid.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> obj.newStream(0, -2));
        assertEquals("length must be positive or READ_TO_END, got: -2", e.getMessage());
    }

    public void testNewStreamOpensReader() throws IOException {
        ReadChannel mockReader = mock(ReadChannel.class);
        when(mockStorage.reader(any(BlobId.class))).thenReturn(mockReader);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        try (InputStream stream = obj.newStream()) {
            assertNotNull(stream);
        }
        verify(mockStorage).reader(BlobId.of("my-bucket", "data/file.parquet"));
    }

    public void testNewStreamRangeOpensReaderWithSeekAndLimit() throws IOException {
        ReadChannel mockReader = mock(ReadChannel.class);
        when(mockStorage.reader(any(BlobId.class))).thenReturn(mockReader);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        try (InputStream stream = obj.newStream(10, 50)) {
            assertNotNull(stream);
        }
        verify(mockReader).seek(10);
        verify(mockReader).limit(60);
    }

    public void testNewStreamWraps404AsIOException() {
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(404, "Not Found"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        IOException e = expectThrows(IOException.class, obj::newStream);
        assertTrue(e.getMessage().contains("Object not found"));
    }

    public void testNewStreamWrapsOtherStorageExceptionAsIOException() {
        // A non-retryable, non-404 status (here 412) is a client-class failure: wrapped as IOException
        // (which the external source operator maps to 400).
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(412, "Precondition Failed"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        IOException e = expectThrows(IOException.class, obj::newStream);
        assertTrue(e.getMessage().contains("Failed to read object from"));
    }

    public void testNewStreamClassifies503AsThrottling() {
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(503, "Service Unavailable"));
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "k", StoragePath.of("gs://my-bucket/k"));
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertTrue("503 is throttling", e.throttling());
    }

    public void testNewStreamMapsRetryableStorageExceptionToUnavailable() {
        // A retryable transport status (here 503) becomes ExternalUnavailableException (mapped to 503).
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(503, "Service Unavailable"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertTrue(e.getMessage().contains("GCS store unavailable"));
    }

    public void testNewStreamClassifies429AsThrottling() {
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(429, "Too Many Requests"));
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "k", StoragePath.of("gs://my-bucket/k"));
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertTrue("429 is throttling", e.throttling());
    }

    public void testNewStreamClassifies500AsTransientNotThrottling() {
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(500, "Internal Error"));
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "k", StoragePath.of("gs://my-bucket/k"));
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertFalse("500 is transient but not throttling", e.throttling());
    }

    public void testNewStreamClassifies403AsNonTransient() {
        // 403 is not a retryable status, so it stays a plain IOException (the external source operator maps it to a
        // client-class 400) rather than the retryable ExternalUnavailableException. expectThrows(IOException.class)
        // already proves it is not ExternalUnavailableException — the unavailable type is not an IOException subtype.
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(403, "Forbidden"));
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "k", StoragePath.of("gs://my-bucket/k"));
        IOException e = expectThrows(IOException.class, obj::newStream);
        assertTrue("403 wraps as a plain read failure", e.getMessage().contains("Failed to read object from"));
    }

    // The GCS ReadChannel never lets a StorageException escape the stream: BaseStorageReadChannel.read catches it
    // and rethrows it wrapped in a plain IOException (its cause). These tests feed that real wrapped shape, not a
    // raw StorageException, so they actually exercise the path a live read produces. The wrapper re-types the
    // IOException as an (unchecked) ExternalUnavailableException, reading the throttle flag off the StorageException
    // cause when present.
    public void testMidReadStorageExceptionRetypedAsThrottling() throws IOException {
        InputStream faulting = new InputStream() {
            private int n = 0;

            @Override
            public int read() throws IOException {
                if (n++ < 1) {
                    return 'x';
                }
                throw new IOException(new StorageException(503, "mid-read drop"));
            }
        };
        GcsTransientTypingInputStream wrapped = new GcsTransientTypingInputStream(faulting, StoragePath.of("gs://b/k"));
        assertEquals('x', wrapped.read());
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertTrue("a 503 surfaced mid-read is throttling", e.throttling());
    }

    public void testMidReadNon503StorageExceptionIsTransientNotThrottling() {
        InputStream faulting = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException(new StorageException(500, "mid-read error"));
            }
        };
        GcsTransientTypingInputStream wrapped = new GcsTransientTypingInputStream(faulting, StoragePath.of("gs://b/k"));
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertFalse("500 mid-read is transient but not throttling", e.throttling());
    }

    public void testMidReadPlainTransportIOExceptionIsTransientNotThrottling() {
        // A transport drop with no StorageException cause (a bare IOException) is still re-typed transient so the
        // resume engages; it is just not throttling.
        InputStream faulting = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("connection reset");
            }
        };
        GcsTransientTypingInputStream wrapped = new GcsTransientTypingInputStream(faulting, StoragePath.of("gs://b/k"));
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertFalse("a plain transport IOException is transient but not throttling", e.throttling());
    }

    public void testLengthFetchesMetadataOnce() throws IOException {
        Blob mockBlob = mock(Blob.class);
        when(mockBlob.getSize()).thenReturn(2048L);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);
        when(mockStorage.get(any(BlobId.class))).thenReturn(mockBlob);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        assertEquals(2048L, obj.length());
        assertEquals(2048L, obj.length());
        verify(mockStorage, times(1)).get(any(BlobId.class));
    }

    public void testExistsReturnsTrueForExistingBlob() throws IOException {
        Blob mockBlob = mock(Blob.class);
        when(mockBlob.getSize()).thenReturn(100L);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);
        when(mockStorage.get(any(BlobId.class))).thenReturn(mockBlob);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        assertTrue(obj.exists());
    }

    public void testExistsReturnsFalseForNullBlob() throws IOException {
        when(mockStorage.get(any(BlobId.class))).thenReturn(null);

        StoragePath path = StoragePath.of("gs://my-bucket/data/missing.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/missing.parquet", path);

        assertFalse(obj.exists());
    }

    public void testExistsReturnsFalseOn404() throws IOException {
        when(mockStorage.get(any(BlobId.class))).thenThrow(new StorageException(404, "Not Found"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/missing.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/missing.parquet", path);

        assertFalse(obj.exists());
    }

    public void testLengthThrowsForNonExistentObject() {
        when(mockStorage.get(any(BlobId.class))).thenReturn(null);

        StoragePath path = StoragePath.of("gs://my-bucket/data/missing.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/missing.parquet", path);

        IOException e = expectThrows(IOException.class, obj::length);
        assertTrue(e.getMessage().contains("Object not found"));
    }

    public void testLastModifiedFetchesMetadata() throws IOException {
        Blob mockBlob = mock(Blob.class);
        OffsetDateTime odt = OffsetDateTime.of(2025, 6, 15, 10, 30, 0, 0, ZoneOffset.UTC);
        when(mockBlob.getSize()).thenReturn(100L);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(odt);
        when(mockStorage.get(any(BlobId.class))).thenReturn(mockBlob);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        assertEquals(odt.toInstant(), obj.lastModified());
    }

    public void testLastModifiedReturnsNullWhenNotAvailable() throws IOException {
        Blob mockBlob = mock(Blob.class);
        when(mockBlob.getSize()).thenReturn(100L);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(null);
        when(mockStorage.get(any(BlobId.class))).thenReturn(mockBlob);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        assertNull(obj.lastModified());
    }

    public void testFetchMetadataThrowsOnNon404Error() {
        when(mockStorage.get(any(BlobId.class))).thenThrow(new StorageException(500, "Internal Error"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        IOException e = expectThrows(IOException.class, obj::length);
        assertTrue(e.getMessage().contains("Failed to get metadata for"));
    }

    public void testPreknownLengthSkipsMetadataFetch() throws IOException {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path, 4096L);

        assertEquals(4096L, obj.length());
        verify(mockStorage, times(0)).get(any(BlobId.class));
    }

    // === readBytes tests ===

    public void testReadBytesUsesReadChannel() throws IOException {
        ReadChannel mockReader = mock(ReadChannel.class);
        when(mockStorage.reader(any(BlobId.class))).thenReturn(mockReader);
        doAnswer(invocation -> {
            ByteBuffer buf = invocation.getArgument(0);
            byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
            buf.put(data);
            return data.length;
        }).when(mockReader).read(any(ByteBuffer.class));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        ByteBuffer target = ByteBuffer.allocate(5);
        int bytesRead = obj.readBytes(100, target);

        assertEquals(5, bytesRead);
        assertEquals(0, target.remaining());
        verify(mockReader).seek(100);
        verify(mockReader).limit(105);
        verify(mockReader).close();
    }

    public void testReadBytesEmptyBufferReturnsZero() throws IOException {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        ByteBuffer target = ByteBuffer.allocate(0);
        int result = obj.readBytes(0, target);
        assertEquals(0, result);
    }

    public void testReadBytesReturnsMinusOneAtEof() throws IOException {
        ReadChannel mockReader = mock(ReadChannel.class);
        when(mockStorage.reader(any(BlobId.class))).thenReturn(mockReader);
        when(mockReader.read(any(ByteBuffer.class))).thenReturn(-1);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        ByteBuffer target = ByteBuffer.allocate(10);
        int result = obj.readBytes(9999, target);
        assertEquals(-1, result);
        verify(mockReader).close();
    }

    public void testReadBytesWraps404AsIOException() {
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(404, "Not Found"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        ByteBuffer target = ByteBuffer.allocate(10);
        IOException e = expectThrows(IOException.class, () -> obj.readBytes(0, target));
        assertTrue(e.getMessage().contains("Object not found"));
    }

    public void testReadBytesWrapsOtherStorageExceptionAsIOException() {
        // A non-retryable, non-404 status (here 412) is a client-class failure: wrapped as IOException.
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(412, "Precondition Failed"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        ByteBuffer target = ByteBuffer.allocate(10);
        IOException e = expectThrows(IOException.class, () -> obj.readBytes(0, target));
        assertTrue(e.getMessage().contains("Failed to read bytes from"));
    }

    public void testReadBytesMapsRetryableStorageExceptionToUnavailable() {
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(503, "Service Unavailable"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        ByteBuffer target = ByteBuffer.allocate(10);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, () -> obj.readBytes(0, target));
        assertTrue(e.getMessage().contains("GCS store unavailable"));
    }

    // === readBytesAsync tests ===

    public void testReadBytesAsyncReturnsData() throws Exception {
        ReadChannel mockReader = mock(ReadChannel.class);
        when(mockStorage.reader(any(BlobId.class))).thenReturn(mockReader);
        doAnswer(invocation -> {
            ByteBuffer buf = invocation.getArgument(0);
            byte[] data = "async".getBytes(StandardCharsets.UTF_8);
            buf.put(data);
            return data.length;
        }).when(mockReader).read(any(ByteBuffer.class));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(10, 5, FACTORY, Runnable::run, ActionListener.wrap(buf -> {
            result.set(buf);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(error.get());
        assertNotNull(result.get());
        try (DirectReadBuffer drb = result.get()) {
            assertTrue("readBytesAsync must return a direct ByteBuffer", drb.buffer().isDirect());
            assertEquals(5, drb.buffer().remaining());
        }
        verify(mockReader).seek(10);
        verify(mockReader).limit(15);
    }

    public void testReadBytesAsyncNegativePositionFails() throws Exception {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(-1, 10, FACTORY, Runnable::run, ActionListener.wrap(buf -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get() instanceof IllegalArgumentException);
        assertTrue(error.get().getMessage().contains("position must be non-negative"));
    }

    public void testReadBytesAsyncNegativeLengthFails() throws Exception {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(0, -1, FACTORY, Runnable::run, ActionListener.wrap(buf -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get() instanceof IllegalArgumentException);
        assertTrue(error.get().getMessage().contains("length must be non-negative"));
    }

    public void testReadBytesAsync404Fails() throws Exception {
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(404, "Not Found"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(0, 10, FACTORY, Runnable::run, ActionListener.wrap(buf -> latch.countDown(), e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(error.get());
        assertTrue(error.get() instanceof IOException);
        assertTrue(error.get().getMessage().contains("Object not found"));
    }

    public void testSupportsNativeAsyncReturnsTrue() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);
        assertTrue(obj.supportsNativeAsync());
    }

    /**
     * newStream(pos, length) increments {@link StorageObjectMetrics} request counters and records
     * the requested byte count. GCS ReadChannel does not expose content length on open, so the
     * implementation records the requested range length as bytesRead.
     */
    public void testRangeNewStreamIncrementsMetrics() throws IOException {
        long rangeBytes = 1024L;
        ReadChannel mockReader = mock(ReadChannel.class);
        when(mockStorage.reader(any(BlobId.class))).thenReturn(mockReader);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path, 100_000L);

        assertEquals(0L, obj.metrics().requestCount());
        obj.newStream(0, rangeBytes).close();

        StorageObjectMetrics metrics = obj.metrics();
        assertEquals(1L, metrics.requestCount());
        assertTrue("bytesRead should be >= 0", metrics.bytesRead() >= 0);
        assertEquals(rangeBytes, metrics.bytesRead());
        assertTrue("requestNanos should be > 0", metrics.requestNanos() > 0);
        assertEquals(0L, metrics.retryCount());
    }

    /**
     * Metadata-probe paths (length(), exists(), lastModified()) are intentionally NOT counted
     * in metrics() — they're not data reads.
     */
    public void testMetadataProbesDoNotCountAsRequests() throws IOException {
        Blob mockBlob = mock(Blob.class);
        when(mockBlob.getSize()).thenReturn(100L);
        OffsetDateTime odt = OffsetDateTime.of(2025, 6, 15, 10, 30, 0, 0, ZoneOffset.UTC);
        when(mockBlob.getUpdateTimeOffsetDateTime()).thenReturn(odt);
        when(mockStorage.get(any(BlobId.class))).thenReturn(mockBlob);

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        obj.length();
        obj.exists();
        obj.lastModified();

        assertEquals(0L, obj.metrics().requestCount());
        assertEquals(0L, obj.metrics().bytesRead());
    }
}
