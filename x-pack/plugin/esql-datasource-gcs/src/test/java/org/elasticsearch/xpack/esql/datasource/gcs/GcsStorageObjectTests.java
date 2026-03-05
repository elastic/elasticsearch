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

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for GcsStorageObject.
 * Tests constructor validation, accessor methods, cached metadata behavior,
 * stream operations, and error handling.
 */
public class GcsStorageObjectTests extends ESTestCase {

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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> obj.newStream(0, -1));
        assertEquals("length must be non-negative, got: -1", e.getMessage());
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
        when(mockStorage.reader(any(BlobId.class))).thenThrow(new StorageException(500, "Internal Error"));

        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        GcsStorageObject obj = new GcsStorageObject(mockStorage, "my-bucket", "data/file.parquet", path);

        IOException e = expectThrows(IOException.class, obj::newStream);
        assertTrue(e.getMessage().contains("Failed to read object from"));
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
}
