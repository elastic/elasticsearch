/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.cloud.storage.Storage;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for GcsStorageProvider.
 * Tests scheme validation, path extraction, and supported schemes.
 */
public class GcsStorageProviderTests extends ESTestCase {

    private final Storage mockStorage = mock(Storage.class);

    public void testSupportedSchemes() {
        GcsStorageProvider provider = new GcsStorageProvider(mockStorage);
        assertEquals(List.of("gs"), provider.supportedSchemes());
    }

    public void testInvalidSchemeThrows() {
        GcsStorageProvider provider = new GcsStorageProvider(mockStorage);
        StoragePath s3Path = StoragePath.of("s3://my-bucket/path/to/file.parquet");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> provider.newObject(s3Path));
        assertTrue(e.getMessage().contains("GcsStorageProvider only supports gs:// scheme"));
    }

    public void testNewObjectWithValidGsPath() {
        GcsStorageProvider provider = new GcsStorageProvider(mockStorage);
        StoragePath path = StoragePath.of("gs://my-bucket/data/file.parquet");
        var obj = provider.newObject(path);
        assertNotNull(obj);
        assertEquals(path, obj.path());
    }

    public void testGsPathParsing() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/sales.parquet");
        assertEquals("gs", path.scheme());
        assertEquals("my-bucket", path.host());
        assertEquals("/data/sales.parquet", path.path());
        assertEquals("sales.parquet", path.objectName());
    }

    public void testGsPathWithNestedDirectory() {
        StoragePath path = StoragePath.of("gs://my-bucket/warehouse/db/table/part-00000.parquet");
        assertEquals("gs", path.scheme());
        assertEquals("my-bucket", path.host());
        assertEquals("/warehouse/db/table/part-00000.parquet", path.path());
        assertEquals("part-00000.parquet", path.objectName());
    }

    public void testGsPathBucketOnly() {
        StoragePath path = StoragePath.of("gs://my-bucket/");
        assertEquals("gs", path.scheme());
        assertEquals("my-bucket", path.host());
        assertEquals("/", path.path());
    }

    public void testGsPathWithGlobPattern() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/*.parquet");
        assertEquals("gs", path.scheme());
        assertTrue(path.isPattern());
        assertEquals("*.parquet", path.globPart());
    }

    public void testGsPathPatternPrefix() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/2024/*.parquet");
        StoragePath prefix = path.patternPrefix();
        assertEquals("gs://my-bucket/data/2024/", prefix.toString());
    }

    public void testGsPathParentDirectory() {
        StoragePath path = StoragePath.of("gs://my-bucket/data/sales.parquet");
        StoragePath parent = path.parentDirectory();
        assertNotNull(parent);
        assertEquals("gs://my-bucket/data", parent.toString());
    }

    public void testGsPathAppendPath() {
        StoragePath base = StoragePath.of("gs://my-bucket/data");
        StoragePath appended = base.appendPath("sales.parquet");
        assertEquals("gs://my-bucket/data/sales.parquet", appended.toString());
    }
}
