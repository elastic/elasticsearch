/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.rest.ParquetSpecTestCase.StorageBackend;

import java.lang.reflect.Method;

/**
 * Unit tests for ParquetSpecTestCase path transformation logic.
 */
public class ParquetSpecTestCaseTests extends ESTestCase {

    /**
     * Test that S3 paths remain unchanged for S3 backend.
     */
    public void testS3BackendKeepsPathUnchanged() throws Exception {
        String originalPath = "s3://iceberg-test/warehouse/standalone/employees.parquet";
        String transformedPath = invokeTransformPathForStorageBackend(originalPath, StorageBackend.S3);
        assertEquals(originalPath, transformedPath);
    }

    /**
     * Test that S3 paths are transformed to HTTPS for HTTPS backend.
     */
    public void testHttpsBackendTransformsPath() throws Exception {
        String originalPath = "s3://iceberg-test/warehouse/standalone/employees.parquet";
        String transformedPath = invokeTransformPathForStorageBackend(originalPath, StorageBackend.HTTPS);
        assertEquals("https://iceberg-test/warehouse/standalone/employees.parquet", transformedPath);
    }

    /**
     * Test that S3 paths are transformed to local file paths for LOCAL backend.
     */
    public void testLocalBackendTransformsPath() throws Exception {
        String originalPath = "s3://iceberg-test/warehouse/standalone/employees.parquet";
        String transformedPath = invokeTransformPathForStorageBackend(originalPath, StorageBackend.LOCAL);
        assertEquals("file:///tmp/parquet-fixtures/iceberg-test/warehouse/standalone/employees.parquet", transformedPath);
    }

    /**
     * Test extractObjectKey helper method.
     */
    public void testExtractObjectKey() throws Exception {
        assertEquals("bucket/path/to/file.parquet", invokeExtractObjectKey("s3://bucket/path/to/file.parquet"));
        assertEquals("bucket/path/to/file.parquet", invokeExtractObjectKey("https://bucket/path/to/file.parquet"));
        assertEquals("tmp/path/to/file.parquet", invokeExtractObjectKey("file:///tmp/path/to/file.parquet"));
    }

    /**
     * Helper method to invoke the private transformPathForStorageBackend method via reflection.
     */
    private String invokeTransformPathForStorageBackend(String path, StorageBackend backend) throws Exception {
        Method method = ParquetSpecTestCase.class.getDeclaredMethod("transformPathForStorageBackend", String.class, StorageBackend.class);
        method.setAccessible(true);
        return (String) method.invoke(null, path, backend);
    }

    /**
     * Helper method to invoke the private extractObjectKey method via reflection.
     */
    private String invokeExtractObjectKey(String path) throws Exception {
        Method method = ParquetSpecTestCase.class.getDeclaredMethod("extractObjectKey", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, path);
    }
}
