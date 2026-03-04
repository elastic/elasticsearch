/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;

/**
 * Unit tests for AzureStorageProvider.
 * Tests scheme validation, path extraction, and supported schemes.
 * Note: Tests that require BlobServiceClient are excluded as it is a final class
 * and cannot be mocked with standard Mockito.
 */
public class AzureStorageProviderTests extends ESTestCase {

    public void testSupportedSchemes() {
        AzureStorageProvider provider = new AzureStorageProvider((AzureConfiguration) null);
        assertEquals(List.of("wasbs", "wasb"), provider.supportedSchemes());
    }

    public void testInvalidSchemeThrows() {
        AzureStorageProvider provider = new AzureStorageProvider((AzureConfiguration) null);
        StoragePath s3Path = StoragePath.of("s3://my-bucket/path/to/file.parquet");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> provider.newObject(s3Path));
        assertTrue(e.getMessage().contains("AzureStorageProvider only supports wasbs:// and wasb:// schemes"));
    }

    public void testWasbsPathParsing() {
        StoragePath path = StoragePath.of("wasbs://myaccount.blob.core.windows.net/container/data/sales.parquet");
        assertEquals("wasbs", path.scheme());
        assertEquals("myaccount.blob.core.windows.net", path.host());
        assertEquals("/container/data/sales.parquet", path.path());
        assertEquals("sales.parquet", path.objectName());
    }

    public void testWasbPathParsing() {
        StoragePath path = StoragePath.of("wasb://myaccount.blob.core.windows.net/container/data/file.parquet");
        assertEquals("wasb", path.scheme());
        assertEquals("myaccount.blob.core.windows.net", path.host());
        assertEquals("/container/data/file.parquet", path.path());
    }

    public void testPathWithNestedDirectory() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/warehouse/db/table/part-00000.parquet");
        assertEquals("wasbs", path.scheme());
        assertEquals("account.blob.core.windows.net", path.host());
        assertEquals("/warehouse/db/table/part-00000.parquet", path.path());
        assertEquals("part-00000.parquet", path.objectName());
    }

    public void testPathWithGlobPattern() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/container/data/*.parquet");
        assertEquals("wasbs", path.scheme());
        assertTrue(path.isPattern());
        assertEquals("*.parquet", path.globPart());
    }

    public void testPathPatternPrefix() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/container/data/2024/*.parquet");
        StoragePath prefix = path.patternPrefix();
        assertEquals("wasbs://account.blob.core.windows.net/container/data/2024/", prefix.toString());
    }
}
