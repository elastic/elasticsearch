/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
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

    // -- Hadoop/Spark canonical WASB(S) form: wasbs://<container>@<account>.host/<blob> --

    public void testParsePathHadoopForm() {
        StoragePath path = StoragePath.of("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2019/file.parquet");
        AzureStorageProvider.ParsedPath parsed = AzureStorageProvider.parsePath(path);
        assertEquals("azureopendatastorage.blob.core.windows.net", parsed.host());
        assertEquals("nyctlc", parsed.container());
        assertEquals("yellow/puYear=2019/file.parquet", parsed.blobName());
    }

    public void testParsePathHadoopFormRootListing() {
        // Hadoop form with empty path is a valid root-of-container reference for listing.
        StoragePath path = StoragePath.of("wasbs://my-container@account.blob.core.windows.net/");
        AzureStorageProvider.ParsedPath parsed = AzureStorageProvider.parsePath(path);
        assertEquals("my-container", parsed.container());
        assertEquals("", parsed.blobName());
    }

    public void testParsePathPathStyleUnchanged() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/my-container/data/file.parquet");
        AzureStorageProvider.ParsedPath parsed = AzureStorageProvider.parsePath(path);
        assertEquals("account.blob.core.windows.net", parsed.host());
        assertEquals("my-container", parsed.container());
        assertEquals("data/file.parquet", parsed.blobName());
    }

    public void testParsePathPathStyleEmptyRejected() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AzureStorageProvider.parsePath(path));
        assertTrue(e.getMessage().contains("container and blob name required"));
    }

    // -- AzureStorageIterator URI-form preservation: emitted entries match input form --

    public void testIteratorEmitsPathStyleWhenInputIsPathStyle() throws Exception {
        StoragePath base = StoragePath.of("wasbs://account.blob.core.windows.net/c/data/");
        List<StoragePath> emitted = drain(base, "c", "data/file1.parquet", "data/sub/file2.parquet");
        assertEquals(
            List.of(
                StoragePath.of("wasbs://account.blob.core.windows.net/c/data/file1.parquet"),
                StoragePath.of("wasbs://account.blob.core.windows.net/c/data/sub/file2.parquet")
            ),
            emitted
        );
        for (StoragePath p : emitted) {
            assertNull("path-style entries must not carry userInfo", p.userInfo());
        }
    }

    public void testIteratorEmitsHadoopFormWhenInputIsHadoopForm() throws Exception {
        StoragePath base = StoragePath.of("wasbs://c@account.blob.core.windows.net/data/");
        List<StoragePath> emitted = drain(base, "c", "data/file1.parquet", "data/sub/file2.parquet");
        assertEquals(
            List.of(
                StoragePath.of("wasbs://c@account.blob.core.windows.net/data/file1.parquet"),
                StoragePath.of("wasbs://c@account.blob.core.windows.net/data/sub/file2.parquet")
            ),
            emitted
        );
        for (StoragePath p : emitted) {
            assertEquals("Hadoop-form entries must carry container in userInfo", "c", p.userInfo());
        }
    }

    public void testIteratorSkipsPrefixesAndDirectoryEntries() throws Exception {
        BlobItem prefix = new BlobItem().setName("data/sub/").setIsPrefix(Boolean.TRUE);
        BlobItem dir = new BlobItem().setName("data/dir/").setProperties(properties(0L));
        BlobItem file = new BlobItem().setName("data/file.parquet").setProperties(properties(123L));

        StoragePath base = StoragePath.of("wasbs://c@account.blob.core.windows.net/data/");
        AzureStorageProvider.AzureStorageIterator it = new AzureStorageProvider.AzureStorageIterator(List.of(prefix, dir, file), base, "c");

        List<StorageEntry> entries = drainEntries(it);
        assertEquals(1, entries.size());
        assertEquals(StoragePath.of("wasbs://c@account.blob.core.windows.net/data/file.parquet"), entries.get(0).path());
        assertEquals(123L, entries.get(0).length());
    }

    private static List<StoragePath> drain(StoragePath base, String container, String... blobNames) throws Exception {
        List<BlobItem> items = new ArrayList<>(blobNames.length);
        for (String name : blobNames) {
            items.add(new BlobItem().setName(name).setProperties(properties(0L)));
        }
        AzureStorageProvider.AzureStorageIterator it = new AzureStorageProvider.AzureStorageIterator(items, base, container);
        List<StoragePath> paths = new ArrayList<>();
        for (StorageEntry entry : drainEntries(it)) {
            paths.add(entry.path());
        }
        return paths;
    }

    private static List<StorageEntry> drainEntries(StorageIterator it) throws Exception {
        List<StorageEntry> entries = new ArrayList<>();
        try (it) {
            while (it.hasNext()) {
                entries.add(it.next());
            }
        }
        return entries;
    }

    private static BlobItemProperties properties(long contentLength) {
        return new BlobItemProperties().setContentLength(contentLength);
    }
}
