/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalSourceCacheServiceTests extends ESTestCase {

    private static Settings defaultSettings() {
        return Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();
    }

    public void testSchemaHitMiss() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            AtomicInteger loaderCalls = new AtomicInteger();
            SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/data/file.parquet", 1000L, ".parquet", Map.of());

            SchemaCacheEntry entry1 = service.getOrComputeSchema(key, k -> {
                loaderCalls.incrementAndGet();
                return testSchemaEntry();
            });
            assertNotNull(entry1);
            assertEquals(1, loaderCalls.get());

            SchemaCacheEntry entry2 = service.getOrComputeSchema(key, k -> {
                loaderCalls.incrementAndGet();
                return testSchemaEntry();
            });
            assertSame(entry1, entry2);
            assertEquals(1, loaderCalls.get());
        }
    }

    public void testSchemaMtimeChangeInvalidates() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            AtomicInteger loaderCalls = new AtomicInteger();

            SchemaCacheKey key1 = SchemaCacheKey.build("s3://bucket/data/file.parquet", 1000L, ".parquet", Map.of());
            service.getOrComputeSchema(key1, k -> {
                loaderCalls.incrementAndGet();
                return testSchemaEntry();
            });
            assertEquals(1, loaderCalls.get());

            SchemaCacheKey key2 = SchemaCacheKey.build("s3://bucket/data/file.parquet", 2000L, ".parquet", Map.of());
            service.getOrComputeSchema(key2, k -> {
                loaderCalls.incrementAndGet();
                return testSchemaEntry();
            });
            assertEquals(2, loaderCalls.get());
        }
    }

    public void testListingHitMiss() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            AtomicInteger loaderCalls = new AtomicInteger();
            ListingCacheKey key = ListingCacheKey.build("s3", "bucket", "/data/*.parquet", Map.of());

            FileList listing1 = service.getOrComputeListing(key, k -> {
                loaderCalls.incrementAndGet();
                return testCompactFileList();
            });
            assertNotNull(listing1);
            assertEquals(1, loaderCalls.get());
            assertNotNull(listing1);
            assertEquals(10, listing1.fileCount());
            assertNull(listing1.partitionMetadata());

            FileList listing2 = service.getOrComputeListing(key, k -> {
                loaderCalls.incrementAndGet();
                return testCompactFileList();
            });
            assertSame(listing1, listing2);
            assertEquals(1, loaderCalls.get());
        }
    }

    public void testDifferentCredentialsSeparateListingEntries() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            AtomicInteger loaderCalls = new AtomicInteger();

            ListingCacheKey key1 = ListingCacheKey.build("s3", "bucket", "/data/*.parquet", Map.of("access_key", "userA"));
            ListingCacheKey key2 = ListingCacheKey.build("s3", "bucket", "/data/*.parquet", Map.of("access_key", "userB"));
            assertNotEquals(key1, key2);

            service.getOrComputeListing(key1, k -> {
                loaderCalls.incrementAndGet();
                return testCompactFileList();
            });
            service.getOrComputeListing(key2, k -> {
                loaderCalls.incrementAndGet();
                return testCompactFileList();
            });
            assertEquals(2, loaderCalls.get());
        }
    }

    public void testDifferentEndpointSeparateEntries() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            AtomicInteger loaderCalls = new AtomicInteger();

            ListingCacheKey key1 = ListingCacheKey.build("s3", "bucket", "/data/*.parquet", Map.of("endpoint", "us-east-1.amazonaws.com"));
            ListingCacheKey key2 = ListingCacheKey.build("s3", "bucket", "/data/*.parquet", Map.of("endpoint", "eu-west-1.amazonaws.com"));
            assertNotEquals(key1, key2);

            service.getOrComputeListing(key1, k -> {
                loaderCalls.incrementAndGet();
                return testCompactFileList();
            });
            service.getOrComputeListing(key2, k -> {
                loaderCalls.incrementAndGet();
                return testCompactFileList();
            });
            assertEquals(2, loaderCalls.get());
        }
    }

    public void testDisabledBypassesCache() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            service.setEnabled(false);
            AtomicInteger loaderCalls = new AtomicInteger();
            SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, ".parquet", Map.of());

            service.getOrComputeSchema(key, k -> {
                loaderCalls.incrementAndGet();
                return testSchemaEntry();
            });
            service.getOrComputeSchema(key, k -> {
                loaderCalls.incrementAndGet();
                return testSchemaEntry();
            });
            assertEquals(2, loaderCalls.get());
        }
    }

    public void testClearAllEmptiesBothCaches() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            SchemaCacheKey sKey = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, ".parquet", Map.of());
            service.getOrComputeSchema(sKey, k -> testSchemaEntry());

            ListingCacheKey lKey = ListingCacheKey.build("s3", "bucket", "/data/*.parquet", Map.of());
            service.getOrComputeListing(lKey, k -> testCompactFileList());

            Map<String, Object> stats = service.usageStats();
            assertEquals(1, stats.get("schema_cache.count"));
            assertEquals(1, stats.get("listing_cache.count"));

            service.clearAll();

            stats = service.usageStats();
            assertEquals(0, stats.get("schema_cache.count"));
            assertEquals(0, stats.get("listing_cache.count"));
        }
    }

    public void testToggleOffClearsEntries() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, ".parquet", Map.of());
            service.getOrComputeSchema(key, k -> testSchemaEntry());
            assertEquals(1, service.usageStats().get("schema_cache.count"));

            service.setEnabled(false);
            assertEquals(0, service.usageStats().get("schema_cache.count"));

            service.setEnabled(true);
            assertTrue(service.isEnabled());
        }
    }

    public void testLoaderExceptionPropagated() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, ".parquet", Map.of());

            expectThrows(Exception.class, () -> service.getOrComputeSchema(key, k -> { throw new RuntimeException("test error"); }));

            AtomicInteger calls = new AtomicInteger();
            service.getOrComputeSchema(key, k -> {
                calls.incrementAndGet();
                return testSchemaEntry();
            });
            assertEquals(1, calls.get());
        }
    }

    public void testComputeIfAbsentCoalescing() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            AtomicInteger loaderCalls = new AtomicInteger();
            SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, ".parquet", Map.of());

            int threadCount = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            ExecutorService exec = Executors.newFixedThreadPool(threadCount);

            for (int i = 0; i < threadCount; i++) {
                exec.submit(() -> {
                    try {
                        startLatch.await();
                        service.getOrComputeSchema(key, k -> {
                            loaderCalls.incrementAndGet();
                            Thread.sleep(50);
                            return testSchemaEntry();
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            startLatch.countDown();
            doneLatch.await();
            exec.shutdown();

            assertEquals(1, loaderCalls.get());
        }
    }

    public void testUsageStatsReportsCorrectly() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            Map<String, Object> stats = service.usageStats();
            assertEquals(true, stats.get("enabled"));
            assertEquals(0, stats.get("schema_cache.count"));
            assertEquals(0, stats.get("listing_cache.count"));

            SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, ".parquet", Map.of());
            service.getOrComputeSchema(key, k -> testSchemaEntry());
            service.getOrComputeSchema(key, k -> testSchemaEntry());

            stats = service.usageStats();
            assertEquals(1, stats.get("schema_cache.count"));
        }
    }

    public void testSchemaCacheEntryNameIdSafety() {
        SchemaCacheEntry entry = testSchemaEntry();
        List<Attribute> attrs1 = entry.toAttributes();
        List<Attribute> attrs2 = entry.toAttributes();
        assertEquals(attrs1.size(), attrs2.size());
        for (int i = 0; i < attrs1.size(); i++) {
            assertEquals(attrs1.get(i).name(), attrs2.get(i).name());
            assertEquals(attrs1.get(i).dataType(), attrs2.get(i).dataType());
            assertNotEquals("NameId must differ between calls to toAttributes()", attrs1.get(i).id(), attrs2.get(i).id());
        }
    }

    public void testListingCacheStoresHiveFileList() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            ListingCacheKey key = ListingCacheKey.build("s3", "bucket", "/data/*" + "*/*.parquet", Map.of());
            FileList listing = service.getOrComputeListing(key, k -> testCompactHiveFileList());
            assertNotNull(listing.partitionMetadata());
            assertFalse(listing.partitionMetadata().isEmpty());
            assertEquals(12, listing.fileCount());
        }
    }

    public void testDictionaryFileListRoundTrip() {
        FileList raw = testGenericFileList();
        FileList compact = GlobExpander.compact(raw, "s3://bucket/data/");
        assertNotNull(compact);
        assertEquals(raw.fileCount(), compact.fileCount());
        for (int i = 0; i < raw.fileCount(); i++) {
            assertEquals(raw.path(i), compact.path(i));
            assertEquals(raw.size(i), compact.size(i));
            assertEquals(raw.lastModifiedMillis(i), compact.lastModifiedMillis(i));
        }
        assertTrue(compact.estimatedBytes() > 0);
        assertTrue(compact.estimatedBytes() < raw.estimatedBytes());
    }

    public void testHiveFileListRoundTrip() {
        FileList raw = testHiveGenericFileList();
        FileList compact = GlobExpander.compact(raw, "s3://bucket/data/");
        assertNotNull(compact);
        assertEquals(raw.fileCount(), compact.fileCount());
        for (int i = 0; i < raw.fileCount(); i++) {
            assertEquals("Path mismatch at index " + i, raw.path(i), compact.path(i));
            assertEquals(raw.size(i), compact.size(i));
            assertEquals(raw.lastModifiedMillis(i), compact.lastModifiedMillis(i));
        }
        assertTrue(compact.estimatedBytes() > 0);
    }

    public void testSchemaCacheKeyFormatConfigFiltering() {
        Map<String, Object> configWithCreds = new LinkedHashMap<>();
        configWithCreds.put("delimiter", "|");
        configWithCreds.put("access_key", "SECRET");
        configWithCreds.put("format", "csv");

        SchemaCacheKey key1 = SchemaCacheKey.build("s3://b/f.csv", 1000L, ".csv", configWithCreds);
        assertFalse(key1.formatConfig().contains("SECRET"));
        assertTrue(key1.formatConfig().contains("delimiter=|"));
        assertTrue(key1.formatConfig().contains("format=csv"));

        Map<String, Object> configNoCreds = Map.of("delimiter", "|", "format", "csv");
        SchemaCacheKey key2 = SchemaCacheKey.build("s3://b/f.csv", 1000L, ".csv", configNoCreds);
        assertEquals(key1.formatConfig(), key2.formatConfig());
    }

    public void testListingCacheKeyCredentialHash() {
        long[] hash1 = ListingCacheKey.computeCredentialHash(Map.of("access_key", "key1", "secret_key", "sec1"));
        long[] hash2 = ListingCacheKey.computeCredentialHash(Map.of("access_key", "key2", "secret_key", "sec1"));
        long[] hash3 = ListingCacheKey.computeCredentialHash(Map.of("access_key", "key1", "secret_key", "sec1"));

        assertFalse(hash1[0] == hash2[0] && hash1[1] == hash2[1]);
        assertEquals(hash1[0], hash3[0]);
        assertEquals(hash1[1], hash3[1]);

        long[] noCredHash = ListingCacheKey.computeCredentialHash(Map.of("format", "parquet"));
        assertEquals(0L, noCredHash[0]);
        assertEquals(0L, noCredHash[1]);
    }

    // --- test helpers ---

    private static SchemaCacheEntry testSchemaEntry() {
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.FALSE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "value", DataType.DOUBLE, Nullability.TRUE, null, false)
        );
        return SchemaCacheEntry.from(schema, "parquet", "s3://bucket/data/file.parquet", Map.of());
    }

    private static FileList testGenericFileList() {
        Instant now = Instant.now();
        List<StorageEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries.add(
                new StorageEntry(StoragePath.of("s3://bucket/data/part-" + Strings.format("%05d", i) + ".parquet"), 1024L * (i + 1), now)
            );
        }
        return GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");
    }

    private static FileList testCompactFileList() {
        return GlobExpander.compact(testGenericFileList(), "s3://bucket/data/");
    }

    private static FileList testCompactHiveFileList() {
        return GlobExpander.compact(testHiveGenericFileList(), "s3://bucket/data/");
    }

    private static FileList testHiveGenericFileList() {
        Instant now = Instant.now();
        List<StorageEntry> entries = new ArrayList<>();
        Map<StoragePath, Map<String, Object>> filePartitions = new LinkedHashMap<>();

        String[] years = { "2024", "2025" };
        String[] months = { "01", "06", "12" };
        int fileIdx = 0;
        for (String year : years) {
            for (String month : months) {
                for (int f = 0; f < 2; f++) {
                    StoragePath path = StoragePath.of(
                        "s3://bucket/data/year=" + year + "/month=" + month + "/part-" + Strings.format("%05d", fileIdx) + ".parquet"
                    );
                    entries.add(new StorageEntry(path, 1024L * (fileIdx + 1), now));
                    Map<String, Object> pv = new LinkedHashMap<>();
                    pv.put("year", year);
                    pv.put("month", month);
                    filePartitions.put(path, pv);
                    fileIdx++;
                }
            }
        }

        Map<String, DataType> partitionColumns = new LinkedHashMap<>();
        partitionColumns.put("year", DataType.KEYWORD);
        partitionColumns.put("month", DataType.KEYWORD);
        PartitionMetadata pm = new PartitionMetadata(partitionColumns, filePartitions);

        return GlobExpander.fileListOf(entries, "s3://bucket/data/*" + "*/*.parquet", pm);
    }
}
