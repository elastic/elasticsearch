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
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    public void testSchemaCacheKeySeparatesErrorModeAndSchemaResolution() {
        SchemaCacheKey base = SchemaCacheKey.build("s3://b/f.csv", 1000L, ".csv", Map.of("format", "csv", "header_row", true));
        SchemaCacheKey nullField = SchemaCacheKey.build(
            "s3://b/f.csv",
            1000L,
            ".csv",
            Map.of("format", "csv", "header_row", true, "error_mode", "null_field")
        );
        SchemaCacheKey skipRow = SchemaCacheKey.build(
            "s3://b/f.csv",
            1000L,
            ".csv",
            Map.of("format", "csv", "header_row", true, "error_mode", "skip_row")
        );
        SchemaCacheKey unionByName = SchemaCacheKey.build(
            "s3://b/f.csv",
            1000L,
            ".csv",
            Map.of("format", "csv", "header_row", true, "schema_resolution", "union_by_name")
        );
        assertNotEquals(base.formatConfig(), nullField.formatConfig());
        assertNotEquals(base.formatConfig(), skipRow.formatConfig());
        assertNotEquals(nullField.formatConfig(), skipRow.formatConfig());
        assertNotEquals(base.formatConfig(), unionByName.formatConfig());
        assertTrue(nullField.formatConfig().contains("error_mode=null_field"));
        assertTrue(skipRow.formatConfig().contains("error_mode=skip_row"));
        assertTrue(unionByName.formatConfig().contains("schema_resolution=union_by_name"));
    }

    /**
     * The mode changes record boundaries, null-ness ({@code \N}) and values on the same bytes,
     * so queries differing only in mode must never share a schema-cache entry or a stats
     * fingerprint.
     */
    public void testSchemaCacheKeySeparatesModes() {
        SchemaCacheKey base = SchemaCacheKey.build("s3://b/f.tsv", 1000L, ".tsv", Map.of("format", "tsv", "header_row", true));
        SchemaCacheKey quoted = SchemaCacheKey.build(
            "s3://b/f.tsv",
            1000L,
            ".tsv",
            Map.of("format", "tsv", "header_row", true, "mode", "quoted")
        );
        SchemaCacheKey escaped = SchemaCacheKey.build(
            "s3://b/f.tsv",
            1000L,
            ".tsv",
            Map.of("format", "tsv", "header_row", true, "mode", "escaped")
        );
        assertNotEquals(base.formatConfig(), quoted.formatConfig());
        assertNotEquals(base.formatConfig(), escaped.formatConfig());
        assertNotEquals(quoted.formatConfig(), escaped.formatConfig());
        assertTrue(quoted.formatConfig().contains("mode=quoted"));
        assertTrue(escaped.formatConfig().contains("mode=escaped"));
    }

    public void testSchemaCacheKeySeparatesTrimSpaces() {
        // trim_spaces changes stored string values (and the null-ness of whitespace-only cells) on the
        // same bytes, so two configs differing only in it must not share a cached schema/stats fingerprint.
        SchemaCacheKey base = SchemaCacheKey.build("s3://b/f.csv", 1000L, ".csv", Map.of("format", "csv"));
        SchemaCacheKey trimmed = SchemaCacheKey.build("s3://b/f.csv", 1000L, ".csv", Map.of("format", "csv", "trim_spaces", true));
        assertNotEquals(base.formatConfig(), trimmed.formatConfig());
        assertTrue(trimmed.formatConfig().contains("trim_spaces=true"));
    }

    /**
     * Bare {@code multi_value_syntax: brackets} resolves the mode to quoted on a no-quote
     * baseline (and selects the bracket-aware record scanner everywhere), so two configs differing
     * only in this key can interpret the same bytes with different record boundaries — they must
     * not share a schema-cache entry or a stats fingerprint.
     */
    public void testSchemaCacheKeySeparatesMultiValueSyntax() {
        SchemaCacheKey base = SchemaCacheKey.build("s3://b/f.tsv", 1000L, ".tsv", Map.of("format", "tsv", "header_row", true));
        SchemaCacheKey brackets = SchemaCacheKey.build(
            "s3://b/f.tsv",
            1000L,
            ".tsv",
            Map.of("format", "tsv", "header_row", true, "multi_value_syntax", "brackets")
        );
        assertNotEquals(base.formatConfig(), brackets.formatConfig());
        assertTrue(brackets.formatConfig().contains("multi_value_syntax=brackets"));
    }

    public void testReconcileSourceStatsDiscriminatesOnConfigFingerprint() throws Exception {
        // Two queries over the SAME file under different WITH options produce two distinct
        // SchemaCacheEntry records that share (path, mtime) but differ on formatConfig. Each
        // carries its own CONFIG_FINGERPRINT_KEY in safeMetadata. A stats contribution from a
        // scan under one config must enrich ONLY that config's entry, never cross-pollinate the
        // sibling — otherwise the second interpretation serves a row count it never produced.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "s3://bucket/data/file.csv";
            long mtime = 1000L;
            SchemaCacheKey keyHeader = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv", "header_row", true));
            SchemaCacheKey keyNoHeader = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv", "header_row", false));

            List<Attribute> schema = List.of(
                new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.FALSE, null, false)
            );
            service.getOrComputeSchema(
                keyHeader,
                k -> SchemaCacheEntry.from(schema, "csv", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp-header"), Map.of())
            );
            service.getOrComputeSchema(
                keyNoHeader,
                k -> SchemaCacheEntry.from(schema, "csv", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp-noheader"), Map.of())
            );

            Map<String, Object> stats = new LinkedHashMap<>();
            stats.put(ExternalStats.MTIME_MILLIS_KEY, mtime);
            stats.put(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp-header");
            stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 42L);
            service.reconcileSourceStats(Map.of(path, stats));

            SchemaCacheEntry enriched = service.getOrComputeSchema(keyHeader, k -> { throw new AssertionError("should be cached"); });
            assertEquals(42L, enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));

            SchemaCacheEntry untouched = service.getOrComputeSchema(keyNoHeader, k -> { throw new AssertionError("should be cached"); });
            assertFalse(untouched.safeMetadata().containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    public void testReconcileDeduplicatesDuplicateWholeFileContributions() throws Exception {
        // A whole-file read can be captured more than once for the same file in a single query
        // (e.g. a schema-probe pass plus the data scan on the non-seekable compressed path). Each
        // contribution already covers the entire file, so they must be DEDUPLICATED, not summed —
        // summing two complete reads would double COUNT(*). Only PARTIAL_CHUNK contributions, which
        // partition the file, may be summed.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv.bz2";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".bz2", Map.of("format", "csv"));
            List<Attribute> schema = List.of(
                new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.FALSE, null, false)
            );
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(schema, "csv", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp"), Map.of())
            );

            Map<String, Object> wholeFileA = new LinkedHashMap<>();
            wholeFileA.put(ExternalStats.MTIME_MILLIS_KEY, mtime);
            wholeFileA.put(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp");
            wholeFileA.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
            Map<String, Object> wholeFileB = new LinkedHashMap<>(wholeFileA);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(wholeFileA, wholeFileB)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "duplicate whole-file reads must dedup, not sum",
                100L,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    public void testReconcileMergesColumnCoverageAcrossWholeFileContributions() throws Exception {
        // A schema-probe pass and a data scan can both publish a whole-file contribution for the
        // same file under the same pinned config. The row count is invariant between them, but the
        // probe pass typically projects fewer columns than the scan, so it tracks fewer
        // _stats.columns.* keys. First-wins-and-drop would lose the broader-coverage view; we
        // union the column-stats keys instead so the cache ends up with the strictly best
        // coverage available.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv.bz2";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".bz2", Map.of("format", "csv"));
            List<Attribute> schema = List.of(
                new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.FALSE, null, false)
            );
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(schema, "csv", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp"), Map.of())
            );

            Map<String, Object> probe = new LinkedHashMap<>();
            probe.put(ExternalStats.MTIME_MILLIS_KEY, mtime);
            probe.put(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp");
            probe.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
            probe.put(SourceStatisticsSerializer.columnNullCountKey("id"), 0L);

            Map<String, Object> scan = new LinkedHashMap<>();
            scan.put(ExternalStats.MTIME_MILLIS_KEY, mtime);
            scan.put(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp");
            scan.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
            scan.put(SourceStatisticsSerializer.columnNullCountKey("id"), 0L);
            scan.put(SourceStatisticsSerializer.columnNullCountKey("name"), 3L);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(probe, scan)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            Map<String, Object> meta = enriched.safeMetadata();
            assertEquals(100L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertEquals(
                "first contribution's column-stat must win when both supply it",
                0L,
                meta.get(SourceStatisticsSerializer.columnNullCountKey("id"))
            );
            assertEquals(
                "later contribution must fill in column-stats the first one didn't have",
                3L,
                meta.get(SourceStatisticsSerializer.columnNullCountKey("name"))
            );
        }
    }

    public void testReconcilePoisonDiscardsAllContributionsForFile() throws Exception {
        // CHUNK_HAD_ERRORS marker → entire file is dropped, even if WholeFile/StripeFragment
        // contributions ride alongside it. Locks the poison gate in
        // reconcileSourceStatsFromContributions: a SKIP_ROW chunk's stats can't be trusted to
        // represent the file accurately, so we throw away every contribution for this path.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> wholeFile = wholeFileStats(mtime, "fp", 100L);
            Map<String, Object> poison = new LinkedHashMap<>();
            poison.put(ExternalStats.CHUNK_HAD_ERRORS_KEY, Boolean.TRUE);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(wholeFile, poison)));

            SchemaCacheEntry untouched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            // Compare the full metadata map (not just absence of STATS_ROW_COUNT) so the test
            // catches any silent partial-merge — e.g., an mtime or fingerprint key leaking onto
            // the entry from the discarded WholeFile contribution.
            assertEquals(
                "poisoned file must leave the seeded entry untouched",
                Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp"),
                untouched.safeMetadata()
            );
        }
    }

    public void testReconcileSingleCompleteStripe() throws Exception {
        // One chunk fully covers stripe 0 to EOF: complete, marker = 0, whole-file fold = 100.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            service.reconcileSourceStatsFromContributions(
                Map.of(path, List.of(stripeFragment(mtime, "fp", 100L, 1024L, 0, 0, 100, true, true, true)))
            );

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(100L, enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertEquals(0L, enriched.safeMetadata().get(ExternalStats.STRIPE_LAST_INDEX_KEY));
        }
    }

    public void testReconcileStripeTiledByTwoPageFragments() throws Exception {
        // Within one chunk a stripe is split across two pages (batchSize boundary): the fragments tile
        // the stripe head-to-EOF and fold to the true count. This is the page↔stripe split inside a
        // single scan.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            service.reconcileSourceStatsFromContributions(
                Map.of(
                    path,
                    List.of(
                        stripeFragment(mtime, "fp", 40L, 1024L, 0, 0, 40, true, false, false),
                        stripeFragment(mtime, "fp", 60L, 1024L, 0, 40, 100, false, true, true)
                    )
                )
            );

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "page fragments tiling a stripe must sum",
                100L,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    public void testReconcileMisalignedTilingsFromTwoScansFoldOnce() throws Exception {
        // THE central orthogonal-model guarantee: two scans of one file PAGE the same stripe
        // differently — scan A covers stripe 0 in one fragment, scan B splits it at a different page
        // boundary — yet because attribution is record-canonical the interval-cover folds ONE chain
        // to the same count. 100, never 100+100 or 100+40+60. Double-count is unrepresentable.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            // Scan A: stripe 0 in one page [0,100).
            Map<String, Object> a = stripeFragment(mtime, "fp", 100L, 1024L, 0, 0, 100, true, true, true);
            // Scan B: same stripe 0, split at byte 30 (different batchSize/chunking) into [0,30)+[30,100).
            Map<String, Object> b1 = stripeFragment(mtime, "fp", 30L, 1024L, 0, 0, 30, true, false, false);
            Map<String, Object> b2 = stripeFragment(mtime, "fp", 70L, 1024L, 0, 30, 100, false, true, true);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(a, b1, b2)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "misaligned page tilings of one stripe must fold once, not double-count",
                100L,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    public void testReconcileStripeSplitAcrossChunksTiles() throws Exception {
        // A chunk boundary lands mid-stripe (the seekable/multi-node case): stripe 0's head fragment
        // comes from chunk A (atStripeStart, not atStripeEnd — chunk ended mid-stripe) and its tail from
        // chunk B (atStripeEnd). The two record-canonical, contiguous fragments tile the stripe.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> chunkA = stripeFragment(mtime, "fp", 55L, 1024L, 0, 0, 70, true, false, false);
            Map<String, Object> chunkB = stripeFragment(mtime, "fp", 45L, 1024L, 0, 70, 100, false, true, true);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(chunkA, chunkB)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "a stripe split across chunks must tile and fold",
                100L,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    public void testReconcileGapNotFolded() throws Exception {
        // Fragments that leave a gap before reaching the stripe end (a lost chunk) are incomplete: the
        // interval-cover hits a gap, the stripe is skipped, and the warm query re-scans — never an
        // under-count.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> head = stripeFragment(mtime, "fp", 40L, 1024L, 0, 0, 40, true, false, false);
            Map<String, Object> tail = stripeFragment(mtime, "fp", 30L, 1024L, 0, 60, 100, false, true, true); // gap [40,60)

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(head, tail)));

            SchemaCacheEntry untouched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertNull("a gap must leave the stripe uncommitted", untouched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    public void testReconcileNoStartAnchorNotFolded() throws Exception {
        // A stripe whose first record was never observed (no atStripeStart fragment — the prefix chunk
        // was lost) cannot be anchored: incomplete, safe-miss. Guards against treating a missing-prefix
        // suffix as a complete stripe.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> suffix = stripeFragment(mtime, "fp", 60L, 1024L, 0, 40, 100, false, true, true); // no atStripeStart

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(suffix)));

            SchemaCacheEntry untouched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertNull("an unanchored stripe must not fold", untouched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    public void testReconcileMultiStripeFileFoldsAcrossStripes() throws Exception {
        // Two complete stripes + EOF marker: per-stripe folds commit, the whole-file fold sums 0..K.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> s0 = stripeFragment(mtime, "fp", 60L, 100L, 0, 0, 100, true, true, false);
            Map<String, Object> s1 = stripeFragment(mtime, "fp", 40L, 100L, 1, 100, 180, true, true, true);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(s0, s1)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "whole-file fold sums committed stripes 0..K",
                100L,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
            assertEquals(1L, enriched.safeMetadata().get(ExternalStats.STRIPE_LAST_INDEX_KEY));
        }
    }

    public void testStripeMinMaxCoercedToResolvedTypeAcrossSampledTypeFlap() throws Exception {
        // The aggregate's resolved type, pushed down to the stats. A UInt64-style column the entry resolved
        // to DOUBLE can be harvested as a Long on a stripe whose sampled values all fit a long, and as a
        // Double on a stripe that saw a bigger value. Before the fix, folding the Long extremum against the
        // Double one hit SplitStats's "Long+Double intentionally incompatible" branch -> POISON -> the
        // column's min/max was dropped -> warm MIN/MAX re-scanned forever. The fix coerces each stripe's
        // min/max to the resolved DOUBLE up front, so the fold sees only doubles and the whole-file min/max
        // survives, bit-identical to what a full MIN/MAX(double) scan computes.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/events.ndjson";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of("format", "ndjson"));
            seedSchemaCacheTyped(service, key, path, "fp", "uid", DataType.DOUBLE);

            Map<String, Object> s0 = stripeFragment(mtime, "fp", 60L, 100L, 0, 0, 100, true, true, false);
            s0.put(SourceStatisticsSerializer.columnMinKey("uid"), 10L); // stripe-0 harvested uid as LONG
            s0.put(SourceStatisticsSerializer.columnMaxKey("uid"), 50L);
            Map<String, Object> s1 = stripeFragment(mtime, "fp", 40L, 100L, 1, 100, 180, true, true, true);
            s1.put(SourceStatisticsSerializer.columnMinKey("uid"), 20.0); // stripe-1 harvested uid as DOUBLE
            s1.put(SourceStatisticsSerializer.columnMaxKey("uid"), 90.0);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(s0, s1)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            Object min = enriched.safeMetadata().get(SourceStatisticsSerializer.columnMinKey("uid"));
            Object max = enriched.safeMetadata().get(SourceStatisticsSerializer.columnMaxKey("uid"));
            assertNotNull("whole-file uid MIN must survive the Long/Double stripe flap (no POISON)", min);
            assertNotNull("whole-file uid MAX must survive the Long/Double stripe flap (no POISON)", max);
            assertEquals("min folded as the resolved DOUBLE", 10.0, ((Number) min).doubleValue(), 0.0);
            assertEquals("max folded as the resolved DOUBLE", 90.0, ((Number) max).doubleValue(), 0.0);
            assertTrue("min is served as the resolved type (Double)", min instanceof Double);
            assertTrue("max is served as the resolved type (Double)", max instanceof Double);
        }
    }

    public void testWholeFileUnrepresentableStatDroppedForLongColumn() throws Exception {
        // The serve-side wrong-answer guard. If a column the entry resolved to LONG receives a Double min/max
        // that exceeds Long range (the genuinely inconsistent case), it must NOT be stored — otherwise the
        // serve would coerce it via ((Number) value).longValue() and emit overflow garbage. The whole-file
        // path has no POISON fold, so the unrepresentable value is dropped and the column safe-misses.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/events.ndjson";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of("format", "ndjson"));
            seedSchemaCacheTyped(service, key, path, "fp", "uid", DataType.LONG);

            Map<String, Object> wholeFile = wholeFileStats(mtime, "fp", 100L);
            wholeFile.put(SourceStatisticsSerializer.columnMinKey("uid"), 1.0e19); // > Long.MAX, not long-representable
            wholeFile.put(SourceStatisticsSerializer.columnMaxKey("uid"), 2.0e19);
            service.reconcileSourceStats(Map.of(path, wholeFile));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertFalse(
                "an unrepresentable Double must not be stored as a LONG column's min (would serve overflow garbage)",
                enriched.safeMetadata().containsKey(SourceStatisticsSerializer.columnMinKey("uid"))
            );
            assertFalse(
                "an unrepresentable Double must not be stored as a LONG column's max",
                enriched.safeMetadata().containsKey(SourceStatisticsSerializer.columnMaxKey("uid"))
            );
        }
    }

    public void testWholeFileUnrepresentableStatNeutralizesStaleCommittedExtremum() throws Exception {
        // F7 (elastic/elasticsearch#150920): the whole-file reconcile putAll-overlays the new contribution onto
        // the existing entry. If a later scan's extremum is unrepresentable in the resolved type, merely REMOVING
        // the value key would let the PRIOR committed value survive the overlay and serve a stale (wrong) warm
        // answer. The reconcile must write the unservable MARKER so marker-wins normalization forces a safe-miss.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/events.ndjson";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of("format", "ndjson"));
            seedSchemaCacheTyped(service, key, path, "fp", "uid", DataType.LONG);

            // A good LONG min/max commits first.
            Map<String, Object> good = wholeFileStats(mtime, "fp", 100L);
            good.put(SourceStatisticsSerializer.columnMinKey("uid"), 5L);
            good.put(SourceStatisticsSerializer.columnMaxKey("uid"), 90L);
            service.reconcileSourceStats(Map.of(path, good));

            // A later scan of the same file yields a Double past Long range (inconsistent). The stale 5/90 must be
            // neutralized, not left to serve.
            Map<String, Object> bad = wholeFileStats(mtime, "fp", 100L);
            bad.put(SourceStatisticsSerializer.columnMinKey("uid"), 1.0e19);
            bad.put(SourceStatisticsSerializer.columnMaxKey("uid"), 2.0e19);
            service.reconcileSourceStats(Map.of(path, bad));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("cached"); });
            assertTrue(
                "the unservable marker must be written so a stale committed min cannot survive the putAll overlay",
                enriched.safeMetadata().containsKey(SourceStatisticsSerializer.columnMinUnservableKey("uid"))
            );
            SplitStats ss = SplitStats.of(enriched.safeMetadata());
            assertNull("stale min must not be servable (marker-wins safe-miss)", ss.columnMin("uid"));
            assertNull("stale max must not be servable (marker-wins safe-miss)", ss.columnMax("uid"));
        }
    }

    public void testCoerceColumnStatsToResolvedTypesRules() {
        String[] names = { "d", "l", "i", "kw" };
        DataType[] types = { DataType.DOUBLE, DataType.LONG, DataType.INTEGER, DataType.KEYWORD };

        // DOUBLE column: a Long extremum widens to Double (exact for an extremum — monotonic rounding).
        Map<String, Object> in = new LinkedHashMap<>();
        in.put(SourceStatisticsSerializer.columnMinKey("d"), 7L);
        in.put(SourceStatisticsSerializer.columnMaxKey("d"), 7.0); // already Double -> unchanged
        // LONG column: an Integer widens to Long; an exact-integral Double narrows to Long.
        in.put(SourceStatisticsSerializer.columnMinKey("l"), Integer.valueOf(3));
        in.put(SourceStatisticsSerializer.columnMaxKey("l"), 9.0);
        // INTEGER column: an in-range Long narrows to Integer.
        in.put(SourceStatisticsSerializer.columnMinKey("i"), 5L);
        // KEYWORD column: BytesRef-style value is left untouched.
        in.put(SourceStatisticsSerializer.columnMinKey("kw"), "alpha");

        Map<String, Object> out = ExternalSourceCacheService.coerceColumnStatsToResolvedTypes(in, names, types, false);
        assertEquals(Double.valueOf(7.0), out.get(SourceStatisticsSerializer.columnMinKey("d")));
        assertEquals(Double.valueOf(7.0), out.get(SourceStatisticsSerializer.columnMaxKey("d")));
        assertEquals(Long.valueOf(3L), out.get(SourceStatisticsSerializer.columnMinKey("l")));
        assertEquals(Long.valueOf(9L), out.get(SourceStatisticsSerializer.columnMaxKey("l")));
        assertEquals(Integer.valueOf(5), out.get(SourceStatisticsSerializer.columnMinKey("i")));
        assertEquals("alpha", out.get(SourceStatisticsSerializer.columnMinKey("kw")));

        // Unrepresentable: a Double past Long range for a LONG column. dropUnrepresentable=false leaves it
        // (the stripe path relies on the POISON fold); =true drops it (the whole-file path, no POISON).
        Map<String, Object> big = new LinkedHashMap<>();
        big.put(SourceStatisticsSerializer.columnMaxKey("l"), 1.0e19);
        assertEquals(
            "left for POISON on the stripe path",
            Double.valueOf(1.0e19),
            ExternalSourceCacheService.coerceColumnStatsToResolvedTypes(big, names, types, false)
                .get(SourceStatisticsSerializer.columnMaxKey("l"))
        );
        assertFalse(
            "dropped on the whole-file path",
            ExternalSourceCacheService.coerceColumnStatsToResolvedTypes(big, names, types, true)
                .containsKey(SourceStatisticsSerializer.columnMaxKey("l"))
        );
    }

    public void testColumnValueCountFoldsAcrossStripesAsSum() throws Exception {
        // COUNT(col) is served from the harvested value count. Across stripes it must SUM (each stripe holds
        // the non-null value count of its own rows), so a multivalued column's whole-file COUNT is the total
        // value count, not the row count.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/events.ndjson";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of("format", "ndjson"));
            seedSchemaCacheTyped(service, key, path, "fp", "tags", DataType.KEYWORD);

            Map<String, Object> s0 = stripeFragment(mtime, "fp", 60L, 100L, 0, 0, 100, true, true, false);
            s0.put(SourceStatisticsSerializer.columnValueCountKey("tags"), 130L); // 60 rows, 130 values
            Map<String, Object> s1 = stripeFragment(mtime, "fp", 40L, 100L, 1, 100, 180, true, true, true);
            s1.put(SourceStatisticsSerializer.columnValueCountKey("tags"), 95L); // 40 rows, 95 values

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(s0, s1)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            Object vc = enriched.safeMetadata().get(SourceStatisticsSerializer.columnValueCountKey("tags"));
            assertNotNull("value_count must fold across stripes", vc);
            assertEquals("value_count folds as SUM (130 + 95), independent of the 100-row count", 225L, ((Number) vc).longValue());
        }
    }

    public void testStripeFoldDropsColumnMissingFromSomeStripe() throws Exception {
        // Under the default PROJECTED scope, different cold queries harvest different columns, so one file's
        // committed stripes can carry non-uniform column sets. The text within-file fold uses
        // implicitNullsForAbsentColumn=false: a column present in only SOME stripes is DROPPED (so the serve
        // safe-misses), NOT folded under the footer implicit-nulls contract (which would fold the missing
        // stripe's rows into the column's null_count and under-count COUNT(col) / serve a subset MIN/MAX).
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/events.ndjson";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of("format", "ndjson"));
            List<Attribute> schema = List.of(
                new ReferenceAttribute(Source.EMPTY, null, "a", DataType.LONG, Nullability.TRUE, null, false),
                new ReferenceAttribute(Source.EMPTY, null, "b", DataType.LONG, Nullability.TRUE, null, false)
            );
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(schema, "ndjson", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp"), Map.of())
            );

            // stripe 0 harvested only column a; stripe 1 (EOF) harvested only column b.
            Map<String, Object> s0 = stripeFragment(mtime, "fp", 60L, 100L, 0, 0, 100, true, true, false);
            s0.put(SourceStatisticsSerializer.columnValueCountKey("a"), 60L);
            s0.put(SourceStatisticsSerializer.columnNullCountKey("a"), 0L);
            Map<String, Object> s1 = stripeFragment(mtime, "fp", 40L, 100L, 1, 100, 180, true, true, true);
            s1.put(SourceStatisticsSerializer.columnValueCountKey("b"), 40L);
            s1.put(SourceStatisticsSerializer.columnNullCountKey("b"), 0L);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(s0, s1)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            // rowCount still folds (60 + 40), but neither column survives -> COUNT(a)/COUNT(b) safe-miss.
            assertEquals(100L, ((Number) enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());
            assertFalse(
                "column a, present in only one stripe, must be dropped (safe-miss) not implicit-null-folded",
                enriched.safeMetadata().containsKey(SourceStatisticsSerializer.columnValueCountKey("a"))
            );
            assertFalse(
                "column b, present in only one stripe, must be dropped (safe-miss)",
                enriched.safeMetadata().containsKey(SourceStatisticsSerializer.columnValueCountKey("b"))
            );
        }
    }

    public void testStripeFoldKeepsColumnAcrossEmptyStripeGap() throws Exception {
        // The empty-stripe exemption (the other half of the false-fold change): a uniform column observed in
        // every NON-EMPTY stripe must survive even when a record-empty (0-row) edge stripe in the byte-range
        // cover carries no column stats. Without the exemption the column would be wrongly dropped (loss of
        // short-circuit). This is the surviving-column regression guard.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/events.ndjson";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of("format", "ndjson"));
            seedSchemaCacheTyped(service, key, path, "fp", "v", DataType.LONG);

            // stripe 0: 60 rows with col v; stripe 1: EMPTY (0 rows, no col); stripe 2 (EOF): 40 rows with col v.
            Map<String, Object> s0 = stripeFragment(mtime, "fp", 60L, 100L, 0, 0, 100, true, true, false);
            s0.put(SourceStatisticsSerializer.columnValueCountKey("v"), 60L);
            s0.put(SourceStatisticsSerializer.columnNullCountKey("v"), 0L);
            Map<String, Object> s1empty = stripeFragment(mtime, "fp", 0L, 100L, 1, 100, 100, true, true, false);
            Map<String, Object> s2 = stripeFragment(mtime, "fp", 40L, 100L, 2, 100, 180, true, true, true);
            s2.put(SourceStatisticsSerializer.columnValueCountKey("v"), 40L);
            s2.put(SourceStatisticsSerializer.columnNullCountKey("v"), 0L);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(s0, s1empty, s2)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(100L, ((Number) enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());
            Object vc = enriched.safeMetadata().get(SourceStatisticsSerializer.columnValueCountKey("v"));
            assertNotNull("column v must survive the empty-stripe gap (not dropped)", vc);
            assertEquals("COUNT(v) folds across the non-empty stripes (60 + 40)", 100L, ((Number) vc).longValue());
        }
    }

    public void testReconcileAccumulatesStripesAcrossQueries() throws Exception {
        // Stripe knowledge composes across queries: query A commits stripe 0 (no EOF observed), query B
        // commits stripe 1 + EOF; the whole-file fold fires only once the union is complete.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            service.reconcileSourceStatsFromContributions(
                Map.of(path, List.of(stripeFragment(mtime, "fp", 30L, 100L, 0, 0, 100, true, true, false)))
            );
            SchemaCacheEntry afterA = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertNull("incomplete after query A", afterA.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertNotNull("stripe 0 committed by A", afterA.safeMetadata().get(ExternalStats.STRIPE_ENTRY_PREFIX + "0"));

            service.reconcileSourceStatsFromContributions(
                Map.of(path, List.of(stripeFragment(mtime, "fp", 70L, 100L, 1, 100, 150, true, true, true)))
            );
            SchemaCacheEntry afterB = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals("stripes from two queries compose", 100L, afterB.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    /**
     * WRONG-DATA regression: stripe ordinals are only comparable within one grid. Commits from data nodes
     * running DIFFERENT stripe.size values (rolling restart / config drift) used to interleave into one
     * entry — grid-B's stripe 1 overwrote grid-A's stripe 1 while grid-A's EOF marker survived, so the
     * 0..K fold served a row count covering only part of the file over a "complete" cover. The entry now
     * carries a grid stamp: a delta on a different grid CLEARS the stale stripe state and accumulation
     * restarts on the new grid (safe-miss), converging once the new grid covers the file.
     */
    public void testMixedStripeGridsResetInsteadOfCorrupting() throws Exception {
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            // 200-byte file. Grid A=100: stripes {0:[0,100), 1:[100,200)}. Grid B=50: stripes 0..3.
            // Round 1 (grid A): the tail stripe + EOF. No fold yet (stripe 0 missing).
            service.reconcileSourceStatsFromContributions(
                Map.of(path, List.of(stripeFragment(mtime, "fp", 6L, 100L, 1, 100, 200, true, true, true)))
            );
            SchemaCacheEntry afterA = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertNull("incomplete on grid A", afterA.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertEquals("grid A stamped", 100L, afterA.safeMetadata().get(ExternalStats.STRIPE_GRID_KEY));

            // Round 2 (grid B): stripes 0..1 of the finer grid, no EOF. Pre-fix this overwrote grid-A's
            // stripe 1 while its EOF marker survived -> fold 0..1 -> row_count 5 (truth 11) served.
            service.reconcileSourceStatsFromContributions(
                Map.of(
                    path,
                    List.of(
                        stripeFragment(mtime, "fp", 2L, 50L, 0, 0, 50, true, true, false),
                        stripeFragment(mtime, "fp", 3L, 50L, 1, 50, 100, true, true, false)
                    )
                )
            );
            SchemaCacheEntry afterB = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertNull(
                "grid change must reset, NEVER fold mixed grids",
                afterB.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
            assertEquals("re-stamped to grid B", 50L, afterB.safeMetadata().get(ExternalStats.STRIPE_GRID_KEY));
            assertNull("grid-A EOF marker cleared", afterB.safeMetadata().get(ExternalStats.STRIPE_LAST_INDEX_KEY));

            // Round 3 (grid B): the rest of the file + EOF -> converges to the exact count on the new grid.
            service.reconcileSourceStatsFromContributions(
                Map.of(
                    path,
                    List.of(
                        stripeFragment(mtime, "fp", 2L, 50L, 2, 100, 150, true, true, false),
                        stripeFragment(mtime, "fp", 4L, 50L, 3, 150, 200, true, true, true)
                    )
                )
            );
            SchemaCacheEntry afterC = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "accumulation converges on the new grid",
                11L,
                afterC.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    public void testReconcileEmptyStripeFromOversizedRecord() throws Exception {
        // A record larger than the grid skips an ordinal entirely — the reader emits an explicit
        // zero-length empty fragment for it (atStripeStart & atStripeEnd). The whole-file fold counts
        // every row once across the non-empty stripes; the empty stripe contributes 0.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            long grid = 100L;
            // Stripe 0 holds a giant record [0,250); stripe 1 is empty (its line falls inside the record);
            // stripe 2 holds the tail.
            Map<String, Object> s0 = stripeFragment(mtime, "fp", 5L, grid, 0, 0, 250, true, true, false);
            Map<String, Object> s1empty = stripeFragment(mtime, "fp", 0L, grid, 1, 250, 250, true, true, false);
            Map<String, Object> s2 = stripeFragment(mtime, "fp", 5L, grid, 2, 250, 300, true, true, true);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(s0, s1empty, s2)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "every row counted once across the empty-stripe gap",
                10L,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
            assertEquals(2L, enriched.safeMetadata().get(ExternalStats.STRIPE_LAST_INDEX_KEY));
        }
    }

    public void testConcurrentDistinctStripeCommitsAccumulateWithoutLostUpdate() throws Exception {
        // N queries each commit a distinct complete stripe of the same file at the same instant. The
        // per-path commit lock must serialize the read-modify-write so no stripe is dropped: the
        // whole-file fold equals the sum of all stripes and stripes 0..N-1 + EOF are all present.
        // Without the lock, concurrent commits each snapshot the same entry and the later put drops the
        // earlier stripe (lost update) — this is the cross-query concurrency bug the lock guards.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            long grid = 100L;
            int stripes = 16;
            long rowsPerStripe = 10L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(stripes);
            ExecutorService exec = Executors.newFixedThreadPool(stripes);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                for (int i = 0; i < stripes; i++) {
                    final int ordinal = i;
                    exec.submit(() -> {
                        try {
                            start.await();
                            long s = ordinal * grid;
                            boolean eof = ordinal == stripes - 1;
                            service.reconcileSourceStatsFromContributions(
                                Map.of(
                                    path,
                                    List.of(stripeFragment(mtime, "fp", rowsPerStripe, grid, ordinal, s, s + grid, true, true, eof))
                                )
                            );
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        } finally {
                            done.countDown();
                        }
                    });
                }
                start.countDown();
                assertTrue("commits did not finish", done.await(30, java.util.concurrent.TimeUnit.SECONDS));
            } finally {
                exec.shutdown();
            }
            assertNull("commit threw under contention: " + failure.get(), failure.get());

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "no stripe lost under concurrent commits",
                stripes * rowsPerStripe,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
            assertEquals((long) (stripes - 1), enriched.safeMetadata().get(ExternalStats.STRIPE_LAST_INDEX_KEY));
        }
    }

    public void testConcurrentDuplicateWholeFileContributionsFoldToOne() throws Exception {
        // N queries each contribute the SAME whole-file stats for one file simultaneously. Dedup plus
        // the per-path lock must yield the single row count, never N times it.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            long rows = 500L;
            int threads = 12;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            ExecutorService exec = Executors.newFixedThreadPool(threads);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                for (int i = 0; i < threads; i++) {
                    exec.submit(() -> {
                        try {
                            start.await();
                            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(wholeFileStats(mtime, "fp", rows))));
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        } finally {
                            done.countDown();
                        }
                    });
                }
                start.countDown();
                assertTrue("contributions did not finish", done.await(30, java.util.concurrent.TimeUnit.SECONDS));
            } finally {
                exec.shutdown();
            }
            assertNull("reconcile threw under contention: " + failure.get(), failure.get());

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "duplicate whole-file contributions must not sum",
                rows,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    public void testConcurrentReadDuringStripeCommitsNeverSeesTornFold() throws Exception {
        // A reader polls the cache entry while committers add stripes. The whole-file row_count fold is
        // published only once the file is complete (stripes 0..K + EOF), and each commit re-puts the
        // entry's metadata as a whole map, so a concurrent reader must see either no row_count
        // (incomplete) or exactly the final sum — never a partial/torn intermediate.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            long grid = 100L;
            int stripes = 16;
            long rowsPerStripe = 10L;
            long expected = stripes * rowsPerStripe;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch committersDone = new CountDownLatch(stripes);
            AtomicBoolean stopReader = new AtomicBoolean(false);
            AtomicReference<Object> tornValue = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            ExecutorService exec = Executors.newFixedThreadPool(stripes + 1);
            try {
                exec.submit(() -> {
                    try {
                        start.await();
                        while (stopReader.get() == false) {
                            SchemaCacheEntry e = service.getOrComputeSchema(key, k -> { throw new AssertionError("seeded"); });
                            Object rc = e.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT);
                            if (rc != null && rc.equals(expected) == false) {
                                tornValue.compareAndSet(null, rc);
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
                for (int i = 0; i < stripes; i++) {
                    final int ordinal = i;
                    exec.submit(() -> {
                        try {
                            start.await();
                            long s = ordinal * grid;
                            boolean eof = ordinal == stripes - 1;
                            service.reconcileSourceStatsFromContributions(
                                Map.of(
                                    path,
                                    List.of(stripeFragment(mtime, "fp", rowsPerStripe, grid, ordinal, s, s + grid, true, true, eof))
                                )
                            );
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        } finally {
                            committersDone.countDown();
                        }
                    });
                }
                start.countDown();
                assertTrue("commits did not finish", committersDone.await(30, java.util.concurrent.TimeUnit.SECONDS));
                stopReader.set(true);
            } finally {
                exec.shutdown();
            }
            assertNull("a thread threw under contention: " + failure.get(), failure.get());
            assertNull("reader saw a torn/partial row_count fold", tornValue.get());

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(expected, enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    public void testReconcileStripeCommitRequiresMatchingMtime() throws Exception {
        // A complete stripe whose mtime disagrees with the cached entry's key must not enrich it.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            SchemaCacheKey key = SchemaCacheKey.build(path, 1000L, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            service.reconcileSourceStatsFromContributions(
                Map.of(path, List.of(stripeFragment(999L, "fp", 100L, 1024L, 0, 0, 100, true, true, true)))
            );

            SchemaCacheEntry untouched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "a stripe with a mismatched mtime must not enrich the entry",
                Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp"),
                untouched.safeMetadata()
            );
        }
    }

    public void testReconcileMixedStripedAndUnstripedNotCached() throws Exception {
        // One un-addressed fragment (a reader not yet emitting stripes) poisons the set: nothing
        // commits — a deterministic safe miss, never a guess.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> striped = stripeFragment(mtime, "fp", 40L, 1024L, 0, 0, 40, true, true, true);
            Map<String, Object> unstriped = coveredChunk(mtime, "fp", 60L, 40, 100, true);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(striped, unstriped)));

            SchemaCacheEntry untouched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "mixed striped+unstriped fragments must leave the seeded entry untouched",
                Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp"),
                untouched.safeMetadata()
            );
        }
    }

    public void testReconcileUnstripedPartialsNotCached() throws Exception {
        // Coverage-stamped but NOT stripe-addressed fragments (older nodes, the seekable-parallel and
        // macro-split paths) are never cached — a deterministic safe miss, never a guess. This is the
        // v1 contract that replaced the old per-query whole-file tiling.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> partialA = coveredChunk(mtime, "fp", 40L, 0, 40, false);
            Map<String, Object> partialB = coveredChunk(mtime, "fp", 60L, 40, 100, true);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(partialA, partialB)));

            SchemaCacheEntry untouched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "un-striped fragments must leave the seeded entry untouched",
                Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, "fp"),
                untouched.safeMetadata()
            );
        }
    }

    public void testReconcileWholeFileWinsOverConcurrentPartials() throws Exception {
        // Mixed shape: WholeFile + StripeFragments for the same file. The whole-file read is
        // authoritative — its row count already covers every row — and fragments must not be
        // summed on top. Locks the reconciler's whole-file-first routing.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> wholeFile = wholeFileStats(mtime, "fp", 100L);
            Map<String, Object> partial = coveredChunk(mtime, "fp", 40L, 0, 40, true);

            service.reconcileSourceStatsFromContributions(Map.of(path, List.of(partial, wholeFile)));

            SchemaCacheEntry enriched = service.getOrComputeSchema(key, k -> { throw new AssertionError("should be cached"); });
            assertEquals(
                "whole-file read must win over partials, not 140L",
                100L,
                enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    public void testReconcileWholeFileContributionsWithMismatchedMtimeTripsAssertion() throws Exception {
        // Defends the Javadoc contract in mergeWholeFileContributions: row count, mtime, and
        // config fingerprint must agree across all whole-file contributions for the same file.
        // Only row count was previously asserted; mtime and fingerprint mismatches were silently
        // swallowed, which could cause the merged entry to carry the wrong mtime and miss the
        // downstream cache-match in reconcileSourceStats.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> first = wholeFileStats(mtime, "fp", 100L);
            Map<String, Object> second = wholeFileStats(mtime + 1, "fp", 100L); // different mtime, same row count

            expectThrows(AssertionError.class, () -> service.reconcileSourceStatsFromContributions(Map.of(path, List.of(first, second))));
        }
    }

    public void testReconcileWholeFileContributionsWithMismatchedFingerprintTripsAssertion() throws Exception {
        // Companion to the mtime test: config fingerprint disagreement between whole-file
        // contributions is equally invalid and must be caught by the assertion.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp1");

            Map<String, Object> first = wholeFileStats(mtime, "fp1", 100L);
            Map<String, Object> second = wholeFileStats(mtime, "fp2", 100L); // different fingerprint, same row count

            expectThrows(AssertionError.class, () -> service.reconcileSourceStatsFromContributions(Map.of(path, List.of(first, second))));
        }
    }

    public void testReconcileWholeFileContributionsWithDisagreeingColumnStatTripsAssertion() throws Exception {
        // Two whole-file contributions for the same file+fingerprint+mtime must agree on every
        // column-stat key they both publish. Disagreement is a bug in the upstream publisher
        // (e.g., a race or a mis-configured probe pass), not a legitimate coverage difference.
        // Without this assertion the "first-wins" putIfAbsent produces a non-deterministic pick
        // whose survivor depends on data-node response ordering.
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(defaultSettings())) {
            String path = "file:///data/employees.csv";
            long mtime = 1000L;
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of("format", "csv"));
            seedSchemaCache(service, key, path, "fp");

            Map<String, Object> first = wholeFileStats(mtime, "fp", 100L);
            first.put(SourceStatisticsSerializer.columnNullCountKey("id"), 0L);

            Map<String, Object> second = wholeFileStats(mtime, "fp", 100L);
            second.put(SourceStatisticsSerializer.columnNullCountKey("id"), 5L); // same key, different value

            expectThrows(AssertionError.class, () -> service.reconcileSourceStatsFromContributions(Map.of(path, List.of(first, second))));
        }
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

    private static void seedSchemaCache(ExternalSourceCacheService service, SchemaCacheKey key, String path, String fingerprint)
        throws Exception {
        List<Attribute> schema = List.of(new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.FALSE, null, false));
        service.getOrComputeSchema(
            key,
            k -> SchemaCacheEntry.from(schema, "csv", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint), Map.of())
        );
    }

    /** Seeds the schema cache with a single column of a chosen resolved {@link DataType}. */
    private static void seedSchemaCacheTyped(
        ExternalSourceCacheService service,
        SchemaCacheKey key,
        String path,
        String fingerprint,
        String columnName,
        DataType columnType
    ) throws Exception {
        List<Attribute> schema = List.of(new ReferenceAttribute(Source.EMPTY, null, columnName, columnType, Nullability.TRUE, null, false));
        service.getOrComputeSchema(
            key,
            k -> SchemaCacheEntry.from(schema, "ndjson", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint), Map.of())
        );
    }

    private static Map<String, Object> wholeFileStats(long mtime, String fingerprint, long rows) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(ExternalStats.MTIME_MILLIS_KEY, mtime);
        m.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
        m.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rows);
        return m;
    }

    /** A coverage-addressed partial chunk: {@code rows} rows observed over byte range [{@code start},{@code end}). */
    private static Map<String, Object> coveredChunk(long mtime, String fingerprint, long rows, long start, long end, boolean last) {
        Map<String, Object> m = wholeFileStats(mtime, fingerprint, rows);
        m.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        m.put(ExternalStats.COVERAGE_START_KEY, start);
        m.put(ExternalStats.COVERAGE_END_KEY, end);
        m.put(ExternalStats.COVERAGE_IS_LAST_KEY, last);
        return m;
    }

    /**
     * An orthogonal-model stripe fragment as a reader emits it: {@code rows} records of stripe
     * {@code ordinal}, covering the record-canonical byte sub-range [{@code start},{@code end}), with
     * the tiling anchors {@code atStart} (holds the stripe's first record), {@code atEnd} (reached the
     * next stripe / EOF), and {@code eof} (observed end-of-input). See {@code ExternalStats}.
     */
    private static Map<String, Object> stripeFragment(
        long mtime,
        String fingerprint,
        long rows,
        long stripeSize,
        long ordinal,
        long start,
        long end,
        boolean atStart,
        boolean atEnd,
        boolean eof
    ) {
        Map<String, Object> m = wholeFileStats(mtime, fingerprint, rows);
        m.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
        m.put(ExternalStats.STRIPE_SIZE_KEY, stripeSize);
        m.put(ExternalStats.STRIPE_ORDINAL_KEY, ordinal);
        m.put(ExternalStats.COVERAGE_START_KEY, start);
        m.put(ExternalStats.COVERAGE_END_KEY, end);
        m.put(ExternalStats.STRIPE_AT_START_KEY, atStart);
        m.put(ExternalStats.STRIPE_AT_END_KEY, atEnd);
        m.put(ExternalStats.COVERAGE_IS_LAST_KEY, eof);
        return m;
    }

    private static SchemaCacheEntry testSchemaEntry() {
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.FALSE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "value", DataType.DOUBLE, Nullability.TRUE, null, false)
        );
        return SchemaCacheEntry.from(schema, "parquet", "s3://bucket/data/file.parquet", Map.of(), Map.of());
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
