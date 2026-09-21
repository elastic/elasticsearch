/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
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
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * What the two byte-budgeted external-source caches actually charge for the objects they retain.
 */
public class CacheWeightAccountingTests extends ESTestCase {

    // ---------------------------------------------------------------------------------------------
    // Schema cache: the per-column extremum stored in safeMetadata
    // ---------------------------------------------------------------------------------------------

    private static SchemaCacheEntry entryWithMin(String path, String min) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        meta.put("_stats.row_count", 10L);
        meta.put("_stats.columns.c.min", min);
        meta.put("_stats.columns.c.max", min);
        return new SchemaCacheEntry(
            new String[] { "c" },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            path,
            meta,
            Map.of(),
            0L,
            List.of()
        );
    }

    public void testSchemaEntryWeightIgnoresTheSizeOfAStoredColumnExtremum() {
        SchemaCacheEntry small = entryWithMin("s3://b/f.csv", "a");
        SchemaCacheEntry large = entryWithMin("s3://b/f.csv", "x".repeat(1_000_000));
        assertThat(
            "an entry holding a one-megabyte column extremum must not weigh the same as one holding a single character",
            large.estimatedBytes(),
            greaterThan(small.estimatedBytes())
        );
    }

    /** Control: the weigher is not inert — it does charge for the column NAMES it already walks. */
    public void testSchemaEntryWeightDoesCountColumnNames() {
        Map<String, Object> meta = Map.of(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        SchemaCacheEntry shortName = new SchemaCacheEntry(
            new String[] { "c" },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            "s3://b/f.csv",
            meta,
            Map.of(),
            0L,
            List.of()
        );
        SchemaCacheEntry longName = new SchemaCacheEntry(
            new String[] { "c".repeat(100_000) },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            "s3://b/f.csv",
            meta,
            Map.of(),
            0L,
            List.of()
        );
        assertThat(longName.estimatedBytes(), greaterThan(shortName.estimatedBytes()));
    }

    public void testSchemaCacheRetainsManyTimesItsBudgetWhenExtremaAreLarge() throws Exception {
        // 2mb total budget -> schema slice is a fifth of it.
        Settings settings = Settings.builder().put("esql.external.cache.size", "2mb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            long schemaBudget = (2L * 1024 * 1024) / 5;
            int entries = 40;
            String oneMegabyte = "x".repeat(1_000_000);
            for (int i = 0; i < entries; i++) {
                SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/f" + i + ".csv", 1000L, ".csv", Map.of());
                cache.putSchema(key, entryWithMin("s3://bucket/f" + i + ".csv", oneMegabyte));
            }
            Map<String, Object> stats = cache.usageStats();
            long retainedChars = (long) entries * 2 * oneMegabyte.length();
            assertThat(
                "the stored extrema alone hold ["
                    + retainedChars
                    + "] characters against a ["
                    + schemaBudget
                    + "] byte budget, so the cache must have evicted",
                (Long) stats.get("schema_cache.evictions"),
                greaterThan(0L)
            );
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Listing cache: the per-file partition values carried alongside a compacted listing
    // ---------------------------------------------------------------------------------------------

    private static final Map<String, DataType> PARTITION_COLUMNS = new LinkedHashMap<>(
        Map.of("year", DataType.KEYWORD, "month", DataType.KEYWORD, "day", DataType.KEYWORD)
    );

    private static String hivePath(String table, int i) {
        return "s3://warehouse/"
            + table
            + "/year=2024/month="
            + String.format(Locale.ROOT, "%02d", (i % 12) + 1)
            + "/day="
            + String.format(Locale.ROOT, "%02d", (i % 28) + 1)
            + "/part-"
            + String.format(Locale.ROOT, "%05d", i)
            + ".parquet";
    }

    private static List<StorageEntry> hiveEntries(String table, int fileCount) {
        List<StorageEntry> entries = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            entries.add(new StorageEntry(StoragePath.of(hivePath(table, i)), 1024L, Instant.EPOCH));
        }
        return entries;
    }

    private static PartitionMetadata hivePartitions(List<StorageEntry> entries) {
        Map<StoragePath, Map<String, Object>> perFile = new LinkedHashMap<>();
        for (StorageEntry e : entries) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("year", "2024");
            values.put("month", "07");
            values.put("day", "15");
            perFile.put(e.path(), values);
        }
        return new PartitionMetadata(PARTITION_COLUMNS, perFile);
    }

    public void testCompactedListingWeightIgnoresPerFilePartitionValues() {
        List<StorageEntry> entries = hiveEntries("sales", 4000);
        FileList withPartitions = GlobExpander.compact(
            GlobExpander.fileListOf(entries, "s3://warehouse/sales/**", hivePartitions(entries)),
            "s3://warehouse/sales/"
        );
        assertNotNull(withPartitions.partitionMetadata());
        assertEquals(4000, withPartitions.partitionMetadata().filePartitionValues().size());

        // The same compacted listing, differing only in whether it carries the per-file partition values.
        FileList withoutPartitions = GlobExpander.compact(
            GlobExpander.fileListOf(entries, "s3://warehouse/sales/**", new PartitionMetadata(PARTITION_COLUMNS, Map.of())),
            "s3://warehouse/sales/"
        );
        assertEquals(withPartitions.getClass(), withoutPartitions.getClass());
        assertThat(
            "a listing carrying one full path and three values per file must not weigh the same as one carrying none",
            withPartitions.estimatedBytes(),
            greaterThan(withoutPartitions.estimatedBytes())
        );
    }

    /** Control: the listing weigher is not inert — it does charge for the path text it encodes. */
    public void testCompactedListingWeightDoesCountPathText() {
        List<StorageEntry> shortPaths = hiveEntries("s", 4000);
        List<StorageEntry> longPaths = hiveEntries("s".repeat(200), 4000);
        FileList shortList = GlobExpander.compact(GlobExpander.fileListOf(shortPaths, "p", null), "s3://warehouse/");
        FileList longList = GlobExpander.compact(GlobExpander.fileListOf(longPaths, "p", null), "s3://warehouse/");
        assertThat(longList.estimatedBytes(), greaterThan(shortList.estimatedBytes()));
    }

    public void testListingCacheRetainsManyTimesItsBudgetInPartitionValues() throws Exception {
        Settings settings = Settings.builder().put("esql.external.cache.size", "2mb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            int listings = 5;
            int filesPerListing = 4000;
            long countedTotal = 0;
            long pathCharsHeldInPartitionKeys = 0;
            for (int t = 0; t < listings; t++) {
                List<StorageEntry> entries = hiveEntries("table" + t, filesPerListing);
                FileList list = GlobExpander.compact(
                    GlobExpander.fileListOf(entries, "s3://warehouse/table" + t + "/**", hivePartitions(entries)),
                    "s3://warehouse/table" + t + "/"
                );
                countedTotal += list.estimatedBytes();
                for (StorageEntry e : entries) {
                    pathCharsHeldInPartitionKeys += e.path().toString().length();
                }
                ListingCacheKey key = ListingCacheKey.build("s3", "warehouse", "table" + t + "/**", Map.of(), "d" + t);
                cache.getOrComputeListing(key, k -> list);
            }
            Map<String, Object> stats = cache.usageStats();
            long listingBudget = (2L * 1024 * 1024) - (2L * 1024 * 1024) / 5 - (2L * 1024 * 1024) / 50;
            assertThat(
                "the partition maps alone hold ["
                    + pathCharsHeldInPartitionKeys
                    + "] path characters against a ["
                    + listingBudget
                    + "] byte budget, of which the cache charged ["
                    + countedTotal
                    + "], so the cache cannot still be holding all ["
                    + listings
                    + "] listings",
                (Integer) stats.get("listing_cache.count"),
                lessThan(listings)
            );
        }
    }
}
