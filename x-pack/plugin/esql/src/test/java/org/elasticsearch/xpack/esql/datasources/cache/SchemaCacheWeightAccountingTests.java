/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * What the schema cache charges for the objects it retains — acceptance coverage for long keyword/text
 * extrema under-weigh ({@code elastic/esql-planning#2075}).
 */
public class SchemaCacheWeightAccountingTests extends ESTestCase {

    private static SchemaCacheEntry entryWithMin(String path, Object min) {
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

    public void testSchemaEntryWeightChargesTheSizeOfAStoredColumnExtremum() {
        SchemaCacheEntry small = entryWithMin("s3://b/f.csv", "a");
        SchemaCacheEntry large = entryWithMin("s3://b/f.csv", "x".repeat(1_000_000));
        assertThat(
            "an entry holding a one-megabyte column extremum must not weigh the same as one holding a single character",
            large.estimatedBytes(),
            greaterThan(small.estimatedBytes())
        );
    }

    public void testSchemaEntryWeightChargesTheSizeOfAKeywordExtremumHarvestedFromText() {
        // A text harvest stores a keyword extremum as a BytesRef; a columnar footer stores it as a String.
        SchemaCacheEntry small = entryWithMin("s3://b/f.csv", new BytesRef("a"));
        SchemaCacheEntry large = entryWithMin("s3://b/f.csv", new BytesRef("x".repeat(1_000_000)));
        assertThat(
            "an entry holding a one-megabyte keyword extremum must not weigh the same as one holding a single byte",
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

    public void testSchemaEntryWeightChargesConnectorConfigValues() {
        SchemaCacheEntry small = new SchemaCacheEntry(
            new String[] { "c" },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            "s3://b/f.csv",
            Map.of(),
            Map.of("k", "a"),
            0L,
            List.of()
        );
        SchemaCacheEntry large = new SchemaCacheEntry(
            new String[] { "c" },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            "s3://b/f.csv",
            Map.of(),
            Map.of("k", "x".repeat(50_000)),
            0L,
            List.of()
        );
        assertThat(large.estimatedBytes(), greaterThan(small.estimatedBytes()));
    }

    public void testSchemaEntryWeightCountsNestedStripeExtrema() {
        Map<String, Object> emptyStripes = new LinkedHashMap<>();
        emptyStripes.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        emptyStripes.put("_stats.row_count", 10L);

        Map<String, Object> stripeStats = new LinkedHashMap<>();
        stripeStats.put("_stats.columns.c.min", "x".repeat(50_000));
        stripeStats.put("_stats.columns.c.max", "y".repeat(50_000));
        Map<String, Object> withStripes = new LinkedHashMap<>(emptyStripes);
        withStripes.put(ExternalStats.STRIPE_ENTRY_PREFIX + "0", stripeStats);

        SchemaCacheEntry bare = new SchemaCacheEntry(
            new String[] { "c" },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            "s3://b/f.csv",
            emptyStripes,
            Map.of(),
            0L,
            List.of()
        );
        SchemaCacheEntry striped = new SchemaCacheEntry(
            new String[] { "c" },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            "s3://b/f.csv",
            withStripes,
            Map.of(),
            0L,
            List.of()
        );
        assertThat(
            "committed per-stripe extrema must raise the entry weight, not only top-level statistics",
            striped.estimatedBytes(),
            greaterThan(bare.estimatedBytes())
        );
    }

    /**
     * Megabyte-wide extrema exceed the per-entry ceiling (a quarter of the schema slice), so they are
     * refused rather than retained. Retained weight must stay inside the schema budget — the spirit of the
     * issue acceptance arm, adapted for refuse-before-put.
     */
    public void testSchemaCacheDoesNotRetainManyTimesItsBudgetWhenExtremaAreLarge() throws Exception {
        Settings settings = Settings.builder().put("esql.external.cache.size", "2mb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            long schemaBudget = (Long) cache.usageStats().get("schema_budget_bytes");
            int entries = 20;
            int valueChars = 1_000_000;
            for (int i = 0; i < entries; i++) {
                String min = "a" + i + "-" + "x".repeat(valueChars);
                String max = "b" + i + "-" + "y".repeat(valueChars);
                SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/f" + i + ".csv", 1000L, ".csv", Map.of());
                Map<String, Object> meta = new LinkedHashMap<>();
                meta.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
                meta.put("_stats.row_count", 10L);
                meta.put("_stats.columns.c.min", min);
                meta.put("_stats.columns.c.max", max);
                cache.putSchema(
                    key,
                    new SchemaCacheEntry(
                        new String[] { "c" },
                        new DataType[] { DataType.KEYWORD },
                        new Nullability[] { Nullability.TRUE },
                        new boolean[] { false },
                        "csv",
                        "s3://bucket/f" + i + ".csv",
                        meta,
                        Map.of(),
                        0L,
                        List.of()
                    )
                );
            }
            long retained = retainedSchemaWeight(cache);
            assertThat(
                "megabyte-wide extrema must not leave the schema cache holding many times its [" + schemaBudget + "] byte budget",
                retained,
                lessThanOrEqualTo(schemaBudget)
            );
            assertThat(
                "each megabyte-extrema entry exceeds the per-entry ceiling, so none should be retained",
                (Integer) cache.usageStats().get("schema_cache.count"),
                equalTo(0)
            );
        }
    }

    /** An enrichment that grows past the ceiling must drop the key, not leave a stale smaller entry. */
    public void testOversizePutInvalidatesExistingSchemaEntry() throws Exception {
        Settings settings = Settings.builder().put("esql.external.cache.size", "2mb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/grow.csv", 1000L, ".csv", Map.of());
            cache.putSchema(key, entryWithMin("s3://bucket/grow.csv", "a"));
            assertThat(cache.getSchemaIfPresent(key), notNullValue());
            cache.putSchema(key, entryWithMin("s3://bucket/grow.csv", "x".repeat(1_000_000)));
            assertThat(
                "refusing an oversized replacement must invalidate the previous entry so warm stats cannot go stale",
                cache.getSchemaIfPresent(key),
                nullValue()
            );
        }
    }

    /**
     * The warm many-file fold IT uses a 48kb total budget so LRU pressure is deterministic. Under that
     * budget a raw quarter-of-slice ceiling refuses ordinary dataset-aggregate and schema rows; the
     * floor must keep them admissible while still refusing megabyte extrema against a production budget.
     */
    public void testTinyBudgetCeilingStillAdmitsOrdinaryDatasetAggregate() throws Exception {
        Settings settings = Settings.builder().put("esql.external.cache.size", "48kb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            long daCeiling = (Long) cache.usageStats().get("dataset_aggregate_max_entry_bytes");
            SchemaCacheKey key = SchemaCacheKey.forDatasetAggregate(
                "file:///tmp/warm-fold/*.ndjson",
                new FileSetFingerprint(11, 22),
                "ndjson",
                Map.of("format", "ndjson")
            );
            cache.putDatasetAggregate(key, 828_090L, "ndjson", "file:///tmp/warm-fold/*.ndjson");
            assertThat(
                "48kb total budget must still retain the dataset-aggregate row-count entry (ceiling=" + daCeiling + ")",
                cache.getDatasetAggregate(key),
                notNullValue()
            );
            assertThat((Integer) cache.usageStats().get("dataset_aggregate_cache.count"), equalTo(1));
        }
    }

    /**
     * Entries that individually fit under the per-entry ceiling but together exceed the schema budget must
     * drive LRU evictions — proving the weigher (not only the ceiling) bounds retention.
     */
    public void testSchemaCacheEvictsWhenAdmittedExtremaExceedBudget() throws Exception {
        Settings settings = Settings.builder().put("esql.external.cache.size", "2mb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            long schemaBudget = (Long) cache.usageStats().get("schema_budget_bytes");
            long maxEntry = (Long) cache.usageStats().get("schema_max_entry_bytes");
            // Target ~half the ceiling per entry so many admit, then overflow the budget.
            int valueChars = 8_000;
            SchemaCacheEntry probe = entryWithMin("s3://bucket/probe.csv", "x".repeat(valueChars));
            while (probe.estimatedBytes() > maxEntry / 2 && valueChars > 64) {
                valueChars /= 2;
                probe = entryWithMin("s3://bucket/probe.csv", "x".repeat(valueChars));
            }
            long perEntry = probe.estimatedBytes();
            assertThat("fixture entry must fit under the per-entry ceiling", perEntry, lessThanOrEqualTo(maxEntry));
            int entries = (int) (schemaBudget / Math.max(1L, perEntry)) + 8;
            for (int i = 0; i < entries; i++) {
                String min = "a" + i + "-" + "x".repeat(valueChars);
                String max = "b" + i + "-" + "y".repeat(valueChars);
                SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/mid" + i + ".csv", 1000L, ".csv", Map.of());
                Map<String, Object> meta = new LinkedHashMap<>();
                meta.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
                meta.put("_stats.row_count", 10L);
                meta.put("_stats.columns.c.min", min);
                meta.put("_stats.columns.c.max", max);
                SchemaCacheEntry entry = new SchemaCacheEntry(
                    new String[] { "c" },
                    new DataType[] { DataType.KEYWORD },
                    new Nullability[] { Nullability.TRUE },
                    new boolean[] { false },
                    "csv",
                    "s3://bucket/mid" + i + ".csv",
                    meta,
                    Map.of(),
                    0L,
                    List.of()
                );
                assertThat(entry.estimatedBytes(), lessThanOrEqualTo(maxEntry));
                cache.putSchema(key, entry);
            }
            Map<String, Object> stats = cache.usageStats();
            assertThat(
                "mid-size extrema that fit the ceiling but overflow the schema budget must evict",
                (Long) stats.get("schema_cache.evictions"),
                greaterThan(0L)
            );
            assertThat(retainedSchemaWeight(cache), lessThanOrEqualTo(schemaBudget));
        }
    }

    private static long retainedSchemaWeight(ExternalSourceCacheService cache) {
        long[] total = { 0L };
        cache.schemaCache().forEach((key, entry) -> total[0] += entry.estimatedBytes());
        return total[0];
    }
}
