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
import java.util.Locale;
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
            List.of(),
            List.of()
        );
    }

    /**
     * The weight is computed once at construction, because the shared {@code Cache} runs its weigher twice on
     * every hit that is not already at the LRU head ({@code Cache.promote} -> {@code relinkAtHead} ->
     * {@code unlink} subtracting it and {@code linkAtHead} adding it back). The failure mode of precomputing is
     * therefore a STALE weight, so what needs pinning is that the enrichment helper recomputes: a harvest grows
     * the metadata map by megabytes, and an entry still reporting its pre-harvest weight would let the store hold
     * far more than its budget while believing it was inside it.
     * <p>
     * No single line of the change reverts into that bug, because the helper builds a new entry through the
     * constructor and there is no path that carries a weight forward. What this gate catches is an
     * implementation that grows one - verified by injecting a carried weight rather than by reverting a line.
     */
    public void testEnrichmentRecomputesTheWeightRatherThanCarryingTheOldOne() {
        SchemaCacheEntry seeded = entryWithMin("s3://b/f.csv", "a");
        long seededWeight = seeded.estimatedBytes();

        Map<String, Object> harvested = new LinkedHashMap<>(seeded.safeMetadata());
        harvested.put("_stats.columns.c.max", "x".repeat(1_000_000));
        SchemaCacheEntry enriched = seeded.withSafeMetadata(harvested);

        assertThat(
            "an enriched entry must charge for the metadata it now holds, not for what it held when it was built",
            enriched.estimatedBytes(),
            greaterThan(seededWeight + 1_000_000)
        );
        assertThat("enrichment must not mutate the entry it was derived from", seeded.estimatedBytes(), equalTo(seededWeight));
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
            List.of(),
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
            List.of(),
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
            List.of(),
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
            List.of(),
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
            List.of(),
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
            List.of(),
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
                SchemaCacheKey key = SchemaCacheKey.build(
                    "s3://bucket/f" + i + ".csv",
                    1000L,
                    TestDatasetIdentities.identity(".csv", "", Map.of()),
                    false
                );
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
                        List.of(),
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
            SchemaCacheKey key = SchemaCacheKey.build(
                "s3://bucket/grow.csv",
                1000L,
                TestDatasetIdentities.identity(".csv", "", Map.of()),
                false
            );
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
            DatasetAggregateKey key = DatasetAggregateKey.of(
                "file:///tmp/warm-fold/*.ndjson",
                new FileSetFingerprint(11, 22),
                TestDatasetIdentities.identity("ndjson", "", Map.of("format", "ndjson"))
            );
            cache.putDatasetAggregate(key, 828_090L);
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
                SchemaCacheKey key = SchemaCacheKey.build(
                    "s3://bucket/mid" + i + ".csv",
                    1000L,
                    TestDatasetIdentities.identity(".csv", "", Map.of()),
                    false
                );
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
                    List.of(),
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

    /**
     * The eviction count, exactly, rather than "more than none".
     * <p>
     * A budget that evicts is not the same as a budget that evicts <em>proportionately</em>. An LRU that discards
     * more than it needs to make room turns a cache one entry over budget into a cache that keeps re-reading the
     * siblings it just dropped, and every assertion of the form {@code evictions > 0} is green for both. So the
     * fixture makes every entry weigh the same — a zero-padded path and constant-length extrema, since the weigher
     * charges for both — fills the budget exactly, and then adds one.
     * <p>
     * One entry over a budget of equal-weight entries costs exactly one of them.
     */
    public void testOneEntryOverBudgetEvictsExactlyOneEntry() throws Exception {
        Settings settings = Settings.builder().put("esql.external.cache.size", "2mb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            long schemaBudget = (Long) cache.usageStats().get("schema_budget_bytes");
            long maxEntry = (Long) cache.usageStats().get("schema_max_entry_bytes");

            int valueChars = 512;
            while (equalWeightEntry(0, valueChars).estimatedBytes() > maxEntry / 2 && valueChars > 16) {
                valueChars /= 2;
            }
            long perEntry = equalWeightEntry(0, valueChars).estimatedBytes();
            assertThat("every fixture entry must weigh the same", equalWeightEntry(7, valueChars).estimatedBytes(), equalTo(perEntry));

            int capacity = (int) (schemaBudget / perEntry);
            assertThat("the budget must hold several entries for this to say anything", capacity, greaterThan(2));

            for (int i = 0; i < capacity; i++) {
                cache.putSchema(equalWeightKey(i), equalWeightEntry(i, valueChars));
            }
            assertThat(
                "entries that fit the budget exactly must not evict",
                (Long) cache.usageStats().get("schema_cache.evictions"),
                equalTo(0L)
            );

            cache.putSchema(equalWeightKey(capacity), equalWeightEntry(capacity, valueChars));
            assertThat(
                "one entry over a budget of equal-weight entries must cost exactly one of them, not a swathe of them",
                (Long) cache.usageStats().get("schema_cache.evictions"),
                equalTo(1L)
            );
            assertThat(retainedSchemaWeight(cache), lessThanOrEqualTo(schemaBudget));
        }
    }

    /** Zero-padded so every path is the same length, because the weigher charges for the path. */
    private static SchemaCacheKey equalWeightKey(int i) {
        return SchemaCacheKey.build(
            String.format(Locale.ROOT, "s3://bucket/eq%06d.csv", i),
            1000L,
            TestDatasetIdentities.identity(".csv", "", Map.of()),
            false
        );
    }

    /** Identical in weight for every {@code i}: constant-length path, column name, and extrema. */
    private static SchemaCacheEntry equalWeightEntry(int i, int valueChars) {
        String path = String.format(Locale.ROOT, "s3://bucket/eq%06d.csv", i);
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put(ExternalStats.MTIME_MILLIS_KEY, 1000L);
        meta.put("_stats.row_count", 10L);
        meta.put("_stats.columns.c.min", "a".repeat(valueChars));
        meta.put("_stats.columns.c.max", "b".repeat(valueChars));
        return new SchemaCacheEntry(
            new String[] { "c" },
            new DataType[] { DataType.KEYWORD },
            new Nullability[] { Nullability.TRUE },
            new boolean[] { false },
            "csv",
            path,
            meta,
            Map.of(),
            List.of(),
            List.of()
        );
    }

    /**
     * {@code weight_bytes} must be the store's real occupancy, because it is the figure the budget is enforced
     * against and the counts beside it cannot stand in for it - one many-striped file's entry can outweigh
     * thousands of narrow ones. Pinned against the independent sum over the retained entries, so a stat that
     * reported a count, a budget or a stale total would disagree with the oracle.
     */
    public void testReportedWeightAgreesWithWhatTheStoreActuallyHolds() {
        Settings settings = Settings.builder().put("esql.external.cache.size", "10mb").build();
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(settings)) {
            assertThat("an empty store holds nothing", cache.usageStats().get("schema_cache.weight_bytes"), equalTo(0L));

            for (int i = 0; i < 4; i++) {
                SchemaCacheKey key = SchemaCacheKey.build(
                    "s3://bucket/f" + i + ".csv",
                    1000L + i,
                    TestDatasetIdentities.identity("csv", "identity", Map.of()),
                    false
                );
                cache.putSchema(key, entryWithMin("s3://bucket/f" + i + ".csv", "v".repeat(1000 * (i + 1))));
            }

            assertThat(
                "the reported weight must equal the sum of the weights of the entries retained",
                cache.usageStats().get("schema_cache.weight_bytes"),
                equalTo(retainedSchemaWeight(cache))
            );
            assertThat("and it must be non-trivial once four entries are held", retainedSchemaWeight(cache), greaterThan(4000L));
        }
    }

    private static long retainedSchemaWeight(ExternalSourceCacheService cache) {
        long[] total = { 0L };
        cache.schemaCache().forEach((key, entry) -> total[0] += entry.estimatedBytes());
        return total[0];
    }
}
