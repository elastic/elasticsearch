/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceStatisticsSerializerTests extends ESTestCase {

    public void testMergeStatisticsEmpty() {
        assertNull(SourceStatisticsSerializer.mergeStatistics(null));
        assertNull(SourceStatisticsSerializer.mergeStatistics(List.of()));
    }

    public void testMergeStatisticsSingleSplit() {
        Map<String, Object> stats = Map.of(
            SourceStatisticsSerializer.STATS_ROW_COUNT,
            100L,
            SourceStatisticsSerializer.STATS_SIZE_BYTES,
            5000L
        );
        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(stats));
        assertNotNull(result);
        assertEquals(100L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    public void testMergeStatisticsSingleSplitWithoutRowCount() {
        Map<String, Object> stats = Map.of(SourceStatisticsSerializer.STATS_SIZE_BYTES, 5000L);
        assertNull(SourceStatisticsSerializer.mergeStatistics(List.of(stats)));
    }

    public void testMergeStatisticsMultipleSplits() {
        Map<String, Object> s1 = new HashMap<>();
        s1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        s1.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, 5000L);
        s1.put("_stats.columns.age.null_count", 10L);
        s1.put("_stats.columns.age.min", 18);
        s1.put("_stats.columns.age.max", 50);

        Map<String, Object> s2 = new HashMap<>();
        s2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        s2.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, 8000L);
        s2.put("_stats.columns.age.null_count", 5L);
        s2.put("_stats.columns.age.min", 22);
        s2.put("_stats.columns.age.max", 65);

        Map<String, Object> s3 = new HashMap<>();
        s3.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 300L);
        s3.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, 12000L);
        s3.put("_stats.columns.age.null_count", 0L);
        s3.put("_stats.columns.age.min", 25);
        s3.put("_stats.columns.age.max", 40);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2, s3));
        assertNotNull(result);
        assertEquals(600L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(25000L, result.get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertEquals(15L, result.get("_stats.columns.age.null_count"));
        assertEquals(18, result.get("_stats.columns.age.min"));
        assertEquals(65, result.get("_stats.columns.age.max"));
    }

    public void testMergeStatisticsMissingSplitReturnsNull() {
        Map<String, Object> s1 = Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        Map<String, Object> s2 = Map.of(SourceStatisticsSerializer.STATS_SIZE_BYTES, 5000L);

        assertNull(SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2)));
    }

    public void testMergeStatisticsNullSplitReturnsNull() {
        Map<String, Object> s1 = Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(s1);
        list.add(null);

        assertNull(SourceStatisticsSerializer.mergeStatistics(list));
    }

    public void testColumnSizeBytesRoundTrip() {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        meta.put(SourceStatisticsSerializer.columnSizeBytesKey("age"), 50000L);
        meta.put(SourceStatisticsSerializer.columnSizeBytesKey("name"), 120000L);

        assertEquals(Long.valueOf(50000L), SourceStatisticsSerializer.extractColumnSizeBytes(meta, "age"));
        assertEquals(Long.valueOf(120000L), SourceStatisticsSerializer.extractColumnSizeBytes(meta, "name"));
        assertNull(SourceStatisticsSerializer.extractColumnSizeBytes(meta, "missing"));
        assertNull(SourceStatisticsSerializer.extractColumnSizeBytes(null, "age"));
    }

    public void testMergeStatisticsCrossTypeIntegerLong() {
        Map<String, Object> s1 = new HashMap<>();
        s1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        s1.put(SourceStatisticsSerializer.columnMinKey("price"), 10);
        s1.put(SourceStatisticsSerializer.columnMaxKey("price"), 50);

        Map<String, Object> s2 = new HashMap<>();
        s2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        s2.put(SourceStatisticsSerializer.columnMinKey("price"), 5L);
        s2.put(SourceStatisticsSerializer.columnMaxKey("price"), 60L);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2));
        assertNotNull(result);
        assertEquals(300L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(5L, result.get(SourceStatisticsSerializer.columnMinKey("price")));
        assertEquals(60L, result.get(SourceStatisticsSerializer.columnMaxKey("price")));
    }

    public void testMergeStatisticsCrossTypeIntegerDouble() {
        Map<String, Object> s1 = new HashMap<>();
        s1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        s1.put(SourceStatisticsSerializer.columnMinKey("score"), 3);
        s1.put(SourceStatisticsSerializer.columnMaxKey("score"), 80);

        Map<String, Object> s2 = new HashMap<>();
        s2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        s2.put(SourceStatisticsSerializer.columnMinKey("score"), 1.5);
        s2.put(SourceStatisticsSerializer.columnMaxKey("score"), 99.9);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2));
        assertNotNull(result);
        assertEquals(1.5, result.get(SourceStatisticsSerializer.columnMinKey("score")));
        assertEquals(99.9, result.get(SourceStatisticsSerializer.columnMaxKey("score")));
    }

    public void testMergeStatisticsCrossTypeLongDoubleIncompatibleClearsStats() {
        Map<String, Object> s1 = new HashMap<>();
        s1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        s1.put(SourceStatisticsSerializer.columnMinKey("val"), 10L);
        s1.put(SourceStatisticsSerializer.columnMaxKey("val"), 50L);

        Map<String, Object> s2 = new HashMap<>();
        s2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        s2.put(SourceStatisticsSerializer.columnMinKey("val"), 5.0);
        s2.put(SourceStatisticsSerializer.columnMaxKey("val"), 60.0);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2));
        assertNotNull(result);
        assertNull("incompatible types should clear min entry", result.get(SourceStatisticsSerializer.columnMinKey("val")));
        assertNull("incompatible types should clear max entry", result.get(SourceStatisticsSerializer.columnMaxKey("val")));
    }

    public void testMergeStatisticsCrossTypeLongDoubleIncompatibleReversed() {
        Map<String, Object> s1 = new HashMap<>();
        s1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        s1.put(SourceStatisticsSerializer.columnMinKey("val"), 5.0);
        s1.put(SourceStatisticsSerializer.columnMaxKey("val"), 60.0);

        Map<String, Object> s2 = new HashMap<>();
        s2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        s2.put(SourceStatisticsSerializer.columnMinKey("val"), 10L);
        s2.put(SourceStatisticsSerializer.columnMaxKey("val"), 50L);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2));
        assertNotNull(result);
        assertNull("incompatible types should clear min regardless of order", result.get(SourceStatisticsSerializer.columnMinKey("val")));
        assertNull("incompatible types should clear max regardless of order", result.get(SourceStatisticsSerializer.columnMaxKey("val")));
    }

    public void testMergeStatisticsIncompatibleStaysPoisonedWithThirdSplit() {
        Map<String, Object> s1 = new HashMap<>();
        s1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        s1.put(SourceStatisticsSerializer.columnMinKey("val"), 10L);
        s1.put(SourceStatisticsSerializer.columnMaxKey("val"), 50L);

        Map<String, Object> s2 = new HashMap<>();
        s2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        s2.put(SourceStatisticsSerializer.columnMinKey("val"), 5.0);
        s2.put(SourceStatisticsSerializer.columnMaxKey("val"), 60.0);

        Map<String, Object> s3 = new HashMap<>();
        s3.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 300L);
        s3.put(SourceStatisticsSerializer.columnMinKey("val"), 1L);
        s3.put(SourceStatisticsSerializer.columnMaxKey("val"), 100L);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2, s3));
        assertNotNull(result);
        assertEquals(600L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertNull("once poisoned, min must stay cleared", result.get(SourceStatisticsSerializer.columnMinKey("val")));
        assertNull("once poisoned, max must stay cleared", result.get(SourceStatisticsSerializer.columnMaxKey("val")));
    }

    public void testMergeStatisticsAddsImplicitNullsForAbsentColumns() {
        // File A: 100 rows, has bonus with 5 explicit nulls.
        Map<String, Object> a = new HashMap<>();
        a.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        a.put(SourceStatisticsSerializer.columnNullCountKey("bonus"), 5L);
        a.put(SourceStatisticsSerializer.columnMinKey("bonus"), 10);
        a.put(SourceStatisticsSerializer.columnMaxKey("bonus"), 50);
        a.put(SourceStatisticsSerializer.columnSizeBytesKey("bonus"), 800L);

        // File B: 200 rows, no bonus column at all (no _stats.columns.bonus.* keys).
        Map<String, Object> b = new HashMap<>();
        b.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);

        // File C: 700 rows, has bonus with 10 explicit nulls.
        Map<String, Object> c = new HashMap<>();
        c.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 700L);
        c.put(SourceStatisticsSerializer.columnNullCountKey("bonus"), 10L);
        c.put(SourceStatisticsSerializer.columnMinKey("bonus"), 20);
        c.put(SourceStatisticsSerializer.columnMaxKey("bonus"), 70);
        c.put(SourceStatisticsSerializer.columnSizeBytesKey("bonus"), 5600L);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(a, b, c));
        assertNotNull(result);
        assertEquals(1000L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        // 5 (A explicit) + 10 (C explicit) + 200 (B implicit, every row counts as null) = 215.
        assertEquals(215L, result.get(SourceStatisticsSerializer.columnNullCountKey("bonus")));
        // Min/max/size_bytes only consider files where the column is present.
        assertEquals(10, result.get(SourceStatisticsSerializer.columnMinKey("bonus")));
        assertEquals(70, result.get(SourceStatisticsSerializer.columnMaxKey("bonus")));
        assertEquals(6400L, result.get(SourceStatisticsSerializer.columnSizeBytesKey("bonus")));
    }

    public void testMergeStatisticsImplicitNullsAcrossMultipleColumns() {
        // File A has only "x"; file B has only "y". Each file's row count flows to the other column.
        Map<String, Object> a = new HashMap<>();
        a.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        a.put(SourceStatisticsSerializer.columnNullCountKey("x"), 0L);

        Map<String, Object> b = new HashMap<>();
        b.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 250L);
        b.put(SourceStatisticsSerializer.columnNullCountKey("y"), 7L);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(a, b));
        assertNotNull(result);
        assertEquals(350L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        // x absent in B (250 rows): 0 + 250 = 250.
        assertEquals(250L, result.get(SourceStatisticsSerializer.columnNullCountKey("x")));
        // y absent in A (100 rows): 7 + 100 = 107.
        assertEquals(107L, result.get(SourceStatisticsSerializer.columnNullCountKey("y")));
    }

    public void testMergeStatisticsPoisonsNullCountWhenAnyFilePresentsColumnWithoutNullCount() {
        // File A: bonus with full stats.
        Map<String, Object> a = new HashMap<>();
        a.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        a.put(SourceStatisticsSerializer.columnNullCountKey("bonus"), 5L);
        a.put(SourceStatisticsSerializer.columnMinKey("bonus"), 10);
        a.put(SourceStatisticsSerializer.columnMaxKey("bonus"), 50);
        a.put(SourceStatisticsSerializer.columnSizeBytesKey("bonus"), 800L);

        // File B: bonus is physically present (size_bytes set) but reader produced no null_count
        // (rare Parquet path with stats disabled). We must not invent a count for those rows.
        Map<String, Object> b = new HashMap<>();
        b.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        b.put(SourceStatisticsSerializer.columnSizeBytesKey("bonus"), 1600L);

        // File C: bonus absent entirely.
        Map<String, Object> c = new HashMap<>();
        c.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 700L);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(a, b, c));
        assertNotNull(result);
        assertEquals(1000L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertFalse(
            "null_count must be dropped when any present file lacks a null_count value",
            result.containsKey(SourceStatisticsSerializer.columnNullCountKey("bonus"))
        );
        // Min/max/size_bytes are still informative from file A (and B for size_bytes).
        assertEquals(10, result.get(SourceStatisticsSerializer.columnMinKey("bonus")));
        assertEquals(50, result.get(SourceStatisticsSerializer.columnMaxKey("bonus")));
        assertEquals(2400L, result.get(SourceStatisticsSerializer.columnSizeBytesKey("bonus")));
    }

    public void testMergeStatistics_sumsSizeBytes() {
        Map<String, Object> s1 = new HashMap<>();
        s1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        s1.put(SourceStatisticsSerializer.columnSizeBytesKey("age"), 5000L);
        s1.put(SourceStatisticsSerializer.columnSizeBytesKey("name"), 10000L);

        Map<String, Object> s2 = new HashMap<>();
        s2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 200L);
        s2.put(SourceStatisticsSerializer.columnSizeBytesKey("age"), 8000L);
        s2.put(SourceStatisticsSerializer.columnSizeBytesKey("name"), 15000L);

        Map<String, Object> result = SourceStatisticsSerializer.mergeStatistics(List.of(s1, s2));
        assertNotNull(result);
        assertEquals(300L, result.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(13000L, result.get(SourceStatisticsSerializer.columnSizeBytesKey("age")));
        assertEquals(25000L, result.get(SourceStatisticsSerializer.columnSizeBytesKey("name")));
    }

    /**
     * Reproduces the production double-merge of the warm multi-file path: each file's whole-file map is
     * itself the result of {@code mergeStatistics} over that file's stripes, carrying the non-{@code _stats.columns.*}
     * keying fields (mtime, fingerprint, per-stripe entry maps). The dataset-wide merge then folds the N
     * per-file maps, and the optimizer reads the result through {@link SplitStats#of}. Asserts the column
     * min/max survives both hops and the {@link SplitStats} round-trip across many files (the single-file
     * fold ITs never exercise this).
     */
    public void testDoubleMergeManyFilesPreservesColumnMinMax() {
        int fileCount = 25;
        int stripesPerFile = 4;
        long rowsPerStripe = 10_000L;
        List<Map<String, Object>> perFileMaps = new ArrayList<>(fileCount);
        long globalRow = 0;
        for (int f = 0; f < fileCount; f++) {
            List<Map<String, Object>> stripeMaps = new ArrayList<>(stripesPerFile);
            for (int s = 0; s < stripesPerFile; s++) {
                Map<String, Object> stripe = new HashMap<>();
                stripe.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowsPerStripe);
                stripe.put(SourceStatisticsSerializer.columnNullCountKey("EventDate"), 0L);
                stripe.put(SourceStatisticsSerializer.columnMinKey("EventDate"), globalRow);
                stripe.put(SourceStatisticsSerializer.columnMaxKey("EventDate"), globalRow + rowsPerStripe - 1);
                globalRow += rowsPerStripe;
                stripeMaps.add(stripe);
            }
            Map<String, Object> wholeFile = SourceStatisticsSerializer.mergeStatistics(stripeMaps);
            assertNotNull(wholeFile);
            // Re-attach the keying fields the real cache entry carries (these must NOT corrupt the merge).
            wholeFile.put("_stats.file_mtime_millis", 1_700_000_000_000L + f);
            wholeFile.put("_stats.config_fingerprint", "fp");
            wholeFile.put("_stats.stripe.0", Map.of("inner", "ignored"));
            perFileMaps.add(wholeFile);
        }
        long totalRows = globalRow;

        Map<String, Object> dataset = SourceStatisticsSerializer.mergeStatistics(perFileMaps);
        assertNotNull(dataset);
        assertEquals(totalRows, ((Number) dataset.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());
        assertEquals(0L, dataset.get(SourceStatisticsSerializer.columnMinKey("EventDate")));
        assertEquals(totalRows - 1, dataset.get(SourceStatisticsSerializer.columnMaxKey("EventDate")));

        // The optimizer reads the merged dataset stats through SplitStats.of; min/max must survive that hop.
        SplitStats stats = SplitStats.of(dataset);
        assertNotNull(stats);
        assertEquals(totalRows, stats.rowCount());
        assertEquals(0L, stats.columnMin("EventDate"));
        assertEquals(totalRows - 1, stats.columnMax("EventDate"));
    }

    public void testMergeStatisticsTextDropsColumnNotObservedInEveryFile() {
        // Text partial-harvest (implicitNulls=false): file A harvested value's min/max, file B never
        // observed value (e.g. a COUNT(*) scan). The dataset cannot serve a correct MIN(value), so
        // value is dropped entirely -> the consumer safe-misses rather than serving A's subset min.
        Map<String, Object> withValue = new HashMap<>();
        withValue.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 20_000L);
        withValue.put(SourceStatisticsSerializer.columnMinKey("value"), 1_000_000L);
        withValue.put(SourceStatisticsSerializer.columnMaxKey("value"), 1_019_999L);

        Map<String, Object> countOnly = new HashMap<>();
        countOnly.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 20_000L);

        Map<String, Object> text = SourceStatisticsSerializer.mergeStatistics(List.of(withValue, countOnly), false);
        assertNotNull(text);
        assertEquals("COUNT(*) stays warm", 40_000L, ((Number) text.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());
        assertFalse("value unobserved in some file -> dropped", text.containsKey(SourceStatisticsSerializer.columnMinKey("value")));
        assertFalse(text.containsKey(SourceStatisticsSerializer.columnMaxKey("value")));

        // Same inputs under footer semantics (implicitNulls=true) keep value's extremum: an absent
        // column is all-null, which does not move the min/max.
        Map<String, Object> footer = SourceStatisticsSerializer.mergeStatistics(List.of(withValue, countOnly), true);
        assertEquals(1_000_000L, footer.get(SourceStatisticsSerializer.columnMinKey("value")));
    }
}
