/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class MergedSplitStatsTests extends ESTestCase {

    // -- rowCount --

    public void testRowCountSumsChildren() {
        SplitStats a = stats(100, -1);
        SplitStats b = stats(200, -1);
        SplitStats c = stats(300, -1);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b, c));
        assertEquals(600, merged.rowCount());
    }

    public void testRowCountSingleChild() {
        MergedSplitStats merged = new MergedSplitStats(List.of(stats(42, -1)));
        assertEquals(42, merged.rowCount());
    }

    // -- sizeInBytes --

    public void testSizeInBytesSumsWhenAllKnown() {
        SplitStats a = stats(100, 1000);
        SplitStats b = stats(200, 2000);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(3000, merged.sizeInBytes());
    }

    public void testSizeInBytesReturnsMinusOneWhenAnyUnknown() {
        SplitStats a = stats(100, 1000);
        SplitStats b = stats(200, -1);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(-1, merged.sizeInBytes());
    }

    public void testSizeInBytesAllUnknown() {
        SplitStats a = stats(100, -1);
        SplitStats b = stats(200, -1);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(-1, merged.sizeInBytes());
    }

    // -- columnNullCount --

    public void testColumnNullCountSumsChildren() {
        SplitStats a = splitStatsWithColumn("age", 5L, 10, 90, 400);
        SplitStats b = splitStatsWithColumn("age", 3L, 15, 80, 800);
        SplitStats c = splitStatsWithColumn("age", 2L, 20, 70, 1200);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b, c));
        assertEquals(10, merged.columnNullCount("age"));
    }

    public void testColumnNullCountReturnsMinusOneWhenAnyUnknown() {
        SplitStats a = splitStatsWithColumn("age", -1L, 10, 90, 400);
        SplitStats b = splitStatsWithColumn("age", 3L, 15, 80, 800);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(-1, merged.columnNullCount("age"));
    }

    public void testColumnNullCountReturnsRowCountForMissingColumn() {
        // Under the SPI's "implicit nulls" contract, a column absent from a child
        // contributes that child's full row count as implicit nulls (not -1).
        SplitStats a = splitStatsWithColumn("age", 5L, 10, 90, 400);
        MergedSplitStats merged = new MergedSplitStats(List.of(a));
        assertEquals("absent column contributes rowCount, not -1", a.rowCount(), merged.columnNullCount("name"));
    }

    public void testColumnNullCountWithAbsentChildrenSumsCorrectly() {
        // Child A: 100 rows, has bonus with 5 explicit nulls.
        // Child B: 200 rows, no bonus column at all -> contributes 200 implicit nulls.
        // Sum: 5 + 200 = 205.
        SplitStats a = splitStatsRowCountWithColumn(100, "bonus", 5L, 10, 90, 400);
        SplitStats b = splitStatsRowCountWithColumn(200, "age", 0L, 1, 2, 100);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(205L, merged.columnNullCount("bonus"));
    }

    public void testColumnMinSkipsChildrenWithoutColumnValue() {
        // Child A: has bonus, min=10. Child B: no bonus. Child C: has bonus, min=20.
        // Skip B rather than poison; result is 10.
        SplitStats a = splitStatsRowCountWithColumn(100, "bonus", 0L, 10, 90, 400);
        SplitStats b = splitStatsRowCountWithColumn(50, "age", 0L, 1, 2, 100);
        SplitStats c = splitStatsRowCountWithColumn(100, "bonus", 0L, 20, 80, 400);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b, c));
        assertEquals(10, merged.columnMin("bonus"));
        assertEquals(90, merged.columnMax("bonus"));
    }

    public void testColumnMinSkipsAllNullChildren() {
        // Child A: has bonus, min=10 (some rows non-null). Child B: has bonus but every row is null.
        // Child C: has bonus, min=20. B contributes no candidate value and must be skipped.
        SplitStats a = splitStatsRowCountWithColumn(100, "bonus", 5L, 10, 90, 400);
        // B has bonus column but nullCount == rowCount (all rows null) and no min/max stat.
        SplitStats.Builder bb = new SplitStats.Builder().rowCount(50);
        bb.addColumn("bonus", 50L, null, null, 200);
        SplitStats b = bb.build();
        SplitStats c = splitStatsRowCountWithColumn(100, "bonus", 0L, 20, 80, 400);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b, c));
        assertEquals(10, merged.columnMin("bonus"));
        assertEquals(90, merged.columnMax("bonus"));
    }

    public void testColumnMinMaxUsesChildValueWhenNullCountUnknownButMinMaxPresent() {
        // Regression for the multi-FILE warm MIN/MAX short-circuit: a per-file SplitStats can legitimately
        // carry a known min/max for a column while its null_count is unknown (-1). This happens when the
        // file's own multi-stripe fold poison-dropped the null_count (a stripe that presented the column
        // without a null_count value) but retained the min/max — SourceStatisticsSerializer.mergeStatistics
        // drops only the null_count entry, never the min/max. Such a child contributes a valid extremum
        // candidate: the min stat is the minimum of the column's non-null values regardless of how many
        // nulls there are. Poisoning the whole dataset min/max here is what made warm MIN/MAX full-scan a
        // multi-file glob while COUNT(*) (which never reads null_count for COUNT(*)) short-circuited.
        SplitStats a = splitStatsRowCountWithColumn(100, "EventDate", 0L, 10, 90, 400);
        // B: column present with a real min/max but null_count unknown (-1).
        SplitStats.Builder bb = new SplitStats.Builder().rowCount(50);
        bb.addColumn("EventDate", -1L, 5, 95, 200);
        SplitStats b = bb.build();
        SplitStats c = splitStatsRowCountWithColumn(100, "EventDate", 0L, 20, 80, 400);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b, c));
        assertEquals("min must merge B's known value (5) despite B's unknown null_count", 5, merged.columnMin("EventDate"));
        assertEquals("max must merge B's known value (95) despite B's unknown null_count", 95, merged.columnMax("EventDate"));
    }

    public void testColumnMinPoisonedByPresentButStatsLessChild() {
        // Child A: has bonus with full stats. Child B: column physically present (column added)
        // but null count is unknown (-1). Defensive: poison rather than fabricate.
        SplitStats a = splitStatsRowCountWithColumn(100, "bonus", 0L, 10, 90, 400);
        SplitStats.Builder bb = new SplitStats.Builder().rowCount(50);
        bb.addColumn("bonus", -1L, null, null, 200);
        SplitStats b = bb.build();
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertNull("present-but-stats-less child must poison the min", merged.columnMin("bonus"));
        assertNull("present-but-stats-less child must poison the max", merged.columnMax("bonus"));
    }

    // -- columnMin --

    public void testColumnMinTakesMinimumAcrossChildren() {
        SplitStats a = splitStatsWithColumn("age", 0L, 18, 65, 400);
        SplitStats b = splitStatsWithColumn("age", 0L, 25, 80, 800);
        SplitStats c = splitStatsWithColumn("age", 0L, 10, 70, 1200);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b, c));
        assertEquals(10, merged.columnMin("age"));
    }

    public void testColumnMinReturnsNullWhenAnyChildHasNullMin() {
        SplitStats withMin = splitStatsWithColumn("age", 0L, 18, 65, 400);
        SplitStats withoutMin = splitStatsWithNullMinMax("age");
        MergedSplitStats merged = new MergedSplitStats(List.of(withMin, withoutMin));
        assertNull(merged.columnMin("age"));
    }

    public void testColumnMinReturnsNullForMissingColumn() {
        SplitStats a = splitStatsWithColumn("age", 0L, 18, 65, 400);
        MergedSplitStats merged = new MergedSplitStats(List.of(a));
        assertNull(merged.columnMin("name"));
    }

    public void testColumnMinCrossTypeIntegerLong() {
        SplitStats a = splitStatsWithColumn("score", 0L, 18, 65, 400);   // Integer min=18
        SplitStats b = splitStatsWithLongColumn("score", 10L, 20L, 200L); // Long min=20
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        // Integer 18 vs Long 20 -> min is 18, widened to Long
        Object min = merged.columnMin("score");
        assertNotNull(min);
        assertEquals(18L, ((Number) min).longValue());
    }

    // -- columnMax --

    public void testColumnMaxTakesMaximumAcrossChildren() {
        SplitStats a = splitStatsWithColumn("age", 0L, 18, 65, 400);
        SplitStats b = splitStatsWithColumn("age", 0L, 25, 90, 800);
        SplitStats c = splitStatsWithColumn("age", 0L, 10, 70, 1200);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b, c));
        assertEquals(90, merged.columnMax("age"));
    }

    public void testColumnMaxReturnsNullWhenAnyChildHasNullMax() {
        SplitStats withMax = splitStatsWithColumn("age", 0L, 18, 65, 400);
        SplitStats withoutMax = splitStatsWithNullMinMax("age");
        MergedSplitStats merged = new MergedSplitStats(List.of(withMax, withoutMax));
        assertNull(merged.columnMax("age"));
    }

    // -- columnSizeBytes --

    public void testColumnSizeBytesSumsChildren() {
        SplitStats a = splitStatsWithColumn("age", 0L, 18, 65, 400);
        SplitStats b = splitStatsWithColumn("age", 0L, 25, 90, 800);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(1200, merged.columnSizeBytes("age"));
    }

    public void testColumnSizeBytesReturnsMinusOneWhenAnyUnknown() {
        SplitStats a = splitStatsWithColumn("age", 0L, 18, 65, -1);
        SplitStats b = splitStatsWithColumn("age", 0L, 25, 90, 800);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(-1, merged.columnSizeBytes("age"));
    }

    // -- pure value-fold: unit/representation reconciliation is owned UPSTREAM by
    // SourceStatisticsSerializer.normalizeStatsToReconciled (applied in ExternalSourceResolver.aggregateFileStatistics
    // and FileSplitProvider), NOT here. The merge folds already-normalized values. --

    public void testColumnMinMaxIsPureValueFoldNoRescale() {
        // Regression guard for the double-rescale bug: per-file stats are normalized to the reconciled unit at
        // split construction BEFORE reaching the merge, so both children below are already epoch-nanos. A merge
        // that re-reconciled units (reading the split's file-local DATETIME type and multiplying an already-nanos
        // value by 1e6, as the removed childColumnTypes/mergeExtremum rescale did) would return 1_000_000_000_000
        // for the min instead of 1_000_000. The merge must fold value-only.
        SplitStats a = temporalColumn("ts", 2_000_000L, 5_000_000L);
        SplitStats b = temporalColumn("ts", 1_000_000L, 9_000_000L);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals("min folded value-only, no ×1e6 rescale", 1_000_000L, ((Number) merged.columnMin("ts")).longValue());
        assertEquals("max folded value-only, no ×1e6 rescale", 9_000_000L, ((Number) merged.columnMax("ts")).longValue());
    }

    // -- children() --

    public void testChildrenReturnsAllChildren() {
        SplitStats a = stats(100, -1);
        SplitStats b = stats(200, -1);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(2, merged.children().size());
    }

    // -- constructor validation --

    public void testConstructorRejectsNullChildren() {
        expectThrows(IllegalArgumentException.class, () -> new MergedSplitStats(null));
    }

    public void testConstructorRejectsEmptyChildren() {
        expectThrows(IllegalArgumentException.class, () -> new MergedSplitStats(List.of()));
    }

    // -- toString --

    public void testToStringIncludesChildCount() {
        SplitStats a = stats(100, -1);
        SplitStats b = stats(200, -1);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        String str = merged.toString();
        assertTrue(str.contains("2"));
    }

    // -- helpers --

    private static SplitStats stats(long rowCount, long sizeInBytes) {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(rowCount);
        if (sizeInBytes >= 0) {
            b.sizeInBytes(sizeInBytes);
        }
        return b.build();
    }

    private static SplitStats splitStatsWithColumn(String name, long nullCount, int min, int max, long sizeBytes) {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        b.addColumn(name, nullCount, min, max, sizeBytes);
        return b.build();
    }

    private static SplitStats splitStatsRowCountWithColumn(long rowCount, String name, long nullCount, int min, int max, long sizeBytes) {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(rowCount);
        b.addColumn(name, nullCount, min, max, sizeBytes);
        return b.build();
    }

    private static SplitStats splitStatsWithLongColumn(String name, long nullCount, long min, long max) {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        b.addColumn(name, nullCount, min, max, -1);
        return b.build();
    }

    private static SplitStats splitStatsWithNullMinMax(String name) {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        b.addColumn(name, -1L, null, null, -1L);
        return b.build();
    }

    private static SplitStats temporalColumn(String name, long minEpoch, long maxEpoch) {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        b.addColumn(name, 0L, minEpoch, maxEpoch, 400);
        return b.build();
    }
}
