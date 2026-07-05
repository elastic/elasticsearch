/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    // -- temporal-unit reconciliation (millis DATETIME vs nanos DATE_NANOS) --

    public void testColumnMinMaxMixedTemporalUnitsRescaleToNanos() {
        // File A: ts is DATETIME (epoch-millis), min=2 max=5 -> 2_000_000 / 5_000_000 ns.
        // File B: ts is DATE_NANOS (epoch-nanos), min=1 max=9_999_999 ns.
        SplitStats a = temporalColumn("ts", 2L, 5L);
        SplitStats b = temporalColumn("ts", 1L, 9_999_999L);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b), types(DataType.DATETIME, DataType.DATE_NANOS));
        assertEquals("min widened to epoch-nanos", 1L, ((Number) merged.columnMin("ts")).longValue());
        assertEquals("max widened to epoch-nanos", 9_999_999L, ((Number) merged.columnMax("ts")).longValue());
    }

    public void testColumnMinMaxUniformDatetimeStaysMillis() {
        // Both files DATETIME: no rescale, result stays epoch-millis (reconciled type is DATETIME).
        SplitStats a = temporalColumn("ts", 2L, 5L);
        SplitStats b = temporalColumn("ts", 1L, 9L);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b), types(DataType.DATETIME, DataType.DATETIME));
        assertEquals(1L, ((Number) merged.columnMin("ts")).longValue());
        assertEquals(9L, ((Number) merged.columnMax("ts")).longValue());
    }

    public void testColumnMinMaxUniformDateNanosUnchanged() {
        SplitStats a = temporalColumn("ts", 2000L, 5000L);
        SplitStats b = temporalColumn("ts", 1000L, 9000L);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b), types(DataType.DATE_NANOS, DataType.DATE_NANOS));
        assertEquals(1000L, ((Number) merged.columnMin("ts")).longValue());
        assertEquals(9000L, ((Number) merged.columnMax("ts")).longValue());
    }

    public void testColumnMinMaxTemporalWithUnknownTypeIsPoisoned() {
        // Column is temporal in file A but file B carries no per-file type; we cannot safely rescale -> poison.
        SplitStats a = temporalColumn("ts", 2L, 5L);
        SplitStats b = temporalColumn("ts", 1L, 9L);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b), types(DataType.DATE_NANOS, null));
        assertNull(merged.columnMin("ts"));
        assertNull(merged.columnMax("ts"));
    }

    public void testColumnMinMaxMillisOverflowingNanosIsPoisoned() {
        // File A DATETIME value has no nanosecond representation (millis * 1e6 overflows a long) -> poison.
        SplitStats a = temporalColumn("ts", 10_000_000_000_000L, 20_000_000_000_000L);
        SplitStats b = temporalColumn("ts", 1L, 9L);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b), types(DataType.DATETIME, DataType.DATE_NANOS));
        assertNull(merged.columnMin("ts"));
        assertNull(merged.columnMax("ts"));
    }

    public void testColumnMinMaxMixedUnitsWithoutTypesMergesUnitBlind() {
        // No per-file types (old-node / self-infer path): behavior is unchanged value-only merge. Documents that
        // the reconciliation only kicks in when per-file types are available.
        SplitStats a = temporalColumn("ts", 2L, 5L);
        SplitStats b = temporalColumn("ts", 1L, 9L);
        MergedSplitStats merged = new MergedSplitStats(List.of(a, b));
        assertEquals(1L, ((Number) merged.columnMin("ts")).longValue());
        assertEquals(9L, ((Number) merged.columnMax("ts")).longValue());
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

    private static List<Map<String, DataType>> types(DataType... perChild) {
        List<Map<String, DataType>> result = new ArrayList<>(perChild.length);
        for (DataType type : perChild) {
            result.add(type == null ? null : Map.of("ts", type));
        }
        return result;
    }
}
