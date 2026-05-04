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

    public void testColumnNullCountReturnsMinusOneForMissingColumn() {
        SplitStats a = splitStatsWithColumn("age", 5L, 10, 90, 400);
        MergedSplitStats merged = new MergedSplitStats(List.of(a));
        assertEquals(-1, merged.columnNullCount("name"));
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
}
