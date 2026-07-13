/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitStatsTests extends ESTestCase {

    public void testBuilderBasicRoundTrip() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(1000);
        builder.sizeInBytes(50000);

        int ageOrd = builder.addColumn("age");
        builder.nullCount(ageOrd, 10);
        builder.min(ageOrd, 18);
        builder.max(ageOrd, 85);
        builder.sizeBytes(ageOrd, 4000);

        int nameOrd = builder.addColumn("name");
        builder.nullCount(nameOrd, 0);
        builder.min(nameOrd, "Alice");
        builder.max(nameOrd, "Zara");
        builder.sizeBytes(nameOrd, 12000);

        SplitStats stats = builder.build();

        assertEquals(1000, stats.rowCount());
        assertEquals(50000, stats.sizeInBytes());
        assertEquals(2, stats.columnCount());

        assertEquals("age", stats.columnName(0));
        assertEquals(10, stats.nullCount(0));
        assertEquals(18, stats.min(0));
        assertEquals(85, stats.max(0));
        assertEquals(4000, stats.sizeBytes(0));

        assertEquals("name", stats.columnName(1));
        assertEquals(0, stats.nullCount(1));
        assertEquals("Alice", stats.min(1));
        assertEquals("Zara", stats.max(1));
        assertEquals(12000, stats.sizeBytes(1));
    }

    public void testValueCountServesCountColForMultivaluedColumn() throws IOException {
        // A multivalued column: 100 rows, 10 null, but 270 non-null VALUES (the other 90 rows average 3 each).
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(100);
        int tags = builder.addColumn("tags");
        builder.nullCount(tags, 10);
        builder.valueCount(tags, 270);
        SplitStats stats = builder.build();

        // COUNT(tags) must be the VALUE count (270), not rowCount - nullCount (90, the non-null ROW count).
        assertEquals(270L, stats.columnValueCount("tags"));
        assertEquals(270L, stats.valueCount(0));
        assertEquals(90L, stats.rowCount() - stats.columnNullCount("tags"));

        // wire round-trip preserves valueCount
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        SplitStats back = new SplitStats(out.bytes().streamInput());
        assertEquals(270L, back.columnValueCount("tags"));

        // flat-map carries it under the canonical key
        assertEquals(270L, ((Number) stats.toMap().get(SourceStatisticsSerializer.columnValueCountKey("tags"))).longValue());
    }

    public void testColumnValueCountIsMinusOneWhenNotHarvested() {
        // Footer formats (parquet) don't harvest a value count; columnValueCount returns -1 so COUNT(col)
        // falls back to rowCount - nullCount. A column added without valueCount(...) must report -1.
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.rowCount(10);
        int age = builder.addColumn("age");
        builder.nullCount(age, 2);
        SplitStats stats = builder.build();
        assertEquals(-1L, stats.columnValueCount("age"));
        assertEquals(-1L, stats.columnValueCount("does_not_exist"));
    }

    public void testMergedMinMaxTreatNaNAsUnmergeable() {
        // A NaN MERGED with a real value is unmergeable -> null (poison) -> the column safe-misses and a
        // full scan returns the correct NaN. Comparable.compareTo would otherwise order NaN as the largest
        // double and silently drop it from a MIN.
        assertNull(SplitStats.mergedMin(Double.NaN, 5.0));
        assertNull(SplitStats.mergedMin(5.0, Double.NaN));
        assertNull(SplitStats.mergedMax(Double.NaN, 5.0));
        assertNull(SplitStats.mergedMax(5.0, Double.NaN));
        // Non-NaN doubles still merge normally.
        assertEquals(1.0, SplitStats.mergedMin(1.0, 2.0));
        assertEquals(2.0, SplitStats.mergedMax(1.0, 2.0));
        // A lone NaN contribution (other side null) is the column's value and is served as NaN — the poison
        // only fires when NaN actually meets another value in a fold.
        assertEquals(Double.NaN, SplitStats.mergedMin(Double.NaN, null));
        assertEquals(Double.NaN, SplitStats.mergedMax(null, Double.NaN));
    }

    public void testValueCountWireGatedByTransportVersion() throws IOException {
        // valueCount is gated on a transport version. A peer at an older version (that still serializes
        // SplitStats, e.g. ESQL_SPLIT_STATS_COMPACT) must not see the field; it defaults to -1 so COUNT(col)
        // falls back to rowCount - nullCount. Verifies the BWC gate, not just the current-version round-trip.
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        int tags = b.addColumn("tags");
        b.nullCount(tags, 10);
        b.valueCount(tags, 270);
        SplitStats stats = b.build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(FileSplit.ESQL_SPLIT_STATS_COMPACT); // predates the valueCount gate
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(FileSplit.ESQL_SPLIT_STATS_COMPACT);
        SplitStats back = new SplitStats(in);
        assertEquals("valueCount absent on the wire before its gate -> -1 (COUNT falls back)", -1L, back.columnValueCount("tags"));
        // nullCount (an older field) still round-trips at the older version.
        assertEquals(10L, back.columnNullCount("tags"));

        // At the current version it DOES round-trip (sanity vs the gated case above).
        BytesStreamOutput cur = new BytesStreamOutput();
        stats.writeTo(cur);
        assertEquals(270L, new SplitStats(cur.bytes().streamInput()).columnValueCount("tags"));
    }

    public void testUnservableExtremumReadsNullButDistinguishesFromNoData() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        int poisoned = b.addColumn("poisoned", 0L, null, null, 400);
        b.minUnservable(poisoned);
        b.maxUnservable(poisoned);
        b.addColumn("nodata", 0L, null, null, 400); // no extremum, but NOT poisoned
        SplitStats stats = b.build();

        // Both read as null through the accessor...
        assertNull(stats.columnMin("poisoned"));
        assertNull(stats.columnMax("poisoned"));
        assertNull(stats.columnMin("nodata"));

        // ...but the flat map distinguishes them: the poisoned column carries the marker, no-data does not.
        Map<String, Object> map = stats.toMap();
        assertEquals(Boolean.TRUE, map.get(SourceStatisticsSerializer.columnMinUnservableKey("poisoned")));
        assertEquals(Boolean.TRUE, map.get(SourceStatisticsSerializer.columnMaxUnservableKey("poisoned")));
        assertNull(map.get(SourceStatisticsSerializer.columnMinKey("poisoned")));
        assertNull(map.get(SourceStatisticsSerializer.columnMinUnservableKey("nodata")));
    }

    public void testUnservableMarkerRoundTripsThroughMap() {
        // The compact form is now a lossless superset of the flat map: a .min_unservable marker survives of()->toMap().
        Map<String, Object> input = new HashMap<>();
        input.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        input.put(SourceStatisticsSerializer.columnNullCountKey("ts"), 0L);
        input.put(SourceStatisticsSerializer.columnMaxKey("ts"), 5000L);            // max servable
        input.put(SourceStatisticsSerializer.columnMinUnservableKey("ts"), Boolean.TRUE); // min poisoned

        SplitStats stats = SplitStats.of(input);
        assertNotNull(stats);
        assertNull("poisoned min reads null", stats.columnMin("ts"));
        assertEquals("servable max still served", 5000L, stats.columnMax("ts"));

        Map<String, Object> out = stats.toMap();
        assertEquals(Boolean.TRUE, out.get(SourceStatisticsSerializer.columnMinUnservableKey("ts")));
        assertNull(out.get(SourceStatisticsSerializer.columnMinKey("ts")));
        assertEquals(5000L, out.get(SourceStatisticsSerializer.columnMaxKey("ts")));
        assertNull(out.get(SourceStatisticsSerializer.columnMaxUnservableKey("ts")));
    }

    public void testUnservableMarkerSurvivesWireRoundTrip() throws IOException {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        int c = b.addColumn("c", 0L, null, 90, 400); // min poisoned, max servable
        b.minUnservable(c);
        SplitStats stats = b.build();

        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        SplitStats back = new SplitStats(out.bytes().streamInput());

        assertEquals("compact wire form preserves the servability markers", stats, back);
        assertNull("poisoned min stays unservable across the wire", back.columnMin("c"));
        assertEquals("servable max survives", 90, back.columnMax("c"));
    }

    public void testUnservableMarkerWireGatedByTransportVersion() throws IOException {
        // The servability markers ship under the warm-aggregate TV. An older peer (that still serializes SplitStats
        // under ESQL_SPLIT_STATS_COMPACT) does not see them and defaults to servable -- SAFE only because a poisoned
        // extremum carries NO value (the enforced invariant minServable||min==null), so it reaches the old peer as
        // an absent value and the old peer safe-misses, never serving a poisoned value as real.
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        int c = b.addColumn("c", 0L, null, 90, 400); // min poisoned (no value); max servable
        b.minUnservable(c);
        SplitStats stats = b.build();

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(FileSplit.ESQL_SPLIT_STATS_COMPACT); // predates the marker gate
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(FileSplit.ESQL_SPLIT_STATS_COMPACT);
        SplitStats back = new SplitStats(in);

        assertNull("a poisoned min reaches an old peer as an absent value -> safe-miss, never a wrong value", back.columnMin("c"));
        assertEquals("a servable max still round-trips at the older version", 90, back.columnMax("c"));
    }

    public void testOfDropsValueWhenUnservableMarkerPresent() {
        // A malformed flat map carrying BOTH .min and .min_unservable must normalize to min=null (marker wins),
        // enforcing minServable||min==null so a wire round-trip can never ship a poisoned value to an old peer.
        Map<String, Object> input = new HashMap<>();
        input.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        input.put(SourceStatisticsSerializer.columnMinKey("c"), 42);              // stray value...
        input.put(SourceStatisticsSerializer.columnMinUnservableKey("c"), Boolean.TRUE); // ...alongside the marker

        SplitStats stats = SplitStats.of(input); // must not trip the servability assert
        assertNotNull(stats);
        assertNull("marker wins: the stray min value is dropped", stats.columnMin("c"));
        assertNull("and it re-emits no value", stats.toMap().get(SourceStatisticsSerializer.columnMinKey("c")));
        assertEquals(Boolean.TRUE, stats.toMap().get(SourceStatisticsSerializer.columnMinUnservableKey("c")));
    }

    /**
     * EQUIVALENCE PROOF for the compact fold: over a randomized corpus (multiple files, absent columns,
     * cross-type / NaN poison, pre-seeded unservable markers, both footer and text modes), the compact
     * {@code SplitStats.fold(...).toMap()} must be key-equal to {@code mergeStatistics(...)}. This was the signal
     * that let {@code mergeStatistics} be rewritten to delegate to {@code fold} without behavior change (it ran
     * against the independent flat-map fold at that point); now that {@code mergeStatistics} delegates, it guards
     * the {@code of}/{@code fold}/{@code toMap} round-trip over the same broad corpus.
     */
    public void testFoldMatchesMergeStatisticsDifferential() {
        for (int trial = 0; trial < 500; trial++) {
            int nFiles = randomIntBetween(2, 5);
            int nCols = randomIntBetween(0, 4);
            List<Map<String, Object>> maps = new ArrayList<>();
            for (int f = 0; f < nFiles; f++) {
                Map<String, Object> m = new HashMap<>();
                m.put(SourceStatisticsSerializer.STATS_ROW_COUNT, (long) randomIntBetween(0, 1000));
                if (randomBoolean()) {
                    m.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, (long) randomIntBetween(0, 10000));
                }
                for (int c = 0; c < nCols; c++) {
                    String col = "c" + c;
                    if (randomBoolean()) {
                        continue; // absent in this file
                    }
                    if (randomBoolean()) {
                        m.put(SourceStatisticsSerializer.columnNullCountKey(col), (long) randomIntBetween(0, 100));
                    }
                    if (randomBoolean()) {
                        m.put(SourceStatisticsSerializer.columnValueCountKey(col), (long) randomIntBetween(0, 100));
                    }
                    if (randomBoolean()) {
                        m.put(SourceStatisticsSerializer.columnMinKey(col), randomExtremum());
                        m.put(SourceStatisticsSerializer.columnMaxKey(col), randomExtremum());
                    }
                    if (randomBoolean()) {
                        m.put(SourceStatisticsSerializer.columnSizeBytesKey(col), (long) randomIntBetween(0, 5000));
                    }
                    if (rarely()) { // an already-poisoned input: marker present, value absent
                        m.remove(SourceStatisticsSerializer.columnMinKey(col));
                        m.put(SourceStatisticsSerializer.columnMinUnservableKey(col), Boolean.TRUE);
                    }
                    if (rarely()) {
                        m.remove(SourceStatisticsSerializer.columnMaxKey(col));
                        m.put(SourceStatisticsSerializer.columnMaxUnservableKey(col), Boolean.TRUE);
                    }
                }
                maps.add(m);
            }
            for (boolean implicitNulls : new boolean[] { true, false }) {
                Map<String, Object> viaMerge = SourceStatisticsSerializer.mergeStatistics(maps, implicitNulls);
                List<SplitStats> splits = new ArrayList<>();
                for (Map<String, Object> m : maps) {
                    splits.add(SplitStats.of(m));
                }
                SplitStats folded = SplitStats.fold(splits, implicitNulls);
                Map<String, Object> viaFold = folded == null ? null : folded.toMap();
                assertEquals("fold != mergeStatistics (implicitNulls=" + implicitNulls + ") for " + maps, viaMerge, viaFold);
            }
        }
    }

    /**
     * TRIPWIRE linking the two extremum-merge implementations: the fold-time {@link SplitStats#fold} (uses the
     * stored servable bit) and the serve-time {@link MergedSplitStats} (re-derives poison from a null-count
     * heuristic). They share the {@code mergedMin}/{@code mergedMax} law but implement the poison POLICY twice; a
     * future SUM/AVG or a heuristic tweak could silently diverge the serve path from the fold path. On realistic
     * per-file children -- every child HAS the column, each either servable, all-null, or poisoned (never the
     * unrealistic present-values-but-no-servable-min state where the two correctly differ by granularity) -- the
     * two paths MUST agree on columnMin/columnMax (and the counts). Enforces the "argued-benign" split as tested.
     */
    public void testMergedSplitStatsMatchesFoldOnRealisticChildren() {
        for (int trial = 0; trial < 400; trial++) {
            int n = randomIntBetween(2, 5);
            List<SplitStats> children = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                long rows = randomIntBetween(1, 100);
                SplitStats.Builder b = new SplitStats.Builder().rowCount(rows);
                int c = b.addColumn("c");
                switch (randomIntBetween(0, 3)) {
                    case 0 -> { // servable: has non-null values (nc < rows) and a min/max
                        b.nullCount(c, randomIntBetween(0, (int) rows - 1));
                        b.min(c, randomExtremum());
                        b.max(c, randomExtremum());
                    }
                    case 1 -> // all-null: every row null, no extremum, not poisoned
                        b.nullCount(c, rows);
                    case 2 -> { // poisoned: has values (nc < rows) but the extremum was cleared
                        b.nullCount(c, randomIntBetween(0, (int) rows - 1));
                        b.minUnservable(c);
                        b.maxUnservable(c);
                    }
                    default -> // present with non-null rows but NO extremum committed and NOT poisoned
                        // (minServable stays true, min/max null). This is the case ColumnFold used to serve a
                        // subset extremum for while MergedSplitStats poisons — the fold must poison too.
                        b.nullCount(c, randomIntBetween(0, (int) rows - 1));
                }
                // Randomly give the (present) column a known value count or leave it unknown (-1, the
                // addColumn default). Mixing present-with-count and present-without-count children is the
                // case a naive sum under-counts: fold must poison COUNT(col) exactly like MergedSplitStats.
                if (randomBoolean()) {
                    b.valueCount(c, randomIntBetween(0, (int) rows));
                }
                children.add(b.build());
            }
            SplitStats folded = SplitStats.fold(children, randomBoolean());
            MergedSplitStats merged = new MergedSplitStats(List.copyOf(children));
            String ctx = "children=" + children;
            assertEquals("columnMin fold vs MergedSplitStats: " + ctx, folded.columnMin("c"), merged.columnMin("c"));
            assertEquals("columnMax fold vs MergedSplitStats: " + ctx, folded.columnMax("c"), merged.columnMax("c"));
            assertEquals("columnNullCount fold vs MergedSplitStats: " + ctx, folded.columnNullCount("c"), merged.columnNullCount("c"));
            assertEquals("columnValueCount fold vs MergedSplitStats: " + ctx, folded.columnValueCount("c"), merged.columnValueCount("c"));
            assertEquals("rowCount fold vs MergedSplitStats: " + ctx, folded.rowCount(), merged.rowCount());
        }
    }

    /** A random extremum value spanning the fold-relevant types: int / long / double / NaN. */
    private Object randomExtremum() {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> randomIntBetween(-100, 100);
            case 1 -> (long) randomIntBetween(-100, 100);
            case 2 -> (double) randomIntBetween(-100, 100);
            default -> Double.NaN;
        };
    }

    public void testBulkAddColumn() {
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(500).sizeInBytes(10000);
        int ord = builder.addColumn("score", 5L, 0.0, 100.0, 2000);
        SplitStats stats = builder.build();

        assertEquals(0, ord);
        assertEquals("score", stats.columnName(0));
        assertEquals(5, stats.nullCount(0));
        assertEquals(0.0, stats.min(0));
        assertEquals(100.0, stats.max(0));
        assertEquals(2000, stats.sizeBytes(0));
    }

    public void testPathSegmentDictionary() {
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(100);
        builder.addColumn("customer.billing_address.city", 0L, "Austin", "Zurich", 5000);
        builder.addColumn("customer.billing_address.zip", 2L, "10001", "99999", 3000);
        builder.addColumn("customer.name", 1L, "Alice", "Zara", 8000);

        SplitStats stats = builder.build();

        assertEquals(3, stats.columnCount());
        assertEquals("customer.billing_address.city", stats.columnName(0));
        assertEquals("customer.billing_address.zip", stats.columnName(1));
        assertEquals("customer.name", stats.columnName(2));
    }

    public void testWriteableRoundTrip() throws IOException {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(2000).sizeInBytes(100000);
        b.addColumn("id", 0L, 1L, 2000L, 16000);
        b.addColumn("customer.name", 5L, "Alice", "Zara", 40000);
        b.addColumn("amount", 10L, 1.5, 999.99, 16000);
        SplitStats original = b.build();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        SplitStats deserialized = new SplitStats(in);

        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
        assertEquals(original.columnCount(), deserialized.columnCount());
        for (int i = 0; i < original.columnCount(); i++) {
            assertEquals(original.columnName(i), deserialized.columnName(i));
            assertEquals(original.nullCount(i), deserialized.nullCount(i));
            assertEquals(original.min(i), deserialized.min(i));
            assertEquals(original.max(i), deserialized.max(i));
            assertEquals(original.sizeBytes(i), deserialized.sizeBytes(i));
        }
    }

    public void testToMapMatchesSerializerConvention() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(500).sizeInBytes(20000);
        b.addColumn("age", 10L, 18, 85, 4000);
        b.addColumn("name", 0L, "Alice", "Zara", 12000);
        SplitStats stats = b.build();

        Map<String, Object> map = stats.toMap();

        assertEquals(500L, map.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(20000L, map.get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertEquals(10L, map.get(SourceStatisticsSerializer.columnNullCountKey("age")));
        assertEquals(18, map.get(SourceStatisticsSerializer.columnMinKey("age")));
        assertEquals(85, map.get(SourceStatisticsSerializer.columnMaxKey("age")));
        assertEquals(4000L, map.get(SourceStatisticsSerializer.columnSizeBytesKey("age")));
        assertEquals(0L, map.get(SourceStatisticsSerializer.columnNullCountKey("name")));
        assertEquals("Alice", map.get(SourceStatisticsSerializer.columnMinKey("name")));
        assertEquals("Zara", map.get(SourceStatisticsSerializer.columnMaxKey("name")));
        assertEquals(12000L, map.get(SourceStatisticsSerializer.columnSizeBytesKey("name")));
    }

    public void testOfMapRoundTrip() {
        Map<String, Object> input = new HashMap<>();
        input.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        input.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, 50000L);
        input.put(SourceStatisticsSerializer.columnNullCountKey("age"), 10L);
        input.put(SourceStatisticsSerializer.columnMinKey("age"), 18);
        input.put(SourceStatisticsSerializer.columnMaxKey("age"), 85);
        input.put(SourceStatisticsSerializer.columnSizeBytesKey("age"), 4000L);

        SplitStats stats = SplitStats.of(input);
        assertNotNull(stats);
        assertEquals(1000, stats.rowCount());
        assertEquals(50000, stats.sizeInBytes());

        Map<String, Object> output = stats.toMap();
        assertEquals(input.get(SourceStatisticsSerializer.STATS_ROW_COUNT), output.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(input.get(SourceStatisticsSerializer.STATS_SIZE_BYTES), output.get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertEquals(
            input.get(SourceStatisticsSerializer.columnNullCountKey("age")),
            output.get(SourceStatisticsSerializer.columnNullCountKey("age"))
        );
        assertEquals(input.get(SourceStatisticsSerializer.columnMinKey("age")), output.get(SourceStatisticsSerializer.columnMinKey("age")));
        assertEquals(input.get(SourceStatisticsSerializer.columnMaxKey("age")), output.get(SourceStatisticsSerializer.columnMaxKey("age")));
        assertEquals(
            input.get(SourceStatisticsSerializer.columnSizeBytesKey("age")),
            output.get(SourceStatisticsSerializer.columnSizeBytesKey("age"))
        );
    }

    public void testUnknownStats() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        b.addColumn("x");
        SplitStats stats = b.build();

        assertEquals(-1, stats.nullCount(0));
        assertNull(stats.min(0));
        assertNull(stats.max(0));
        assertEquals(-1, stats.sizeBytes(0));

        Map<String, Object> map = stats.toMap();
        assertEquals(100L, map.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertFalse(map.containsKey(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertFalse(map.containsKey(SourceStatisticsSerializer.columnNullCountKey("x")));
        assertFalse(map.containsKey(SourceStatisticsSerializer.columnMinKey("x")));
        assertFalse(map.containsKey(SourceStatisticsSerializer.columnMaxKey("x")));
        assertFalse(map.containsKey(SourceStatisticsSerializer.columnSizeBytesKey("x")));
    }

    public void testColumnNullCountReturnsRowCountWhenColumnAbsent() {
        // Per-file SplitStats with no "bonus" column at all. Under the SPI's "implicit nulls"
        // contract, every row of this file counts as a null for "bonus".
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        b.addColumn("age", 5L, 18, 65, 400);
        SplitStats stats = b.build();

        assertEquals("absent column should return rowCount as implicit nulls", 100L, stats.columnNullCount("bonus"));
        assertNull(stats.columnMin("bonus"));
        assertNull(stats.columnMax("bonus"));
        assertEquals(-1L, stats.columnSizeBytes("bonus"));
    }

    public void testColumnNullCountReturnsMinusOneWhenColumnPresentButStatsLess() {
        // Defensive: a column physically present but with an unknown null count must keep
        // returning -1 (callers bail out rather than fabricate). This mirrors the rare
        // Parquet path where stats were not written.
        SplitStats.Builder b = new SplitStats.Builder().rowCount(100);
        b.addColumn("bonus");
        SplitStats stats = b.build();

        assertEquals("present-but-stats-less column must keep -1 sentinel", -1L, stats.columnNullCount("bonus"));
    }

    public void testFindColumn() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(50);
        b.addColumn("a.b.c", 0L, null, null, -1);
        b.addColumn("x", 0L, null, null, -1);
        SplitStats stats = b.build();

        assertEquals(0, stats.findColumn("a.b.c"));
        assertEquals(1, stats.findColumn("x"));
        assertEquals(-1, stats.findColumn("nonexistent"));
    }

    public void testEmptyColumns() {
        SplitStats stats = new SplitStats.Builder().rowCount(42).sizeInBytes(1024).build();

        assertEquals(0, stats.columnCount());
        assertEquals(42, stats.rowCount());
        assertEquals(1024, stats.sizeInBytes());

        Map<String, Object> map = stats.toMap();
        assertEquals(42L, map.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(1024L, map.get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertEquals(2, map.size());
    }

    public void testLargeColumnCount() {
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(10000).sizeInBytes(5000000);
        for (int i = 0; i < 500; i++) {
            builder.addColumn(
                "schema.table.col_" + i,
                randomLongBetween(0, 100),
                randomIntBetween(0, 1000),
                randomIntBetween(1000, 2000),
                randomLongBetween(1000, 100000)
            );
        }
        SplitStats stats = builder.build();

        assertEquals(500, stats.columnCount());
        for (int i = 0; i < 500; i++) {
            assertEquals("schema.table.col_" + i, stats.columnName(i));
        }

        Map<String, Object> map = stats.toMap();
        assertTrue(map.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertTrue(map.containsKey(SourceStatisticsSerializer.columnNullCountKey("schema.table.col_0")));
    }

    public void testFlatNames() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(10);
        b.addColumn("id", 0L, 1L, 100L, 800);
        b.addColumn("value", 0L, 0.0, 1.0, 800);
        SplitStats stats = b.build();

        assertEquals("id", stats.columnName(0));
        assertEquals("value", stats.columnName(1));
    }

    public void testOfNullAndEmptyMap() {
        assertNull(SplitStats.of(null));
        assertNull(SplitStats.of(Map.of()));
        assertNull(SplitStats.of(Map.of("unrelated_key", "value")));
    }

    public void testDuplicateColumnNameThrows() {
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(10);
        builder.addColumn("x");
        expectThrows(IllegalArgumentException.class, () -> builder.addColumn("x"));
    }

    public void testBuildWithoutRowCountThrows() {
        SplitStats.Builder builder = new SplitStats.Builder();
        builder.addColumn("col");
        expectThrows(IllegalArgumentException.class, builder::build);
    }

    public void testColumnNameBoundsCheck() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(10);
        b.addColumn("x", 0L, null, null, -1);
        SplitStats stats = b.build();
        expectThrows(IndexOutOfBoundsException.class, () -> stats.columnName(1));
        expectThrows(IndexOutOfBoundsException.class, () -> stats.columnName(-1));
    }

    public void testWriteableRoundTripWithUnknownStats() throws IOException {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(0);
        b.addColumn("empty_col");
        SplitStats original = b.build();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        SplitStats deserialized = new SplitStats(out.bytes().streamInput());
        assertEquals(original, deserialized);
        assertEquals(-1, deserialized.nullCount(0));
        assertNull(deserialized.min(0));
        assertNull(deserialized.max(0));
        assertEquals(-1, deserialized.sizeBytes(0));
    }

    public void testToString() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(42);
        b.addColumn("a", 0L, null, null, -1);
        b.addColumn("b", 0L, null, null, -1);
        SplitStats stats = b.build();
        assertEquals("SplitStats[rowCount=42, columns=2]", stats.toString());
    }

    public void testOfMapWithNestedColumnNames() {
        Map<String, Object> input = new HashMap<>();
        input.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 500L);
        input.put(SourceStatisticsSerializer.columnNullCountKey("a.b.c"), 10L);
        input.put(SourceStatisticsSerializer.columnMinKey("a.b.c"), 1);
        input.put(SourceStatisticsSerializer.columnMaxKey("a.b.c"), 100);

        SplitStats stats = SplitStats.of(input);
        assertNotNull(stats);
        assertEquals(0, stats.findColumn("a.b.c"));
        assertEquals("a.b.c", stats.columnName(0));
        assertEquals(10, stats.nullCount(0));
        assertEquals(1, stats.min(0));
        assertEquals(100, stats.max(0));
        assertEquals(-1, stats.sizeInBytes());

        Map<String, Object> output = stats.toMap();
        assertEquals(
            input.get(SourceStatisticsSerializer.columnMinKey("a.b.c")),
            output.get(SourceStatisticsSerializer.columnMinKey("a.b.c"))
        );
        assertEquals(
            input.get(SourceStatisticsSerializer.columnMaxKey("a.b.c")),
            output.get(SourceStatisticsSerializer.columnMaxKey("a.b.c"))
        );
    }

    // --- resolveEffectiveStats tests ---

    public void testResolveEffectiveStatsEmptySplitsUsesSourceMetadata() {
        Map<String, Object> sourceMetadata = new HashMap<>();
        sourceMetadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);

        var result = SplitStats.resolveEffectiveStats(List.of(), sourceMetadata);
        assertNotNull(result);
        assertEquals(1000, result.rowCount());
    }

    public void testResolveEffectiveStatsEmptySplitsReturnsNullWhenPartial() {
        Map<String, Object> sourceMetadata = new HashMap<>();
        sourceMetadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        sourceMetadata.put(SourceStatisticsSerializer.STATS_PARTIAL, Boolean.TRUE);

        var result = SplitStats.resolveEffectiveStats(List.of(), sourceMetadata);
        assertNull("Should return null for partial stats to prevent wrong pushdown", result);
    }

    public void testResolveEffectiveStatsEmptySplitsPartialFalseUsesSourceMetadata() {
        Map<String, Object> sourceMetadata = new HashMap<>();
        sourceMetadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        sourceMetadata.put(SourceStatisticsSerializer.STATS_PARTIAL, Boolean.FALSE);

        var result = SplitStats.resolveEffectiveStats(List.of(), sourceMetadata);
        assertNotNull("Non-true partial flag should not block stats", result);
        assertEquals(1000, result.rowCount());
    }

    public void testResolveEffectiveStatsSingleSplitWithStats() {
        SplitStats.Builder b = new SplitStats.Builder().rowCount(500);
        b.addColumn("x", 0L, 1, 100, 400);
        SplitStats splitStats = b.build();
        FileSplit split = fileSplitWithStats(splitStats);

        var result = SplitStats.resolveEffectiveStats(List.of(split), Map.of());
        assertNotNull(result);
        assertEquals(500, result.rowCount());
        assertEquals(0, result.columnNullCount("x"));
        assertEquals(1, result.columnMin("x"));
        assertEquals(100, result.columnMax("x"));
    }

    public void testResolveEffectiveStatsMultipleSplitsMerged() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("age", 5L, 18, 65, 400);
        SplitStats s1 = b1.build();
        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("age", 10L, 20, 80, 800);
        SplitStats s2 = b2.build();
        SplitStats.Builder b3 = new SplitStats.Builder().rowCount(300);
        b3.addColumn("age", 0L, 25, 90, 1200);
        SplitStats s3 = b3.build();

        List<ExternalSplit> splits = List.of(fileSplitWithStats(s1), fileSplitWithStats(s2), fileSplitWithStats(s3));

        var result = SplitStats.resolveEffectiveStats(splits, Map.of());
        assertNotNull(result);
        assertEquals(600, result.rowCount());
        assertEquals(15, result.columnNullCount("age"));
        assertEquals(18, result.columnMin("age"));
        assertEquals(90, result.columnMax("age"));
        assertEquals(2400, result.columnSizeBytes("age"));
    }

    public void testResolveEffectiveStatsMultipleSplitsOneMissingStatsReturnsNull() {
        SplitStats s1 = new SplitStats.Builder().rowCount(100).build();
        FileSplit splitWithStats = fileSplitWithStats(s1);
        FileSplit splitWithoutStats = new FileSplit(
            "parquet",
            StoragePath.of("file:///nostat.parquet"),
            0,
            100,
            "parquet",
            Map.of(),
            Map.of(),
            null,
            null
        );

        List<ExternalSplit> splits = List.of(splitWithStats, splitWithoutStats);

        var result = SplitStats.resolveEffectiveStats(splits, Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 9999L));
        assertNull("Should return null when any split lacks stats", result);
    }

    // --- mergedMin / mergedMax unit tests ---

    public void testMergedMinNullHandling() {
        assertNull(SplitStats.mergedMin(null, null));
        assertEquals(10, SplitStats.mergedMin(null, 10));
        assertEquals(10, SplitStats.mergedMin(10, null));
    }

    public void testMergedMaxNullHandling() {
        assertNull(SplitStats.mergedMax(null, null));
        assertEquals(10, SplitStats.mergedMax(null, 10));
        assertEquals(10, SplitStats.mergedMax(10, null));
    }

    public void testMergedMinSameType() {
        assertEquals(5, SplitStats.mergedMin(10, 5));
        assertEquals(5L, SplitStats.mergedMin(5L, 20L));
        assertEquals(1.5, SplitStats.mergedMin(1.5, 3.0));
        assertEquals("Alice", SplitStats.mergedMin("Alice", "Zara"));
    }

    public void testMergedMaxSameType() {
        assertEquals(10, SplitStats.mergedMax(10, 5));
        assertEquals(20L, SplitStats.mergedMax(5L, 20L));
        assertEquals(3.0, SplitStats.mergedMax(1.5, 3.0));
        assertEquals("Zara", SplitStats.mergedMax("Alice", "Zara"));
    }

    public void testMergedMinCrossTypeIntegerLong() {
        assertEquals(5L, SplitStats.mergedMin(10, 5L));
        assertEquals(5L, SplitStats.mergedMin(5L, 10));
        assertEquals(10L, SplitStats.mergedMin(10, 20L));
    }

    public void testMergedMaxCrossTypeIntegerLong() {
        assertEquals(10L, SplitStats.mergedMax(10, 5L));
        assertEquals(10L, SplitStats.mergedMax(5L, 10));
        assertEquals(20L, SplitStats.mergedMax(10, 20L));
    }

    public void testMergedMinCrossTypeIntegerDouble() {
        assertEquals(1.5, SplitStats.mergedMin(3, 1.5));
        assertEquals(3.0, SplitStats.mergedMin(3, 5.0));
    }

    public void testMergedMaxCrossTypeIntegerDouble() {
        assertEquals(5.0, SplitStats.mergedMax(3, 5.0));
        assertEquals(3.0, SplitStats.mergedMax(3, 1.5));
    }

    public void testMergedMinCrossTypeFloatDouble() {
        assertEquals(5.0, SplitStats.mergedMin(10.0f, 5.0));
        assertEquals(10.0, SplitStats.mergedMin(10.0f, 20.0));
    }

    public void testMergedMaxCrossTypeFloatDouble() {
        assertEquals(20.0, SplitStats.mergedMax(10.0f, 20.0));
        assertEquals(10.0, SplitStats.mergedMax(10.0f, 5.0));
    }

    public void testMergedMinCrossTypeIntegerFloat() {
        assertEquals(5.0, SplitStats.mergedMin(10, 5.0f));
        assertEquals(10.0, SplitStats.mergedMin(10, 20.0f));
    }

    public void testMergedMaxCrossTypeIntegerFloat() {
        assertEquals(20.0, SplitStats.mergedMax(10, 20.0f));
        assertEquals(10.0, SplitStats.mergedMax(10, 5.0f));
    }

    public void testMergedMinLongDoubleIncompatible() {
        assertNull("Long + Double incompatible", SplitStats.mergedMin(10L, 5.0));
        assertNull("Double + Long incompatible", SplitStats.mergedMin(5.0, 10L));
    }

    public void testMergedMaxLongDoubleIncompatible() {
        assertNull("Long + Double incompatible", SplitStats.mergedMax(10L, 50.0));
        assertNull("Double + Long incompatible", SplitStats.mergedMax(50.0, 10L));
    }

    public void testMergedMinLongFloatIncompatible() {
        assertNull("Long + Float incompatible", SplitStats.mergedMin(10L, 5.0f));
        assertNull("Float + Long incompatible", SplitStats.mergedMin(5.0f, 10L));
    }

    public void testMergedMinStringNumericIncompatible() {
        assertNull(SplitStats.mergedMin("hello", 10));
        assertNull(SplitStats.mergedMin(10, "hello"));
    }

    public void testMergedMinMaxKeywordUsesUtf8ByteOrderNotUtf16() {
        // U+FFFD (replacement char, BMP) vs U+1F600 (emoji, astral plane). String.compareTo (UTF-16 code units)
        // orders the emoji BELOW the replacement char (its leading surrogate 0xD83D < 0xFFFD); UTF-8 bytes order
        // the replacement char below the emoji (0xEF < 0xF0). The runtime keyword MIN/MAX aggregators use UTF-8
        // (BytesRef) byte order, so the stat fold must too — otherwise a warm MIN/MAX disagrees with a scan.
        String bmp = "�";
        String astral = "😀";
        assertEquals("MIN is the UTF-8-smaller value, not the UTF-16 winner", bmp, SplitStats.mergedMin(bmp, astral));
        assertEquals(bmp, SplitStats.mergedMin(astral, bmp));
        assertEquals("MAX is the UTF-8-larger value", astral, SplitStats.mergedMax(bmp, astral));
        assertEquals(astral, SplitStats.mergedMax(astral, bmp));
        // The text-harvest representation (BytesRef) is already UTF-8-ordered — the String path must agree with it.
        assertEquals(new BytesRef(bmp), SplitStats.mergedMin(new BytesRef(bmp), new BytesRef(astral)));
        assertEquals(new BytesRef(astral), SplitStats.mergedMax(new BytesRef(bmp), new BytesRef(astral)));
    }

    private static FileSplit fileSplitWithStats(SplitStats stats) {
        Map<String, Object> statsMap = stats.toMap();
        return new FileSplit("parquet", StoragePath.of("file:///test.parquet"), 0, 100, "parquet", Map.of(), Map.of(), null, statsMap);
    }
}
