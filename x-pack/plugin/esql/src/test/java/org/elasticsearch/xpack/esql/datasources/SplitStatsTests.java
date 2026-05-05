/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
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

    // --- merge tests ---

    public void testMergeMultipleFilesSumsRowCounts() {
        SplitStats f1 = new SplitStats.Builder().rowCount(10000).build();
        SplitStats f2 = new SplitStats.Builder().rowCount(20000).build();
        SplitStats f3 = new SplitStats.Builder().rowCount(30000).build();

        SplitStats merged = SplitStats.merge(List.of(f1, f2, f3));
        assertNotNull(merged);
        assertEquals(60000, merged.rowCount());
    }

    public void testMergePreservesMinMaxAcrossFiles() {
        SplitStats.Builder fb1 = new SplitStats.Builder().rowCount(100);
        fb1.addColumn("ts", 0L, 1000L, 2000L, 800);
        SplitStats f1 = fb1.build();
        SplitStats.Builder fb2 = new SplitStats.Builder().rowCount(200);
        fb2.addColumn("ts", 0L, 500L, 1500L, 1600);
        SplitStats f2 = fb2.build();
        SplitStats.Builder fb3 = new SplitStats.Builder().rowCount(300);
        fb3.addColumn("ts", 0L, 800L, 3000L, 2400);
        SplitStats f3 = fb3.build();

        SplitStats merged = SplitStats.merge(List.of(f1, f2, f3));
        assertNotNull(merged);
        assertEquals(600, merged.rowCount());
        assertEquals(500L, merged.columnMin("ts"));
        assertEquals(3000L, merged.columnMax("ts"));
        assertEquals(0, merged.columnNullCount("ts"));
        assertEquals(4800, merged.columnSizeBytes("ts"));
    }

    public void testMergeSingleElementReturnsSame() {
        SplitStats.Builder sb = new SplitStats.Builder().rowCount(42);
        sb.addColumn("x", 0L, 1, 2, 100);
        SplitStats single = sb.build();

        SplitStats merged = SplitStats.merge(List.of(single));
        assertSame(single, merged);
    }

    public void testMergeNullAndEmptyReturnsNull() {
        assertNull(SplitStats.merge(null));
        assertNull(SplitStats.merge(List.of()));
    }

    // --- cross-type merge tests (type widening for UNION_BY_NAME) ---

    public void testMergeCrossTypeIntegerLongMinMax() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("price", 0L, 10, 50, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("price", 0L, 5L, 60L, 800);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertEquals(300, merged.rowCount());
        assertEquals(5L, merged.columnMin("price"));
        assertEquals(60L, merged.columnMax("price"));
    }

    public void testMergeCrossTypeIntegerDoubleMinMax() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("score", 0L, 3, 80, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("score", 0L, 1.5, 99.9, 800);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertEquals(1.5, merged.columnMin("score"));
        assertEquals(99.9, merged.columnMax("score"));
    }

    public void testMergeCrossTypeFloatDoubleMinMax() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("temp", 0L, 10.0f, 30.0f, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("temp", 0L, 5.0, 40.0, 800);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertEquals(5.0, merged.columnMin("temp"));
        assertEquals(40.0, merged.columnMax("temp"));
    }

    public void testMergeCrossTypeIntegerFloatMinMax() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("val", 0L, 10, 50, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("val", 0L, 5.0f, 60.0f, 800);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertEquals(5.0, merged.columnMin("val"));
        assertEquals(60.0, merged.columnMax("val"));
    }

    public void testMergeThreeWayCrossType() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("price", 0L, 10, 50, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("price", 0L, 5L, 60L, 800);
        SplitStats s2 = b2.build();

        SplitStats.Builder b3 = new SplitStats.Builder().rowCount(300);
        b3.addColumn("price", 0L, 3L, 100L, 1200);
        SplitStats s3 = b3.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2, s3));
        assertNotNull(merged);
        assertEquals(600, merged.rowCount());
        assertEquals(3L, merged.columnMin("price"));
        assertEquals(100L, merged.columnMax("price"));
    }

    public void testMergeLongDoubleIncompatibleClearsStats() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("val", 0L, 10L, 50L, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("val", 0L, 5.0, 60.0, 800);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertEquals(300, merged.rowCount());
        assertNull("incompatible types should clear min", merged.columnMin("val"));
        assertNull("incompatible types should clear max", merged.columnMax("val"));
    }

    public void testMergeLongDoubleIncompatibleClearsStatsReversed() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(200);
        b1.addColumn("val", 0L, 5.0, 60.0, 800);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(100);
        b2.addColumn("val", 0L, 10L, 50L, 400);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertEquals(300, merged.rowCount());
        assertNull("incompatible types should clear min regardless of order", merged.columnMin("val"));
        assertNull("incompatible types should clear max regardless of order", merged.columnMax("val"));
    }

    public void testMergeLongFloatIncompatibleClearsStats() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("val", 0L, 10L, 50L, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("val", 0L, 5.0f, 60.0f, 800);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertNull("Long + Float is incompatible, should clear min", merged.columnMin("val"));
        assertNull("Long + Float is incompatible, should clear max", merged.columnMax("val"));
    }

    public void testMergeIncompatibleStaysPoisonedWithThirdSplit() {
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("val", 0L, 10L, 50L, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("val", 0L, 5.0, 60.0, 800);
        SplitStats s2 = b2.build();

        SplitStats.Builder b3 = new SplitStats.Builder().rowCount(300);
        b3.addColumn("val", 0L, 1L, 100L, 1200);
        SplitStats s3 = b3.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2, s3));
        assertNotNull(merged);
        assertEquals(600, merged.rowCount());
        assertNull("once poisoned by incompatibility, min must stay null", merged.columnMin("val"));
        assertNull("once poisoned by incompatibility, max must stay null", merged.columnMax("val"));
    }

    public void testMergeCrossTypeIntegerLongReversedOrder() {
        // Verify widening works regardless of which split comes first
        SplitStats.Builder b1 = new SplitStats.Builder().rowCount(100);
        b1.addColumn("price", 0L, 5L, 60L, 400);
        SplitStats s1 = b1.build();

        SplitStats.Builder b2 = new SplitStats.Builder().rowCount(200);
        b2.addColumn("price", 0L, 10, 50, 800);
        SplitStats s2 = b2.build();

        SplitStats merged = SplitStats.merge(List.of(s1, s2));
        assertNotNull(merged);
        assertEquals(5L, merged.columnMin("price"));
        assertEquals(60L, merged.columnMax("price"));
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

    private static FileSplit fileSplitWithStats(SplitStats stats) {
        Map<String, Object> statsMap = stats.toMap();
        return new FileSplit("parquet", StoragePath.of("file:///test.parquet"), 0, 100, "parquet", Map.of(), Map.of(), null, statsMap);
    }
}
