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

import java.io.IOException;
import java.util.HashMap;
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
}
