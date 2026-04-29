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
}
