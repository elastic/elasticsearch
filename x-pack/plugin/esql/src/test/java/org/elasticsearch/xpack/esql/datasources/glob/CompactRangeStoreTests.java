/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.Map;

public class CompactRangeStoreTests extends ESTestCase {

    public void testBuildReturnsNullForNullRanges() {
        CompactRangeStore store = CompactRangeStore.build(3, i -> StoragePath.of("s3://bucket/file_" + i + ".parquet"), null);
        assertNull(store);
    }

    public void testBuildReturnsNullForEmptyRanges() {
        CompactRangeStore store = CompactRangeStore.build(3, i -> StoragePath.of("s3://bucket/file_" + i + ".parquet"), Map.of());
        assertNull(store);
    }

    public void testSingleFileSingleRange() {
        StoragePath path = StoragePath.of("s3://bucket/file_0.parquet");
        long offset = 4;
        long length = 128 * 1024 * 1024L; // 128 MB
        RangeAwareFormatReader.SplitRange range = new RangeAwareFormatReader.SplitRange(offset, length, Map.of("_stats.row_count", 100L));
        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = Map.of(path, List.of(range));

        CompactRangeStore store = CompactRangeStore.build(1, i -> path, rangesByPath);
        assertNotNull(store);
        assertEquals(1, store.rangeCount(0));
        assertEquals(offset, store.rangeOffset(0, 0));
        assertEquals(length, store.rangeLength(0, 0));
        assertNotNull(store.rangeStats(0, 0));
        assertEquals(100L, store.rangeStats(0, 0).rowCount());
    }

    public void testSingleFileMultipleRanges() {
        StoragePath path = StoragePath.of("s3://bucket/file_0.parquet");
        long len1 = 128 * 1024 * 1024L;
        long len2 = 128 * 1024 * 1024L;
        long len3 = 64 * 1024 * 1024L;
        long baseOffset = 4;

        RangeAwareFormatReader.SplitRange r0 = new RangeAwareFormatReader.SplitRange(baseOffset, len1, Map.of("_stats.row_count", 50L));
        RangeAwareFormatReader.SplitRange r1 = new RangeAwareFormatReader.SplitRange(
            baseOffset + len1,
            len2,
            Map.of("_stats.row_count", 50L)
        );
        RangeAwareFormatReader.SplitRange r2 = new RangeAwareFormatReader.SplitRange(
            baseOffset + len1 + len2,
            len3,
            Map.of("_stats.row_count", 25L)
        );

        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = Map.of(path, List.of(r0, r1, r2));
        CompactRangeStore store = CompactRangeStore.build(1, i -> path, rangesByPath);
        assertNotNull(store);

        assertEquals(3, store.rangeCount(0));
        // Verify cumulative offset derivation
        assertEquals(baseOffset, store.rangeOffset(0, 0));
        assertEquals(baseOffset + len1, store.rangeOffset(0, 1));
        assertEquals(baseOffset + len1 + len2, store.rangeOffset(0, 2));
        // Verify lengths
        assertEquals(len1, store.rangeLength(0, 0));
        assertEquals(len2, store.rangeLength(0, 1));
        assertEquals(len3, store.rangeLength(0, 2));
    }

    public void testMultiFileIdenticalGeometry() {
        long len1 = 128 * 1024 * 1024L;
        long len2 = 128 * 1024 * 1024L;
        long len3 = 64 * 1024 * 1024L;

        StoragePath[] paths = new StoragePath[3];
        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = new java.util.HashMap<>();
        for (int f = 0; f < 3; f++) {
            paths[f] = StoragePath.of("s3://bucket/file_" + f + ".parquet");
            long base = f * 1000L;
            rangesByPath.put(
                paths[f],
                List.of(
                    new RangeAwareFormatReader.SplitRange(base, len1, Map.of("_stats.row_count", 10L)),
                    new RangeAwareFormatReader.SplitRange(base + len1, len2, Map.of("_stats.row_count", 10L)),
                    new RangeAwareFormatReader.SplitRange(base + len1 + len2, len3, Map.of("_stats.row_count", 5L))
                )
            );
        }

        CompactRangeStore store = CompactRangeStore.build(3, i -> paths[i], rangesByPath);
        assertNotNull(store);

        // All files should have identical range lengths (same template)
        for (int f = 0; f < 3; f++) {
            assertEquals(3, store.rangeCount(f));
            assertEquals(len1, store.rangeLength(f, 0));
            assertEquals(len2, store.rangeLength(f, 1));
            assertEquals(len3, store.rangeLength(f, 2));
        }
        // Verify template sharing: all files return the same lengths, confirming template dedup
        assertEquals(store.rangeLength(0, 0), store.rangeLength(1, 0));
        assertEquals(store.rangeLength(0, 1), store.rangeLength(2, 1));
        assertEquals(store.rangeLength(0, 2), store.rangeLength(2, 2));
    }

    public void testMultiFileDifferentGeometry() {
        StoragePath path0 = StoragePath.of("s3://bucket/file_0.parquet");
        StoragePath path1 = StoragePath.of("s3://bucket/file_1.parquet");

        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = new java.util.HashMap<>();
        // File 0: 2 ranges of 128MB each
        rangesByPath.put(
            path0,
            List.of(
                new RangeAwareFormatReader.SplitRange(0, 128 * 1024 * 1024L, Map.of("_stats.row_count", 10L)),
                new RangeAwareFormatReader.SplitRange(128 * 1024 * 1024L, 128 * 1024 * 1024L, Map.of("_stats.row_count", 10L))
            )
        );
        // File 1: 1 range of 64MB
        rangesByPath.put(path1, List.of(new RangeAwareFormatReader.SplitRange(0, 64 * 1024 * 1024L, Map.of("_stats.row_count", 5L))));

        StoragePath[] paths = { path0, path1 };
        CompactRangeStore store = CompactRangeStore.build(2, i -> paths[i], rangesByPath);
        assertNotNull(store);

        assertEquals(2, store.rangeCount(0));
        assertEquals(1, store.rangeCount(1));
        assertEquals(128 * 1024 * 1024L, store.rangeLength(0, 0));
        assertEquals(128 * 1024 * 1024L, store.rangeLength(0, 1));
        assertEquals(64 * 1024 * 1024L, store.rangeLength(1, 0));
    }

    public void testNullStats() {
        StoragePath path = StoragePath.of("s3://bucket/file_0.parquet");
        // Empty statistics map -> SplitStats.of returns null
        RangeAwareFormatReader.SplitRange range = new RangeAwareFormatReader.SplitRange(0, 1024);

        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = Map.of(path, List.of(range));
        CompactRangeStore store = CompactRangeStore.build(1, i -> path, rangesByPath);
        assertNotNull(store);
        assertNull(store.rangeStats(0, 0));
    }

    public void testStatsDedup() {
        Map<String, Object> statsMap = Map.of("_stats.row_count", 100L);
        StoragePath path0 = StoragePath.of("s3://bucket/file_0.parquet");
        StoragePath path1 = StoragePath.of("s3://bucket/file_1.parquet");
        StoragePath path2 = StoragePath.of("s3://bucket/file_2.parquet");

        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = new java.util.HashMap<>();
        rangesByPath.put(path0, List.of(new RangeAwareFormatReader.SplitRange(0, 1024, statsMap)));
        rangesByPath.put(path1, List.of(new RangeAwareFormatReader.SplitRange(0, 1024, statsMap)));
        rangesByPath.put(path2, List.of(new RangeAwareFormatReader.SplitRange(0, 1024, statsMap)));

        StoragePath[] paths = { path0, path1, path2 };
        CompactRangeStore store = CompactRangeStore.build(3, i -> paths[i], rangesByPath);
        assertNotNull(store);

        // All three ranges should share the same SplitStats instance (reference equality)
        assertSame(store.rangeStats(0, 0), store.rangeStats(1, 0));
        assertSame(store.rangeStats(1, 0), store.rangeStats(2, 0));
    }

    public void testEstimatedBytesPositive() {
        StoragePath path = StoragePath.of("s3://bucket/file_0.parquet");
        RangeAwareFormatReader.SplitRange range = new RangeAwareFormatReader.SplitRange(0, 1024, Map.of("_stats.row_count", 10L));

        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = Map.of(path, List.of(range));
        CompactRangeStore store = CompactRangeStore.build(1, i -> path, rangesByPath);
        assertNotNull(store);
        assertTrue(store.estimatedBytes() > 0);
    }
}
