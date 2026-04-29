/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenericFileListTests extends ESTestCase {

    public void testUnresolvedIdentity() {
        assertSame(FileList.UNRESOLVED, FileList.UNRESOLVED);
    }

    public void testEmptyIdentity() {
        assertSame(FileList.EMPTY, FileList.EMPTY);
    }

    public void testUnresolvedIsNotEmpty() {
        assertNotSame(FileList.UNRESOLVED, FileList.EMPTY);
    }

    public void testIsResolvedForRegularGenericFileList() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://bucket/*.parquet");
        assertTrue(fileList.isResolved());
        assertFalse(fileList.isEmpty());
    }

    public void testUnresolvedSentinelSemantics() {
        assertFalse(FileList.UNRESOLVED.isResolved());
        assertFalse(FileList.UNRESOLVED.isEmpty());
    }

    public void testEmptySentinelSemantics() {
        assertTrue(FileList.EMPTY.isResolved());
        assertTrue(FileList.EMPTY.isEmpty());
    }

    public void testSizeMatchesFiles() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 10, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 20, Instant.EPOCH);
        GenericFileList fileList = (GenericFileList) GlobExpander.fileListOf(List.of(e1, e2), "s3://b/*.parquet");
        assertEquals(fileList.files().size(), fileList.size());
        assertEquals(2, fileList.size());
    }

    public void testOriginalPatternPreserved() {
        String pattern = "s3://bucket/data/**" + "/*.parquet";
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/data/sub/file.parquet"), 50, Instant.EPOCH);
        GenericFileList fileList = (GenericFileList) GlobExpander.fileListOf(List.of(entry), pattern);
        assertEquals(pattern, fileList.originalPattern());
    }

    public void testSentinelsNotEqual() {
        assertNotEquals(FileList.UNRESOLVED, FileList.EMPTY);
        assertNotEquals(FileList.EMPTY, FileList.UNRESOLVED);
        assertNotEquals(FileList.UNRESOLVED.hashCode(), FileList.EMPTY.hashCode());
    }

    public void testPartitionMetadataNullByDefault() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://bucket/*.parquet");
        assertNull(fileList.partitionMetadata());
    }

    public void testPartitionMetadataAttached() {
        StoragePath path = StoragePath.of("s3://bucket/year=2024/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        PartitionMetadata pm = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(path, Map.of("year", 2024)));
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        assertNotNull(fileList.partitionMetadata());
        assertFalse(fileList.partitionMetadata().isEmpty());
        assertEquals(DataType.INTEGER, fileList.partitionMetadata().partitionColumns().get("year"));
    }

    public void testSentinelsHaveNullPartitionMetadata() {
        assertNull(FileList.UNRESOLVED.partitionMetadata());
        assertNull(FileList.EMPTY.partitionMetadata());
    }

    public void testFileSplitRangesNullByDefault() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        GenericFileList fileList = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/*.parquet");
        assertNull(fileList.fileSplitRanges());
    }

    public void testWithFileSplitRangesAttachesAndPreservesFiles() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        GenericFileList fileList = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/*.parquet");

        RangeAwareFormatReader.SplitRange r = new RangeAwareFormatReader.SplitRange(0, 100, Map.of("_stats.row_count", 10L));
        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> ranges = Map.of(path, List.of(r));

        GenericFileList withRanges = fileList.withFileSplitRanges(ranges);
        assertNotNull(withRanges.fileSplitRanges());
        assertEquals(1, withRanges.fileSplitRanges().size());
        assertEquals(List.of(r), withRanges.fileSplitRanges().get(path));
        assertEquals(fileList.files(), withRanges.files());
        assertEquals(fileList.originalPattern(), withRanges.originalPattern());
        assertNull(withRanges.partitionMetadata());
    }

    public void testEqualityDistinguishesFileSplitRanges() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        GenericFileList base = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/*.parquet");

        RangeAwareFormatReader.SplitRange r = new RangeAwareFormatReader.SplitRange(0, 100, Map.of("_stats.row_count", 10L));
        GenericFileList withRanges = base.withFileSplitRanges(Map.of(path, List.of(r)));

        assertNotEquals(base, withRanges);
        assertNotEquals(base.hashCode(), withRanges.hashCode());
    }

    public void testEqualityWithPartitionMetadata() {
        StoragePath path = StoragePath.of("s3://bucket/year=2024/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        PartitionMetadata pm = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(path, Map.of("year", 2024)));
        GenericFileList a = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        GenericFileList b = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        GenericFileList c = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", null);

        assertEquals(a, b);
        assertNotEquals(a, c);
    }

    public void testCompactRoundTripPreservesRanges() {
        StoragePath p1 = StoragePath.of("s3://bucket/data/file1.parquet");
        StoragePath p2 = StoragePath.of("s3://bucket/data/file2.parquet");

        RangeAwareFormatReader.SplitRange r1 = new RangeAwareFormatReader.SplitRange(4, 100, Map.of("_stats.row_count", 50L));
        RangeAwareFormatReader.SplitRange r2 = new RangeAwareFormatReader.SplitRange(104, 200, Map.of("_stats.row_count", 75L));
        RangeAwareFormatReader.SplitRange r3 = new RangeAwareFormatReader.SplitRange(8, 150, Map.of("_stats.row_count", 60L));

        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath = new HashMap<>();
        rangesByPath.put(p1, List.of(r1, r2));
        rangesByPath.put(p2, List.of(r3));

        List<StorageEntry> entries = List.of(new StorageEntry(p1, 300, Instant.EPOCH), new StorageEntry(p2, 150, Instant.EPOCH));

        FileList raw = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");
        FileList withRanges = GlobExpander.withFileSplitRanges(raw, rangesByPath);
        FileList compact = GlobExpander.compact(withRanges, "s3://bucket/data/");

        assertEquals(2, compact.fileCount());

        int idx1 = -1, idx2 = -1;
        for (int i = 0; i < compact.fileCount(); i++) {
            if (compact.path(i).equals(p1)) idx1 = i;
            if (compact.path(i).equals(p2)) idx2 = i;
        }
        assertTrue(idx1 >= 0);
        assertTrue(idx2 >= 0);

        assertEquals(2, compact.rangeCount(idx1));
        assertEquals(r1.offset(), compact.rangeOffset(idx1, 0));
        assertEquals(r1.length(), compact.rangeLength(idx1, 0));
        assertEquals(r2.offset(), compact.rangeOffset(idx1, 1));
        assertEquals(r2.length(), compact.rangeLength(idx1, 1));

        assertEquals(1, compact.rangeCount(idx2));
        assertEquals(r3.offset(), compact.rangeOffset(idx2, 0));
        assertEquals(r3.length(), compact.rangeLength(idx2, 0));

        assertNotNull(compact.rangeStats(idx1, 0));
        assertEquals(50L, compact.rangeStats(idx1, 0).rowCount());
        assertNotNull(compact.rangeStats(idx2, 0));
        assertEquals(60L, compact.rangeStats(idx2, 0).rowCount());
    }
}
