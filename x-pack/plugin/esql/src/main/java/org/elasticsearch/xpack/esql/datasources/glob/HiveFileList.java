/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

/**
 * Partition-grouped file listing for Hive-partitioned data. Stores column value
 * dictionaries and groups files by partition, reducing memory to ~32 bytes/file
 * vs ~52 bytes for DictionaryFileList. Created by {@link FileListCompactor}.
 */
final class HiveFileList implements FileList {

    private final String basePath;
    private final String[] partitionColumnNames;
    private final String[][] columnValueDicts;
    private final short[][] groupValueIndices;
    private final int[] groupFileStarts;
    private final long[] sizes;
    @Nullable
    private final long[] mtimesMillis;
    @Nullable
    private final long[] groupMtimes;
    private final String[] leafNames;
    @Nullable
    private final String sharedExtension;
    @Nullable
    private final String originalPattern;
    @Nullable
    private final PartitionMetadata partitionMetadata;
    private final int fileCount;
    @Nullable
    private final CompactRangeStore ranges;

    HiveFileList(
        String basePath,
        String[] partitionColumnNames,
        String[][] columnValueDicts,
        short[][] groupValueIndices,
        int[] groupFileStarts,
        long[] sizes,
        @Nullable long[] mtimesMillis,
        @Nullable long[] groupMtimes,
        String[] leafNames,
        @Nullable String sharedExtension,
        @Nullable String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        int fileCount,
        @Nullable CompactRangeStore ranges
    ) {
        this.basePath = basePath;
        this.partitionColumnNames = partitionColumnNames;
        this.columnValueDicts = columnValueDicts;
        this.groupValueIndices = groupValueIndices;
        this.groupFileStarts = groupFileStarts;
        this.sizes = sizes;
        this.mtimesMillis = mtimesMillis;
        this.groupMtimes = groupMtimes;
        this.leafNames = leafNames;
        this.sharedExtension = sharedExtension;
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        this.fileCount = fileCount;
        this.ranges = ranges;
    }

    HiveFileList withRanges(CompactRangeStore ranges) {
        return new HiveFileList(
            basePath,
            partitionColumnNames,
            columnValueDicts,
            groupValueIndices,
            groupFileStarts,
            sizes,
            mtimesMillis,
            groupMtimes,
            leafNames,
            sharedExtension,
            originalPattern,
            partitionMetadata,
            fileCount,
            ranges
        );
    }

    private int findGroup(int fileIndex) {
        int lo = 0;
        int hi = groupFileStarts.length - 2;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (groupFileStarts[mid] <= fileIndex) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    @Override
    public int fileCount() {
        return fileCount;
    }

    @Override
    public StoragePath path(int i) {
        int group = findGroup(i);
        StringBuilder sb = new StringBuilder(basePath);
        for (int c = 0; c < partitionColumnNames.length; c++) {
            sb.append(partitionColumnNames[c]).append('=');
            sb.append(columnValueDicts[c][Short.toUnsignedInt(groupValueIndices[group][c])]);
            sb.append('/');
        }
        sb.append(leafNames[i]);
        if (sharedExtension != null) {
            sb.append(sharedExtension);
        }
        return StoragePath.of(sb.toString());
    }

    @Override
    public long size(int i) {
        return sizes[i];
    }

    @Override
    public long lastModifiedMillis(int i) {
        if (groupMtimes != null) {
            return groupMtimes[findGroup(i)];
        }
        return mtimesMillis[i];
    }

    @Override
    @Nullable
    public String originalPattern() {
        return originalPattern;
    }

    @Override
    @Nullable
    public PartitionMetadata partitionMetadata() {
        return partitionMetadata;
    }

    @Override
    public boolean isResolved() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return fileCount == 0;
    }

    @Override
    public int rangeCount(int i) {
        return ranges != null ? ranges.rangeCount(i) : -1;
    }

    @Override
    public long rangeOffset(int i, int r) {
        return ranges != null ? ranges.rangeOffset(i, r) : 0L;
    }

    @Override
    public long rangeLength(int i, int r) {
        return ranges != null ? ranges.rangeLength(i, r) : size(i);
    }

    @Override
    @Nullable
    public SplitStats rangeStats(int i, int r) {
        return ranges != null ? ranges.rangeStats(i, r) : null;
    }

    @Override
    public long estimatedBytes() {
        // object header + reference fields
        long bytes = 64;
        // basePath String
        bytes += basePath.length() * (long) Character.BYTES;
        for (String[] dict : columnValueDicts) {
            for (String v : dict) {
                // per-String: ~40B object overhead + char data
                bytes += 40 + v.length() * (long) Character.BYTES;
            }
        }
        // partition value indices: groups × columns × short
        bytes += (long) groupValueIndices.length * partitionColumnNames.length * Short.BYTES;
        bytes += (long) groupFileStarts.length * Integer.BYTES;
        bytes += sizes.length * (long) Long.BYTES;
        if (mtimesMillis != null) {
            bytes += mtimesMillis.length * (long) Long.BYTES;
        }
        if (groupMtimes != null) {
            bytes += groupMtimes.length * (long) Long.BYTES;
        }
        for (String leaf : leafNames) {
            bytes += 40 + leaf.length() * (long) Character.BYTES;
        }
        if (sharedExtension != null) {
            bytes += 40 + sharedExtension.length() * (long) Character.BYTES;
        }
        return bytes + (ranges != null ? ranges.estimatedBytes() : 0);
    }
}
