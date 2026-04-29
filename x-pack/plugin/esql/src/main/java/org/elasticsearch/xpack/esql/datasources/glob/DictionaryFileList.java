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
 * Segment-dictionary-encoded file listing implementing {@link FileList}.
 * Compresses path storage from ~700 bytes/file (StorageEntry) to ~52 bytes/file
 * by interning path segments into a shared dictionary and referencing them by index.
 */
final class DictionaryFileList implements FileList {

    /** Store basePath exactly as the common prefix string (e.g. "s3://bucket/data/") */
    private final String basePath;

    private final String[] tokens; // dictionary: index → segment string
    private final short[] pathTokens; // flat array of token indices for all paths
    private final int[] pathStarts; // start index in pathTokens for each file
    private final long[] sizes; // per-file size
    private final long[] mtimesMillis; // per-file mtime
    @Nullable
    private final String sharedExtension; // common extension (e.g. ".parquet")
    @Nullable
    private final String originalPattern;
    @Nullable
    private final PartitionMetadata partitionMetadata;
    private final int fileCount;
    @Nullable
    private final CompactRangeStore ranges;

    DictionaryFileList(
        String basePath,
        String[] tokens,
        short[] pathTokens,
        int[] pathStarts,
        long[] sizes,
        long[] mtimesMillis,
        @Nullable String sharedExtension,
        @Nullable String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        int fileCount,
        @Nullable CompactRangeStore ranges
    ) {
        this.basePath = basePath;
        this.tokens = tokens;
        this.pathTokens = pathTokens;
        this.pathStarts = pathStarts;
        this.sizes = sizes;
        this.mtimesMillis = mtimesMillis;
        this.sharedExtension = sharedExtension;
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        this.fileCount = fileCount;
        this.ranges = ranges;
    }

    DictionaryFileList withRanges(CompactRangeStore ranges) {
        return new DictionaryFileList(
            basePath,
            tokens,
            pathTokens,
            pathStarts,
            sizes,
            mtimesMillis,
            sharedExtension,
            originalPattern,
            partitionMetadata,
            fileCount,
            ranges
        );
    }

    @Override
    public int fileCount() {
        return fileCount;
    }

    @Override
    public StoragePath path(int i) {
        StringBuilder sb = new StringBuilder(basePath);
        int start = pathStarts[i];
        int end = pathStarts[i + 1];
        for (int t = start; t < end; t++) {
            if (t > start) {
                sb.append('/');
            }
            sb.append(tokens[Short.toUnsignedInt(pathTokens[t])]);
        }
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
        // basePath String: object header (40B) + char data
        bytes += basePath.length() * (long) Character.BYTES;
        for (String token : tokens) {
            // per-String: ~40B object overhead + char data
            bytes += 40 + token.length() * (long) Character.BYTES;
        }
        bytes += pathTokens.length * (long) Short.BYTES;
        bytes += (long) pathStarts.length * Integer.BYTES;
        bytes += sizes.length * (long) Long.BYTES;
        bytes += mtimesMillis.length * (long) Long.BYTES;
        if (sharedExtension != null) {
            bytes += 40 + sharedExtension.length() * (long) Character.BYTES;
        }
        return bytes + (ranges != null ? ranges.estimatedBytes() : 0);
    }

}
