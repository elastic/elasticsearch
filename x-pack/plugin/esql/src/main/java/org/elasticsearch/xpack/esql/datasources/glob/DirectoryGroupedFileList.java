/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

/**
 * Compact file listing that groups files by the relative directory they were listed under. Each file
 * carries the index of its directory; {@link #path(int)} replays {@code basePath + directory + leaf}
 * verbatim, so the reconstructed key is byte-for-byte the one that was listed — value spelling, segment
 * order, and any non-partition directories all survive, because nothing is re-derived from parsed
 * partition values. Beats {@link DictionaryFileList} on layouts with many files per directory and few
 * distinct directories; created by {@link FileListCompactor}.
 */
final class DirectoryGroupedFileList implements FileList {

    private final String basePath;
    private final String[] groupDirs;
    private final short[] fileGroups;
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
    /**
     * File-set fingerprint carried over from the raw list this was compacted from — compaction preserves
     * the file set exactly and the fingerprint is order-independent, so the pass-through value equals what
     * a recomputation over the compacted representation would produce, without re-materializing paths.
     */
    @Nullable
    private final FileSetFingerprint fileSetFingerprint;

    DirectoryGroupedFileList(
        String basePath,
        String[] groupDirs,
        short[] fileGroups,
        long[] sizes,
        @Nullable long[] mtimesMillis,
        @Nullable long[] groupMtimes,
        String[] leafNames,
        @Nullable String sharedExtension,
        @Nullable String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        int fileCount,
        @Nullable FileSetFingerprint fileSetFingerprint
    ) {
        this.basePath = basePath;
        this.groupDirs = groupDirs;
        this.fileGroups = fileGroups;
        this.sizes = sizes;
        this.mtimesMillis = mtimesMillis;
        this.groupMtimes = groupMtimes;
        this.leafNames = leafNames;
        this.sharedExtension = sharedExtension;
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        this.fileCount = fileCount;
        this.fileSetFingerprint = fileSetFingerprint;
    }

    @Override
    @Nullable
    public FileSetFingerprint fileSetFingerprint() {
        return fileSetFingerprint;
    }

    @Override
    public int fileCount() {
        return fileCount;
    }

    @Override
    public StoragePath path(int i) {
        StringBuilder sb = new StringBuilder(basePath);
        sb.append(groupDirs[Short.toUnsignedInt(fileGroups[i])]);
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
            return groupMtimes[Short.toUnsignedInt(fileGroups[i])];
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
    public long estimatedBytes() {
        // object header + reference fields
        long bytes = 64;
        // basePath String
        bytes += basePath.length() * (long) Character.BYTES;
        // one String per distinct relative directory; the group keys are distinct by construction
        for (String dir : groupDirs) {
            // per-String: ~40B object overhead + char data
            bytes += 40 + dir.length() * (long) Character.BYTES;
        }
        // per-file group index
        bytes += fileGroups.length * (long) Short.BYTES;
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
        return bytes;
    }
}
