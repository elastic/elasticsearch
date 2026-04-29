/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a set of files resolved from a glob pattern or comma-separated path list.
 * Optionally carries {@link PartitionMetadata} detected from Hive-style file paths.
 */
final class GenericFileList implements FileList {

    private final List<StorageEntry> files;
    private final String originalPattern;
    private final PartitionMetadata partitionMetadata;
    private final Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo;
    private final Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> fileSplitRanges;

    GenericFileList(List<StorageEntry> files, String originalPattern) {
        this(files, originalPattern, null, null, null);
    }

    GenericFileList(List<StorageEntry> files, String originalPattern, @Nullable PartitionMetadata partitionMetadata) {
        this(files, originalPattern, partitionMetadata, null, null);
    }

    GenericFileList(
        List<StorageEntry> files,
        String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        @Nullable Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo
    ) {
        this(files, originalPattern, partitionMetadata, fileSchemaInfo, null);
    }

    GenericFileList(
        List<StorageEntry> files,
        String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        @Nullable Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo,
        @Nullable Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> fileSplitRanges
    ) {
        if (files == null) {
            throw new IllegalArgumentException("files cannot be null");
        }
        this.files = files;
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        this.fileSchemaInfo = fileSchemaInfo;
        this.fileSplitRanges = fileSplitRanges;
    }

    List<StorageEntry> files() {
        return files;
    }

    @Override
    public String originalPattern() {
        return originalPattern;
    }

    @Override
    @Nullable
    public PartitionMetadata partitionMetadata() {
        return partitionMetadata;
    }

    @Override
    @Nullable
    public Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo() {
        return fileSchemaInfo;
    }

    @Nullable
    Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> fileSplitRanges() {
        return fileSplitRanges;
    }

    @Override
    public int rangeCount(int i) {
        if (fileSplitRanges == null) {
            return -1;
        }
        StoragePath p = path(i);
        List<RangeAwareFormatReader.SplitRange> ranges = fileSplitRanges.get(p);
        return ranges != null ? ranges.size() : 0;
    }

    @Override
    public long rangeOffset(int i, int r) {
        return fileSplitRanges.get(path(i)).get(r).offset();
    }

    @Override
    public long rangeLength(int i, int r) {
        return fileSplitRanges.get(path(i)).get(r).length();
    }

    @Override
    @Nullable
    public SplitStats rangeStats(int i, int r) {
        Map<String, Object> stats = fileSplitRanges.get(path(i)).get(r).statistics();
        return (stats == null || stats.isEmpty()) ? null : SplitStats.of(stats);
    }

    /**
     * Returns a new GenericFileList with per-file schema info attached.
     * Used by schema reconciliation to pass column mappings from planning to split discovery.
     */
    GenericFileList withSchemaInfo(Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo) {
        return new GenericFileList(files, originalPattern, partitionMetadata, schemaInfo, fileSplitRanges);
    }

    /**
     * Returns a new GenericFileList with pre-resolved per-file split ranges attached.
     * Captured during single-pass file layout resolution (Parquet/ORC) and consumed by
     * split discovery to avoid re-reading file footers.
     */
    GenericFileList withFileSplitRanges(Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> splitRanges) {
        return new GenericFileList(files, originalPattern, partitionMetadata, fileSchemaInfo, splitRanges);
    }

    int size() {
        return files.size();
    }

    @Override
    public int fileCount() {
        return files.size();
    }

    @Override
    public StoragePath path(int i) {
        return files.get(i).path();
    }

    @Override
    public long size(int i) {
        return files.get(i).length();
    }

    @Override
    public long lastModifiedMillis(int i) {
        return files.get(i).lastModified().toEpochMilli();
    }

    @Override
    public long estimatedBytes() {
        // 64B object header + ~700B per StorageEntry (path String + Instant + long)
        return 64 + files.size() * 700L;
    }

    @Override
    public boolean isResolved() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return files.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericFileList other = (GenericFileList) o;
        return Objects.equals(files, other.files)
            && Objects.equals(originalPattern, other.originalPattern)
            && Objects.equals(partitionMetadata, other.partitionMetadata)
            && Objects.equals(fileSchemaInfo, other.fileSchemaInfo)
            && Objects.equals(fileSplitRanges, other.fileSplitRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files, originalPattern, partitionMetadata, fileSchemaInfo, fileSplitRanges);
    }

    @Override
    public String toString() {
        return "GenericFileList[" + files.size() + " files, pattern=" + originalPattern + "]";
    }
}
