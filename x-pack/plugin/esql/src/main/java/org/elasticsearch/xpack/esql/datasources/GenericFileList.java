/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a set of files resolved from a glob pattern or comma-separated path list.
 * Uses identity-comparable sentinels for unresolved and empty states.
 * Optionally carries {@link PartitionMetadata} detected from Hive-style file paths.
 */
public final class GenericFileList implements FileList {

    /** Single-file path, no glob applied yet. */
    public static final GenericFileList UNRESOLVED = new GenericFileList(List.of(), null);

    /** Glob matched zero files. */
    public static final GenericFileList EMPTY = new GenericFileList(List.of(), null);

    private final List<StorageEntry> files;
    private final String originalPattern;
    private final PartitionMetadata partitionMetadata;
    private final Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo;

    public GenericFileList(List<StorageEntry> files, String originalPattern) {
        this(files, originalPattern, null, null);
    }

    public GenericFileList(List<StorageEntry> files, String originalPattern, @Nullable PartitionMetadata partitionMetadata) {
        this(files, originalPattern, partitionMetadata, null);
    }

    public GenericFileList(
        List<StorageEntry> files,
        String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        @Nullable Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo
    ) {
        if (files == null) {
            throw new IllegalArgumentException("files cannot be null");
        }
        this.files = List.copyOf(files);
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        this.fileSchemaInfo = fileSchemaInfo;
    }

    public List<StorageEntry> files() {
        return files;
    }

    public String originalPattern() {
        return originalPattern;
    }

    @Nullable
    public PartitionMetadata partitionMetadata() {
        return partitionMetadata;
    }

    @Nullable
    public Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo() {
        return fileSchemaInfo;
    }

    /**
     * Returns a new GenericFileList with per-file schema info attached.
     * Used by schema reconciliation to pass column mappings from planning to split discovery.
     */
    public GenericFileList withSchemaInfo(Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo) {
        return new GenericFileList(files, originalPattern, partitionMetadata, schemaInfo);
    }

    public int size() {
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

    public boolean isUnresolved() {
        return this == UNRESOLVED;
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public boolean isResolved() {
        return this != UNRESOLVED && this != EMPTY;
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
        if (this == UNRESOLVED || this == EMPTY || other == UNRESOLVED || other == EMPTY) {
            return false;
        }
        return Objects.equals(files, other.files)
            && Objects.equals(originalPattern, other.originalPattern)
            && Objects.equals(partitionMetadata, other.partitionMetadata);
    }

    @Override
    public int hashCode() {
        if (this == UNRESOLVED || this == EMPTY) {
            return System.identityHashCode(this);
        }
        return Objects.hash(files, originalPattern, partitionMetadata);
    }

    @Override
    public String toString() {
        if (this == UNRESOLVED) {
            return "GenericFileList[UNRESOLVED]";
        }
        if (this == EMPTY) {
            return "GenericFileList[EMPTY]";
        }
        return "GenericFileList[" + files.size() + " files, pattern=" + originalPattern + "]";
    }
}
