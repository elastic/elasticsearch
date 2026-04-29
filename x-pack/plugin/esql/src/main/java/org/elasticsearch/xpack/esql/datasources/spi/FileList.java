/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.SplitStats;

import java.util.Map;

/**
 * Indexed view over a resolved set of files from an external data source.
 * Implementations are package-private within the {@code datasources.glob} package;
 * consumers should depend only on this interface.
 * <p>
 * Two sentinel instances encode distinct states:
 * <ul>
 *   <li>{@link #UNRESOLVED} — glob not yet expanded ({@code isResolved() == false, isEmpty() == false})</li>
 *   <li>{@link #EMPTY} — glob expanded but matched zero files ({@code isResolved() == true, isEmpty() == true})</li>
 * </ul>
 */
public interface FileList {

    /** Sentinel: single-file path, glob not yet applied ({@code isResolved() == false}). */
    FileList UNRESOLVED = new FileList() {
        @Override
        public int fileCount() {
            return 0;
        }

        @Override
        public StoragePath path(int i) {
            return null;
        }

        @Override
        public long size(int i) {
            return 0;
        }

        @Override
        public long lastModifiedMillis(int i) {
            return 0;
        }

        @Override
        public String originalPattern() {
            return null;
        }

        @Override
        public PartitionMetadata partitionMetadata() {
            return null;
        }

        @Override
        public boolean isResolved() {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public long estimatedBytes() {
            return 0;
        }

        @Override
        public String toString() {
            return "FileList[UNRESOLVED]";
        }
    };

    /** Sentinel: glob expanded but matched zero files ({@code isResolved() == true, isEmpty() == true}). */
    FileList EMPTY = new FileList() {
        @Override
        public int fileCount() {
            return 0;
        }

        @Override
        public StoragePath path(int i) {
            return null;
        }

        @Override
        public long size(int i) {
            return 0;
        }

        @Override
        public long lastModifiedMillis(int i) {
            return 0;
        }

        @Override
        public String originalPattern() {
            return null;
        }

        @Override
        public PartitionMetadata partitionMetadata() {
            return null;
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public long estimatedBytes() {
            return 0;
        }

        @Override
        public String toString() {
            return "FileList[EMPTY]";
        }
    };

    int fileCount();

    StoragePath path(int i);

    long size(int i);

    long lastModifiedMillis(int i);

    @Nullable
    String originalPattern();

    @Nullable
    PartitionMetadata partitionMetadata();

    boolean isResolved();

    boolean isEmpty();

    long estimatedBytes();

    /**
     * Per-file schema info from schema reconciliation, or {@code null} when reconciliation
     * was not performed (e.g. first-file-wins mode or compact file lists).
     */
    @Nullable
    default Map<StoragePath, SchemaReconciliation.FileSchemaInfo> fileSchemaInfo() {
        return null;
    }

    /**
     * Number of pre-resolved split ranges for file {@code i}.
     * Returns {@code -1} if ranges were not resolved (caller should use slow path),
     * {@code 0} if resolved but the file has no independently-readable ranges,
     * or a positive count of available ranges.
     */
    default int rangeCount(int i) {
        return -1;
    }

    /**
     * Byte offset of range {@code r} within file {@code i}.
     * Only valid when {@code rangeCount(i) > 0} and {@code 0 <= r < rangeCount(i)}.
     */
    default long rangeOffset(int i, int r) {
        return 0L;
    }

    /**
     * Byte length of range {@code r} within file {@code i}.
     * Only valid when {@code rangeCount(i) > 0} and {@code 0 <= r < rangeCount(i)}.
     */
    default long rangeLength(int i, int r) {
        return size(i);
    }

    /**
     * Compact statistics for range {@code r} within file {@code i}, or {@code null} if unavailable.
     */
    @Nullable
    default SplitStats rangeStats(int i, int r) {
        return null;
    }
}
