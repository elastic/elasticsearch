/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.util.Map;

/**
 * Holds pre-fetched metadata for a Parquet file's row groups: column indexes, offset indexes,
 * and dictionary pages. Populated in parallel via {@link CoalescedRangeReader} during the
 * optimized reader's initialization phase.
 *
 * <p>This class is populated once during file open and then read concurrently during row group
 * processing. All fields are effectively immutable after construction.
 */
final class PreloadedRowGroupMetadata {

    /**
     * Per-row-group, per-column metadata keyed by {@code "rowGroup:columnPath"}.
     */
    private final Map<String, ColumnIndex> columnIndexes;
    private final Map<String, OffsetIndex> offsetIndexes;

    PreloadedRowGroupMetadata(Map<String, ColumnIndex> columnIndexes, Map<String, OffsetIndex> offsetIndexes) {
        this.columnIndexes = Map.copyOf(columnIndexes);
        this.offsetIndexes = Map.copyOf(offsetIndexes);
    }

    static PreloadedRowGroupMetadata empty() {
        return new PreloadedRowGroupMetadata(Map.of(), Map.of());
    }

    ColumnIndex getColumnIndex(int rowGroupOrdinal, String columnPath) {
        return columnIndexes.get(key(rowGroupOrdinal, columnPath));
    }

    OffsetIndex getOffsetIndex(int rowGroupOrdinal, String columnPath) {
        return offsetIndexes.get(key(rowGroupOrdinal, columnPath));
    }

    boolean hasColumnIndexes() {
        return columnIndexes.isEmpty() == false;
    }

    boolean hasOffsetIndexes() {
        return offsetIndexes.isEmpty() == false;
    }

    private static String key(int rowGroupOrdinal, String columnPath) {
        return rowGroupOrdinal + ":" + columnPath;
    }

    static String key(int rowGroupOrdinal, org.apache.parquet.hadoop.metadata.ColumnChunkMetaData column) {
        return key(rowGroupOrdinal, column.getPath().toDotString());
    }
}
