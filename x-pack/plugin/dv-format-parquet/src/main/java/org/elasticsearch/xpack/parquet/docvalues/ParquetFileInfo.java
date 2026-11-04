/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * What the footer of a Parquet file says, reduced to what the format needs: the leaf columns of the schema and, for each row group,
 * its row count and the chunk of each column.
 */
public final class ParquetFileInfo {

    /**
     * A leaf column of the schema.
     *
     * @param index the position of the column among the leaf columns, which is also its position in the column chunks of a row group
     * @param path the dotted path of the column
     * @param physicalType the Parquet physical type (the value of {@code org.apache.parquet.format.Type})
     * @param convertedType the Parquet converted type, or {@code -1} if there is none
     * @param logical what the annotations of the column mean
     * @param required whether the column and all its parents are {@code REQUIRED}
     * @param flat whether the column is a direct child of the root, so not part of a struct, list or map
     * @param repeated whether a value is repeated for a row, which is what a list does
     */
    public record LeafColumn(
        int index,
        String path,
        int physicalType,
        int convertedType,
        LogicalKind logical,
        boolean required,
        boolean flat,
        boolean repeated
    ) {}

    /**
     * The column chunk of one row group.
     *
     * @param offset the file offset of the first byte of the chunk, which is its dictionary page if it has one
     * @param minValue the smallest value that the footer statistics give for an integer column, or {@link ColumnSnapshot#UNKNOWN_MIN}
     * @param maxValue the largest value, or {@link ColumnSnapshot#UNKNOWN_MAX}
     * @param nullCount the number of missing values, or {@link ColumnSnapshot#UNKNOWN_NULL_COUNT}
     */
    public record Chunk(
        long offset,
        long compressedSize,
        long uncompressedSize,
        long numValues,
        int codec,
        long minValue,
        long maxValue,
        long nullCount
    ) {}

    /** A row group: its row count and one chunk for each leaf column. */
    public record RowGroup(int numRows, List<Chunk> chunks) {}

    private final String location;
    private final long length;
    private final long rowCount;
    private final String createdBy;
    private final List<LeafColumn> columns;
    private final List<RowGroup> rowGroups;

    ParquetFileInfo(String location, long length, long rowCount, String createdBy, List<LeafColumn> columns, List<RowGroup> rowGroups) {
        this.location = location;
        this.length = length;
        this.rowCount = rowCount;
        this.createdBy = createdBy;
        this.columns = List.copyOf(columns);
        this.rowGroups = List.copyOf(rowGroups);
    }

    public String location() {
        return location;
    }

    public long length() {
        return length;
    }

    public long rowCount() {
        return rowCount;
    }

    /** The writer of the file as the footer names it, or {@code null}. */
    public String createdBy() {
        return createdBy;
    }

    public List<LeafColumn> columns() {
        return columns;
    }

    public List<RowGroup> rowGroups() {
        return rowGroups;
    }

    /**
     * Checks that another file can be served together with this one, whose columns gave the mapping: every mapped column must exist in
     * the other file with the same type and meaning. A mount checks the files of one request with it, and the engine checks the files of
     * an index when it attaches them.
     *
     * @throws IllegalArgumentException with the first difference
     */
    public void requireCompatibleWith(ParquetFileInfo other, Collection<String> mappedColumns) {
        for (String path : mappedColumns) {
            LeafColumn expected = column(path);
            LeafColumn found = other.column(path);
            if (found == null) {
                throw new IllegalArgumentException(
                    "[" + other.location() + "] has no column [" + path + "], which [" + location() + "] has"
                );
            }
            if (found.physicalType() != expected.physicalType() || found.logical() != expected.logical()) {
                throw new IllegalArgumentException(
                    "column ["
                        + path
                        + "] of ["
                        + other.location()
                        + "] is ["
                        + found.physicalType()
                        + " "
                        + found.logical()
                        + "], but it is ["
                        + expected.physicalType()
                        + " "
                        + expected.logical()
                        + "] in ["
                        + location()
                        + "]"
                );
            }
            if (found.flat() == false || found.repeated()) {
                throw new IllegalArgumentException("column [" + path + "] of [" + other.location() + "] is nested or repeated");
            }
        }
    }

    /** The leaf column with the given dotted path, or {@code null}. */
    public LeafColumn column(String path) {
        for (LeafColumn column : columns) {
            if (column.path().equals(path)) {
                return column;
            }
        }
        return null;
    }

    /**
     * The piece of the file that covers {@code rowGroupCount} row groups from {@code firstRowGroup}, with the footer snapshot of the
     * given columns.
     *
     * @param indexRowBase the number of rows in the pieces that precede this one in the index
     * @param columnPaths the dotted paths of the columns that the segment serves
     */
    public ExternalFilePiece piece(int firstRowGroup, int rowGroupCount, long indexRowBase, Collection<String> columnPaths) {
        if (firstRowGroup < 0 || rowGroupCount < 0 || firstRowGroup + rowGroupCount > rowGroups.size()) {
            throw new IllegalArgumentException(
                "row groups [" + firstRowGroup + ", +" + rowGroupCount + ") are outside of the [" + rowGroups.size() + "] of " + location
            );
        }
        int[] rows = new int[rowGroupCount];
        for (int i = 0; i < rowGroupCount; i++) {
            rows[i] = rowGroups.get(firstRowGroup + i).numRows();
        }
        List<ColumnSnapshot> snapshots = new ArrayList<>();
        Map<String, LeafColumn> byPath = columns.stream().collect(Collectors.toMap(LeafColumn::path, Function.identity()));
        for (String path : columnPaths) {
            LeafColumn column = byPath.get(path);
            if (column == null) {
                throw new IllegalArgumentException("[" + location + "] has no column [" + path + "]");
            }
            long[] offsets = new long[rowGroupCount];
            long[] compressed = new long[rowGroupCount];
            long[] uncompressed = new long[rowGroupCount];
            long[] values = new long[rowGroupCount];
            long[] minimums = new long[rowGroupCount];
            long[] maximums = new long[rowGroupCount];
            long[] nullCounts = new long[rowGroupCount];
            int codec = 0;
            for (int i = 0; i < rowGroupCount; i++) {
                Chunk chunk = rowGroups.get(firstRowGroup + i).chunks().get(column.index());
                offsets[i] = chunk.offset();
                compressed[i] = chunk.compressedSize();
                uncompressed[i] = chunk.uncompressedSize();
                values[i] = chunk.numValues();
                minimums[i] = chunk.minValue();
                maximums[i] = chunk.maxValue();
                nullCounts[i] = chunk.nullCount();
                codec = chunk.codec();
            }
            snapshots.add(
                new ColumnSnapshot(
                    column.path(),
                    column.physicalType(),
                    column.convertedType(),
                    column.required(),
                    codec,
                    offsets,
                    compressed,
                    uncompressed,
                    values,
                    minimums,
                    maximums,
                    nullCounts
                )
            );
        }
        return new ExternalFilePiece(location, length, indexRowBase, firstRowGroup, rows, snapshots);
    }
}
