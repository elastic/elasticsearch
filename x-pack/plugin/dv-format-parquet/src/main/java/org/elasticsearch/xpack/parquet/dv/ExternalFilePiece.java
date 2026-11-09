/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.core.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The rows of one Parquet file that a segment serves. Document {@code d} of the segment is row {@code firstRow + d} of the file, which
 * is the same file-global row index that the ES|QL reader's positional extraction uses.
 *
 * @param location     the file, as the {@link ParquetReadContext.Resolver} that opens the segment understands it
 * @param length       the file length in bytes, which pins the version of the file the segment was attached to
 * @param firstRow     the first file row of the segment
 * @param rowCount     the number of rows, which is the number of documents in the segment
 * @param indexRowBase the number of rows in the segments that precede this one in the same index, which gives the {@code _id}
 * @param rowGroupRows the number of rows of each row group of the piece, in file order, or empty when the piece has no statistics;
 *                     a piece covers whole row groups, and these are the intervals of its skippers
 * @param stats        for each numeric field, by the name of the field, the bounds of its values in each row group, which give the
 *                     skipper of the field. They are by field and not by column, because the conversion belongs to the field.
 */
public record ExternalFilePiece(
    String location,
    long length,
    long firstRow,
    int rowCount,
    long indexRowBase,
    int[] rowGroupRows,
    Map<String, ColumnStats> stats
) {

    public ExternalFilePiece {
        if (firstRow < 0 || rowCount < 0 || indexRowBase < 0) {
            throw new IllegalArgumentException(
                "first row [" + firstRow + "], row count [" + rowCount + "] and index row base [" + indexRowBase + "] must not be negative"
            );
        }
        long total = 0;
        for (int rows : rowGroupRows) {
            total += rows;
        }
        if (rowGroupRows.length > 0 && total != rowCount) {
            throw new IllegalArgumentException("the row groups hold [" + total + "] rows, but the piece has [" + rowCount + "]");
        }
        stats = Map.copyOf(stats);
    }

    public ExternalFilePiece(String location, long length, long firstRow, int rowCount, long indexRowBase) {
        this(location, length, firstRow, rowCount, indexRowBase, new int[0], Map.of());
    }

    public ExternalFilePiece(String location, long length, long firstRow, int rowCount) {
        this(location, length, firstRow, rowCount, 0);
    }

    /**
     * The piece of a file that covers {@code rowGroupCount} row groups from {@code firstRowGroup}, with the statistics that give a
     * skipper to each numeric field whose column the footer describes well enough.
     *
     * @param indexRowBase the number of rows in the pieces that precede this one in the index
     * @param bindings     the fields that the segment serves
     * @throws IllegalArgumentException if the row groups are not in the file, or hold more rows than a segment can
     */
    public static ExternalFilePiece of(
        ParquetFileInfo file,
        int firstRowGroup,
        int rowGroupCount,
        long indexRowBase,
        List<ColumnBinding> bindings
    ) {
        if (firstRowGroup < 0 || rowGroupCount < 1 || firstRowGroup + rowGroupCount > file.rowGroups().size()) {
            throw new IllegalArgumentException(
                "row groups ["
                    + firstRowGroup
                    + ", +"
                    + rowGroupCount
                    + ") are outside of the ["
                    + file.rowGroups().size()
                    + "] of "
                    + file.location()
            );
        }
        int[] rowGroupRows = new int[rowGroupCount];
        long rows = 0;
        for (int i = 0; i < rowGroupCount; i++) {
            rowGroupRows[i] = file.rowGroups().get(firstRowGroup + i).numRows();
            rows += rowGroupRows[i];
        }
        if (rows > IndexWriter.MAX_DOCS) {
            throw new IllegalArgumentException(
                "row groups ["
                    + firstRowGroup
                    + ", +"
                    + rowGroupCount
                    + ") of "
                    + file.location()
                    + " have ["
                    + rows
                    + "] rows, more than a segment can hold"
            );
        }
        Map<String, ColumnStats> stats = new HashMap<>();
        for (ColumnBinding binding : bindings) {
            if (binding.isSynthesized() == false && binding.type() == DocValuesType.NUMERIC) {
                ColumnStats columnStats = ColumnStats.of(file, binding.parquetColumn(), firstRowGroup, rowGroupCount, binding.conversion());
                if (columnStats != null) {
                    stats.put(binding.fieldName(), columnStats);
                }
            }
        }
        return new ExternalFilePiece(
            file.location(),
            file.length(),
            file.firstRowOf(firstRowGroup),
            (int) rows,
            indexRowBase,
            rowGroupRows,
            stats
        );
    }

    /**
     * The bounds of the values of one numeric field in each row group of a segment, as the footer states them when the segment is attached.
     * They are in the value space of the field, so the conversion of the field and the scale that the ES|QL reader applies are already in
     * them, and the segment's skipper (see {@link ParquetDocValuesSkipper}) serves them without reading the file. The arrays are aligned
     * with {@link ExternalFilePiece#rowGroupRows()}.
     *
     * <p>A row group whose footer has no usable minimum and maximum, or whose bounds the conversion cannot represent, gets the widest
     * range, so the skipper says that it may hold any value and a query reads it: slower and never wrong.
     *
     * @param minValues the smallest value of each row group, or {@link #UNKNOWN_MIN}
     * @param maxValues the largest value of each row group, or {@link #UNKNOWN_MAX}
     * @param docCounts the number of documents of each row group that have a value
     */
    public record ColumnStats(long[] minValues, long[] maxValues, int[] docCounts) {

        public static final long UNKNOWN_MIN = Long.MIN_VALUE;
        public static final long UNKNOWN_MAX = Long.MAX_VALUE;

        /**
         * The factor that turns the value that the file stores into the value that the ES|QL reader returns, or {@code 0} if the column has
         * no skipper: an unsigned 32 bit column is widened by the reader and ordered as unsigned by the file, an unsigned 64 bit one is
         * encoded, and decimals and times are converted in ways that the bounds do not follow.
         */
        static long readerScale(PrimitiveType.PrimitiveTypeName physicalType, ParquetFileInfo.LogicalKind logical) {
            if (physicalType != PrimitiveType.PrimitiveTypeName.INT32 && physicalType != PrimitiveType.PrimitiveTypeName.INT64) {
                return 0;
            }
            return switch (logical) {
                case NONE, INT_8, INT_16, INT_32, INT_64, UINT_8, UINT_16, TIMESTAMP_MILLIS, TIMESTAMP_NANOS -> 1L;
                // the reader returns a DATE as epoch milliseconds, and a microsecond timestamp as epoch nanoseconds
                case DATE -> 86_400_000L;
                case TIMESTAMP_MICROS -> 1_000L;
                case UINT_32, UINT_64, DECIMAL, TIME, STRING, ENUM, JSON, OTHER -> 0L;
            };
        }

        /**
         * The statistics of a column for the row groups {@code [firstRowGroup, firstRowGroup + rowGroupCount)} of a file, or {@code null}
         * if the column cannot have a skipper: the type is not served, or the footer does not say how many values a row group has.
         */
        @Nullable
        static ColumnStats of(ParquetFileInfo file, String column, int firstRowGroup, int rowGroupCount, Conversion conversion) {
            ParquetFileInfo.LeafColumn leaf = file.column(column);
            if (leaf == null) {
                return null;
            }
            long scale = readerScale(leaf.physicalType(), leaf.logical());
            if (scale == 0) {
                return null;
            }
            long[] minValues = new long[rowGroupCount];
            long[] maxValues = new long[rowGroupCount];
            int[] docCounts = new int[rowGroupCount];
            for (int i = 0; i < rowGroupCount; i++) {
                ParquetFileInfo.RowGroup rowGroup = file.rowGroups().get(firstRowGroup + i);
                ParquetFileInfo.ChunkStats chunk = rowGroup.chunks().get(column);
                if (chunk == null || chunk.nullCount() < 0 || chunk.nullCount() > rowGroup.numRows()) {
                    return null;
                }
                docCounts[i] = rowGroup.numRows() - (int) chunk.nullCount();
                minValues[i] = UNKNOWN_MIN;
                maxValues[i] = UNKNOWN_MAX;
                if (chunk.hasMinMax() && docCounts[i] > 0) {
                    try {
                        long min = conversion.apply(Math.multiplyExact(chunk.min(), scale));
                        long max = conversion.apply(Math.multiplyExact(chunk.max(), scale));
                        // a conversion keeps the order of the values, so the minimum stays the minimum
                        minValues[i] = min;
                        maxValues[i] = max;
                    } catch (ArithmeticException e) {
                        // a bound that the scale or the conversion cannot represent: the row group may hold anything
                    }
                }
            }
            return new ColumnStats(minValues, maxValues, docCounts);
        }
    }
}
