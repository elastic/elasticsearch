/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.util.Arrays;
import java.util.List;

/**
 * The part of an external Parquet file that one segment covers: a whole file, or a range of its row groups. The docID of a document
 * in the segment maps to a row by adding up {@link #rowGroupRows}.
 *
 * @param location the storage reference of the file (never credentials)
 * @param length the length of the file when it was attached, so that reads can detect a changed object
 * @param indexRowBase the number of rows in the pieces that precede this one in the same index, which gives {@code _id}
 * @param firstRowGroup the position in the file of the first row group of the piece
 * @param rowGroupRows the number of rows of each row group of the piece, in file order
 * @param columns the footer snapshot of the columns that the segment serves, with the chunks of the row groups of the piece
 */
public record ExternalFilePiece(
    String location,
    long length,
    long indexRowBase,
    int firstRowGroup,
    int[] rowGroupRows,
    List<ColumnSnapshot> columns
) {

    /** The number of documents of the segment, that is the number of rows of the piece. */
    public int maxDoc() {
        long total = 0;
        for (int rows : rowGroupRows) {
            total += rows;
        }
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("a piece of [" + location + "] has [" + total + "] rows, more than a segment can hold");
        }
        return (int) total;
    }

    /** The snapshot of the column with the dotted path, or {@code null} if the piece has none. */
    public ColumnSnapshot column(String path) {
        for (ColumnSnapshot column : columns) {
            if (column.path().equals(path)) {
                return column;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ExternalFilePiece other
            && location.equals(other.location)
            && length == other.length
            && indexRowBase == other.indexRowBase
            && firstRowGroup == other.firstRowGroup
            && Arrays.equals(rowGroupRows, other.rowGroupRows)
            && columns.equals(other.columns);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * (31 * location.hashCode() + Long.hashCode(length)) + Long.hashCode(indexRowBase)) + Arrays.hashCode(rowGroupRows);
    }

    @Override
    public String toString() {
        return "ExternalFilePiece["
            + location
            + ", length="
            + length
            + ", indexRowBase="
            + indexRowBase
            + ", rowGroups="
            + rowGroupRows.length
            + ", columns="
            + columns.size()
            + "]";
    }
}
