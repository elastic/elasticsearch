/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.xpack.parquet.docvalues.ParquetFileInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Plans how the row groups of the files of a mount fill the backing indices (A2a of the plan). The files are taken in the order of the
 * request and their row groups in file order. A row group goes to the current index while the rows of the index stay at or below the
 * limit, and otherwise starts a new index. So a file can span several indices, and a split is always at the border of a row group.
 */
public final class MountPlanner {

    /** A run of consecutive row groups of one file that goes into one index. */
    public record Piece(int file, int firstRowGroup, int rowGroupCount, long rows) {}

    /** The pieces of one backing index, in the order of their rows. */
    public record IndexPlan(List<Piece> pieces, long rows) {}

    private MountPlanner() {}

    /**
     * @param maxRowsPerIndex the most rows that one index holds
     * @throws IllegalArgumentException if one row group has more rows than the limit
     */
    public static List<IndexPlan> plan(List<ParquetFileInfo> files, long maxRowsPerIndex) {
        if (maxRowsPerIndex < 1) {
            throw new IllegalArgumentException("max_rows_per_index must be at least 1");
        }
        List<IndexPlan> indices = new ArrayList<>();
        List<Piece> pieces = new ArrayList<>();
        long indexRows = 0;
        for (int file = 0; file < files.size(); file++) {
            ParquetFileInfo info = files.get(file);
            int pieceStart = -1;
            long pieceRows = 0;
            for (int rowGroup = 0; rowGroup < info.rowGroups().size(); rowGroup++) {
                long rows = info.rowGroups().get(rowGroup).numRows();
                if (rows > maxRowsPerIndex) {
                    throw new IllegalArgumentException(
                        "row group ["
                            + rowGroup
                            + "] of ["
                            + info.location()
                            + "] has ["
                            + rows
                            + "] rows, more than max_rows_per_index ["
                            + maxRowsPerIndex
                            + "]"
                    );
                }
                if (indexRows + rows > maxRowsPerIndex) {
                    // close the piece of this file and the index, and start a new index with this row group
                    if (pieceStart >= 0) {
                        pieces.add(new Piece(file, pieceStart, rowGroup - pieceStart, pieceRows));
                        pieceStart = -1;
                        pieceRows = 0;
                    }
                    indices.add(new IndexPlan(List.copyOf(pieces), indexRows));
                    pieces = new ArrayList<>();
                    indexRows = 0;
                }
                if (pieceStart < 0) {
                    pieceStart = rowGroup;
                }
                pieceRows += rows;
                indexRows += rows;
            }
            if (pieceStart >= 0) {
                pieces.add(new Piece(file, pieceStart, info.rowGroups().size() - pieceStart, pieceRows));
            }
        }
        if (pieces.isEmpty() == false) {
            indices.add(new IndexPlan(List.copyOf(pieces), indexRows));
        }
        return List.copyOf(indices);
    }
}
