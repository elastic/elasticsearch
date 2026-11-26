/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

/**
 * The block-level join a reader runs to emit an {@link ExpandRead}: a batch's parent rows and the elements decoded for
 * them become one page of expanded rows. It is a library a format reader calls on the blocks it already holds, not a
 * stage of the read pipeline, so a second columnar reader expands exactly as the first one does.
 * <p>
 * Parents keep their order; each is repeated once per element it has, and a parent with none gets one row whose element
 * columns are null, as the index side's {@code NestedExpandOperator} does. Elements whose parent the batch dropped
 * (a filter, late materialization or a {@code skip_row} drop) are dropped with it.
 */
public final class ExpandedRows {

    /** Charged for the position arrays the join builds, which no block accounts for. */
    static final String BREAKER_LABEL = "expand_read";

    private ExpandedRows() {}

    /**
     * Expands one batch. Takes ownership of {@code parents}, {@code elementParents} and every block of
     * {@code elements}, releasing them whether or not it succeeds.
     *
     * @param parents        the batch's surviving parent rows, in batch order
     * @param survivors      the batch positions {@code parents} holds, ascending, or {@code null} when it holds every
     *                       one of the batch's {@code batchRows} rows
     * @param batchRows      the rows of the batch the elements were decoded for, before any drop
     * @param elementParents per element, the batch position of its parent, non-decreasing
     * @param elements       per element column, a block of one position per element; a {@code null} entry is a column
     *                       no element of this batch has, null on every row
     * @return the parent blocks repeated per element followed by the element columns, in {@code elements} order
     */
    public static Page expand(
        Page parents,
        @Nullable int[] survivors,
        int batchRows,
        IntVector elementParents,
        Block[] elements,
        BlockFactory blockFactory
    ) {
        long reserved = 0;
        Block[] columns = new Block[elements.length];
        Page repeated = null;
        try {
            int parentCount = parents.getPositionCount();
            int expected = survivors == null ? batchRows : survivors.length;
            if (parentCount != expected) {
                throw new IllegalStateException("[" + parentCount + "] parent rows but the batch kept [" + expected + "]");
            }
            int elementCount = elementParents.getPositionCount();
            for (Block block : elements) {
                if (block != null && block.getPositionCount() != elementCount) {
                    throw new IllegalStateException(
                        "an element column has [" + block.getPositionCount() + "] positions for [" + elementCount + "] elements"
                    );
                }
            }
            int rows = countRows(parentCount, survivors, elementParents);
            long positionBytes = 2L * rows * Integer.BYTES;
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(positionBytes, BREAKER_LABEL);
            // Only once the charge is taken: a trip adds nothing, so there is nothing to give back
            reserved = positionBytes;
            int[] parentPositions = new int[rows];
            // The element of each row, or -1 for the null row of a parent without elements
            int[] elementPositions = new int[rows];
            boolean elementsInPlace = fill(parentCount, survivors, elementParents, parentPositions, elementPositions);
            repeated = parentPositionsInPlace(parentPositions, parentCount) ? parents : parents.filter(true, parentPositions, 0, rows);
            for (int c = 0; c < elements.length; c++) {
                Block block = elements[c];
                if (block == null) {
                    columns[c] = blockFactory.newConstantNullBlock(rows);
                } else if (elementsInPlace) {
                    columns[c] = block;
                    elements[c] = null;
                } else {
                    columns[c] = select(block, elementPositions, blockFactory);
                }
            }
            Page out = repeated.appendBlocks(columns);
            if (repeated == parents) {
                parents = null;
            }
            repeated = null;
            columns = null;
            return out;
        } finally {
            blockFactory.breaker().addWithoutBreaking(-reserved);
            if (repeated != null && repeated != parents) {
                repeated.releaseBlocks();
            }
            if (parents != null) {
                parents.releaseBlocks();
            }
            if (columns != null) {
                Releasables.closeExpectNoException(columns);
            }
            Releasables.closeExpectNoException(elementParents);
            Releasables.closeExpectNoException(elements);
        }
    }

    /** The rows the batch expands to: each surviving parent's elements, or one row for a parent with none. */
    private static int countRows(int parentCount, @Nullable int[] survivors, IntVector elementParents) {
        long rows = 0;
        int e = 0;
        int elementCount = elementParents.getPositionCount();
        for (int p = 0; p < parentCount; p++) {
            int batchPosition = survivors == null ? p : survivors[p];
            while (e < elementCount && elementParents.getInt(e) < batchPosition) {
                e++;
            }
            int first = e;
            while (e < elementCount && elementParents.getInt(e) == batchPosition) {
                e++;
            }
            rows += Math.max(1, e - first);
        }
        if (rows > Integer.MAX_VALUE) {
            throw new IllegalStateException("a batch expands to [" + rows + "] rows, more than a page holds");
        }
        return (int) rows;
    }

    /**
     * Fills the parent and element position of every row, and returns whether the elements are used whole and in order,
     * every row an element and every element a row, so the element blocks can be used as they are.
     */
    private static boolean fill(
        int parentCount,
        @Nullable int[] survivors,
        IntVector elementParents,
        int[] parentPositions,
        int[] elementPositions
    ) {
        int elementCount = elementParents.getPositionCount();
        int row = 0;
        int e = 0;
        boolean inPlace = true;
        for (int p = 0; p < parentCount; p++) {
            int batchPosition = survivors == null ? p : survivors[p];
            while (e < elementCount && elementParents.getInt(e) < batchPosition) {
                // The element of a parent the batch dropped
                inPlace = false;
                e++;
            }
            if (e < elementCount && elementParents.getInt(e) == batchPosition) {
                do {
                    parentPositions[row] = p;
                    elementPositions[row] = e;
                    inPlace &= row == e;
                    row++;
                    e++;
                } while (e < elementCount && elementParents.getInt(e) == batchPosition);
            } else {
                parentPositions[row] = p;
                elementPositions[row] = -1;
                inPlace = false;
                row++;
            }
        }
        return inPlace && e == elementCount;
    }

    private static boolean parentPositionsInPlace(int[] parentPositions, int parentCount) {
        if (parentPositions.length != parentCount) {
            return false;
        }
        for (int i = 0; i < parentPositions.length; i++) {
            if (parentPositions[i] != i) {
                return false;
            }
        }
        return true;
    }

    /** {@code block}'s values at {@code positions}, ascending apart from {@code -1}, which is a null row. */
    private static Block select(Block block, int[] positions, BlockFactory blockFactory) {
        try (Block.Builder builder = block.elementType().newBlockBuilder(positions.length, blockFactory)) {
            int i = 0;
            while (i < positions.length) {
                if (positions[i] < 0) {
                    builder.appendNull();
                    i++;
                    continue;
                }
                // Copy each run of consecutive elements at once
                int from = positions[i];
                int to = from + 1;
                i++;
                while (i < positions.length && positions[i] == to) {
                    to++;
                    i++;
                }
                builder.copyFrom(block, from, to);
            }
            return builder.build();
        }
    }
}
