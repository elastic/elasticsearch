/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

/**
 * Consumes {@link Page}s and looks up each row in a pre-built table, and returns the
 * offsets of each row in the table.
 */
public abstract sealed class RowInTable implements Releasable permits EmptyRowInTable, AscendingSequenceRowInTable, BlockHashRowInTable {
    /**
     * Lookup the values in the {@link Page} and, for each row, return the offset in the
     * table that was provided when building the lookup.
     * <p>
     *     The returned {@link ReleasableIterator} may retain a reference to {@link Block}s
     *     inside the {@link Page}. Close it to release those references.
     * </p>
     */
    public abstract ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize);

    @Override
    public abstract String toString();

    public static RowInTable build(BlockFactory blockFactory, Block[] keys) {
        int positions = keys[0].getPositionCount();
        for (int k = 0; k < keys.length; k++) {
            if (positions != keys[k].getPositionCount()) {
                // TODO double check these errors over REST once we have LOOKUP
                throw new IllegalArgumentException(
                    "keys must have the same number of positions but [" + positions + "] != [" + keys[k].getPositionCount() + "]"
                );
            }
            for (int p = 0; p < keys[k].getPositionCount(); p++) {
                if (keys[k].getValueCount(p) > 1) {
                    // TODO double check these errors over REST once we have LOOKUP
                    throw new IllegalArgumentException("only single valued keys are supported");
                }
            }
        }
        if (positions == 0) {
            return new EmptyRowInTable(blockFactory);
        }
        if (keys.length == 1) {
            RowInTable lookup = single(blockFactory, keys[0]);
            if (lookup != null) {
                return lookup;
            }
        }
        return new BlockHashRowInTable(blockFactory, keys);
    }

    /**
     * Build a {@link RowInTable} for a single {@link Block} or returns {@code null}
     * if we don't have a special implementation for this single block.
     */
    private static RowInTable single(BlockFactory blockFactory, Block b) {
        if (b.elementType() != ElementType.INT) {
            return null;
        }
        IntVector v = (IntVector) b.asVector();
        if (v == null) {
            return null;
        }
        int first = v.getInt(0);
        for (int i = 1; i < v.getPositionCount(); i++) {
            if (v.getInt(i) - first != i) {
                return null;
            }
        }
        return new AscendingSequenceRowInTable(blockFactory, first, first + v.getPositionCount());
    }
}
