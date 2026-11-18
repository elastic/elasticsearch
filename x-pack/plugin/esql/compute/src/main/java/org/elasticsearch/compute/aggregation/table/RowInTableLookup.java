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
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

/**
 * Consumes {@link Page}s and looks up each row in a pre-built table, and returns the
 * offsets of each row in the table.
 */
public abstract sealed class RowInTableLookup implements Releasable permits EmptyRowInTableLookup, AscendingSequenceRowInTableLookup,
    BlockHashRowInTableLookup, RepeatedKeyBlockHashRowInTableLookup {
    /**
     * Look up the values in the {@link Page} and, for each row, return the offsets of the matching rows
     * in the table that was provided when building the lookup.
     * <p>
     *     The returned {@link ReleasableIterator} may retain a reference to {@link Block}s
     *     inside the {@link Page}. Close it to release those references.
     * </p>
     * <p>
     *     Lookups don't modify the table and build their results with {@code blockFactory} rather
     *     than the table's factory, so several threads may probe one table at once, each charging
     *     its own breaker.
     * </p>
     */
    public abstract ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize);

    /**
     * Look up rows with a warning collector owned by the probing driver, not by the shared table.
     */
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize, Warnings warnings) {
        return lookup(page, blockFactory, targetBlockSize);
    }

    @Override
    public abstract String toString();

    public static RowInTableLookup build(BlockFactory blockFactory, Block[] keys) {
        return build(blockFactory, keys, false);
    }

    /**
     * Allow repeated build keys for callers that need every match, without relaxing the unique-key contract for other callers.
     */
    public static RowInTableLookup build(BlockFactory blockFactory, Block[] keys, boolean repeatedKeys) {
        if (keys.length < 1) {
            throw new IllegalArgumentException("expected [keys] to be non-empty");
        }
        int positions = keys[0].getPositionCount();
        for (int k = 0; k < keys.length; k++) {
            if (positions != keys[k].getPositionCount()) {
                // TODO double check these errors over REST once we have LOOKUP
                throw new IllegalArgumentException(
                    "keys must have the same number of positions but [" + positions + "] != [" + keys[k].getPositionCount() + "]"
                );
            }
            if (keys[k].doesHaveMultivaluedFields()) {
                throw new IllegalArgumentException("only single valued keys are supported");
            }
        }
        if (positions == 0) {
            return new EmptyRowInTableLookup();
        }
        if (repeatedKeys) {
            return new RepeatedKeyBlockHashRowInTableLookup(blockFactory, keys);
        }
        if (keys.length == 1) {
            RowInTableLookup lookup = single(keys[0]);
            if (lookup != null) {
                return lookup;
            }
        }
        return new BlockHashRowInTableLookup(blockFactory, keys);
    }

    /**
     * Build a {@link RowInTableLookup} for a single {@link Block} or returns {@code null}
     * if we don't have a special implementation for this single block.
     */
    private static RowInTableLookup single(Block b) {
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
        return new AscendingSequenceRowInTableLookup(first, first + v.getPositionCount());
    }
}
