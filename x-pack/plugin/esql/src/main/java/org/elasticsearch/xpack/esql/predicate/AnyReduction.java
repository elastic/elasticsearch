/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;

/**
 * Reduction for {@code NESTED_ANY}, kept separate from the child-to-parent mapping. A parent is true when any child is
 * true. A child null does not make the parent null. {@link #add} may be called for many batches, and parent positions
 * mean the same parent in each of them. {@link #seal()} is valid only once the child cursor for these parents is
 * exhausted: until then a parent with no true child is unfinished, not false.
 */
public final class AnyReduction {
    private final BlockFactory factory;
    private final boolean[] trues;
    private boolean sealed;

    public AnyReduction(BlockFactory factory, int parentCount) {
        this.factory = factory;
        this.trues = new boolean[parentCount];
    }

    /**
     * Parents already proven true, one position per parent. A false position is unfinished until {@link #seal()}, not a
     * proven false. The caller closes the block.
     */
    public BooleanBlock knownTrues() {
        if (sealed) {
            throw new IllegalStateException("this reduction is already sealed");
        }
        return parents();
    }

    /**
     * Folds an already evaluated child batch. {@code result} has one position per entry of {@code selection}, in selection
     * order. The reducer does not evaluate the child predicate.
     */
    public void add(ChildBatch batch, int[] selection, BooleanBlock result) {
        if (sealed) {
            throw new IllegalStateException("this reduction is already sealed");
        }
        if (result.getPositionCount() != selection.length) {
            throw new IllegalArgumentException(
                "result covers [" + result.getPositionCount() + "] positions and the selection has [" + selection.length + "]"
            );
        }
        for (int i = 0; i < selection.length; i++) {
            if (result.isNull(i) == false && result.getBoolean(result.getFirstValueIndex(i))) {
                trues[batch.parentPosition(selection[i])] = true;
            }
        }
    }

    /** True or false for every parent; a parent is never null. The caller closes the block. */
    public BooleanBlock seal() {
        sealed = true;
        return parents();
    }

    private BooleanBlock parents() {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(trues.length)) {
            for (boolean parent : trues) {
                builder.appendBoolean(parent);
            }
            return builder.build();
        }
    }
}
