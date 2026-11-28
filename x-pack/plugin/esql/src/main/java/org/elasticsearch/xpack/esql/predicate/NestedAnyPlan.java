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
 * Prototype of {@code NESTED_ANY} over a child cursor. The child predicate runs on each batch's own positions, so both
 * sides of a conjunction see the same child. Parents become false only once the cursor is exhausted.
 * <p>
 * Production reads children through {@code NestedChildren} and reduces them in {@code NestedPredicateEvaluator}. This
 * class records that shape for a source that does not yet have its own page.
 */
public final class NestedAnyPlan {
    private NestedAnyPlan() {}

    /** One true or false position per parent. The caller closes the block. */
    public static BooleanBlock evaluate(BlockFactory factory, int parentCount, CandidateCursor children, RowPredicate childPredicate) {
        AnyReduction reduction = new AnyReduction(factory, parentCount);
        try {
            for (SelectionBatch batch = children.nextBatch(); batch != null; batch = children.nextBatch()) {
                if (batch instanceof ChildBatch child) {
                    int[] selection = batch.candidates();
                    try (BooleanBlock result = childPredicate.evaluate(batch, selection)) {
                        reduction.add(child, selection, result);
                    }
                } else {
                    throw new IllegalArgumentException("a nested cursor must yield child batches");
                }
            }
            return reduction.seal();
        } finally {
            children.close();
        }
    }
}
