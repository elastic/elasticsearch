/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * {@code NESTED_ANY} over a child cursor. The child predicate runs on each batch's own positions, so both sides of a
 * conjunction see the same child. Parents become false only once the cursor is exhausted.
 */
public final class NestedAnyPlan {
    private NestedAnyPlan() {}

    public static PredicateResult evaluate(int parentCount, CandidateCursor children, RowPredicate childPredicate) {
        AnyReduction reduction = new AnyReduction(parentCount);
        try {
            for (SelectionBatch batch = children.nextBatch(); batch != null; batch = children.nextBatch()) {
                if (batch instanceof ChildBatch child) {
                    reduction.add(child, childPredicate.evaluate(batch, batch.candidates()));
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
