/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * Reduction for {@code NESTED_ANY}, kept separate from the child-to-parent mapping. A parent is true when any child is
 * true. A child null does not make the parent null. {@link #add} may be called for many batches, and parent positions
 * mean the same parent in each of them. {@link #seal()} is valid only once the child cursor for these parents is
 * exhausted: until then a parent with no true child is unfinished, not false.
 */
public final class AnyReduction {
    private final boolean[] trues;
    private boolean sealed;

    public AnyReduction(int parentCount) {
        this.trues = new boolean[parentCount];
    }

    /** Parents already proven true. A false bit is unfinished until {@link #seal()}, not a proven false. */
    public SelectionMask knownTrues() {
        if (sealed) {
            throw new IllegalStateException("this reduction is already sealed");
        }
        return SelectionMask.of(trues.clone());
    }

    /** Folds an already evaluated child batch. The reducer does not evaluate the child predicate. */
    public void add(ChildBatch batch, PredicateResult result) {
        if (sealed) {
            throw new IllegalStateException("this reduction is already sealed");
        }
        if (result.trues().positionCount() != batch.positionCount()) {
            throw new IllegalArgumentException(
                "result covers [" + result.trues().positionCount() + "] positions and the batch has [" + batch.positionCount() + "]"
            );
        }
        if (batch.positionCount() == 0) {
            return;
        }
        for (int i = 0; i < batch.positionCount(); i++) {
            if (result.trues().get(i)) {
                trues[batch.parentPosition(i)] = true;
            }
        }
    }

    public PredicateResult seal() {
        sealed = true;
        return PredicateResult.of(SelectionMask.of(trues), SelectionMask.none(trues.length));
    }
}
