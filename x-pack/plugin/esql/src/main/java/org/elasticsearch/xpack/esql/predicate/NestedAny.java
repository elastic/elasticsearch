/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.List;

/**
 * Collection reduction for {@code NESTED_ANY}. A parent is true when any child is true. A child null does not make the
 * parent null: that is the collection result, decided after each child's Boolean result is known. Parent positions remain
 * valid when one parent's children arrive in several batches.
 */
public final class NestedAny {
    private NestedAny() {}

    public static PredicateResult any(int parentCount, List<? extends ChildBatch> batches, RowPredicate childPredicate) {
        boolean[] trues = new boolean[parentCount];
        for (ChildBatch batch : batches) {
            if (batch.positionCount() == 0) {
                continue;
            }
            PredicateResult result = childPredicate.evaluate(batch, batch.candidates());
            for (int i = 0; i < batch.positionCount(); i++) {
                if (result.trues().get(i)) {
                    trues[batch.parentPosition(i)] = true;
                }
            }
        }
        return PredicateResult.of(SelectionMask.of(trues), SelectionMask.none(parentCount));
    }
}
