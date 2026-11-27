/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * In-place evaluation of a predicate. Unlike {@link FilterPlan}, this does not ask the source for a new candidate stream,
 * so rows that are false or null stay in the batch.
 */
public final class EvalPlan {
    private final RowPredicate predicate;

    EvalPlan(RowPredicate predicate) {
        this.predicate = predicate;
    }

    public PredicateResult evaluate(SelectionBatch batch) {
        return predicate.evaluate(batch, batch.candidates());
    }
}
