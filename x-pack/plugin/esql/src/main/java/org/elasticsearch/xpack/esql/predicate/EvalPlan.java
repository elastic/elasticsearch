/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.core.Releasable;

/**
 * In-place evaluation of a predicate. Unlike {@link FilterPlan}, this does not ask the source for a new candidate stream,
 * so rows that are false or null stay in the batch.
 */
public final class EvalPlan implements Releasable {
    private final RowPredicate predicate;

    EvalPlan(RowPredicate predicate) {
        this.predicate = predicate;
    }

    /** One three-valued position per candidate of the batch, in candidate order. The caller closes the block. */
    public BooleanBlock evaluate(SelectionBatch batch) {
        return predicate.evaluate(batch, batch.candidates());
    }

    @Override
    public void close() {
        predicate.close();
    }
}
