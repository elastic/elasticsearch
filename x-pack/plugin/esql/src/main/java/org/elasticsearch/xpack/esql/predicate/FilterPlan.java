/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.ArrayList;
import java.util.List;

/**
 * A filter plan: the source produces candidate batches, and the residual predicate decides which of those rows are true.
 * Null is not accepted.
 */
public final class FilterPlan {
    private final CandidateConstraint constraint;
    private final Projection projection;
    private final CandidateCursor cursor;
    private final RowPredicate residual;
    private List<Integer> accepted;

    FilterPlan(CandidateConstraint constraint, Projection projection, CandidateCursor cursor, RowPredicate residual) {
        this.constraint = constraint;
        this.projection = projection;
        this.cursor = cursor;
        this.residual = residual;
    }

    public CandidateConstraint constraint() {
        return constraint;
    }

    public Projection projection() {
        return projection;
    }

    /** Source row ids whose residual result is true, in ascending order. */
    public List<Integer> acceptedRows() {
        if (accepted != null) {
            return accepted;
        }
        List<Integer> rows = new ArrayList<>();
        for (SelectionBatch batch = cursor.nextBatch(); batch != null; batch = cursor.nextBatch()) {
            PredicateResult result = residual.evaluate(batch, batch.candidates());
            for (int i = 0; i < batch.positionCount(); i++) {
                if (result.trues().get(i)) {
                    rows.add(batch.sourceRow(i));
                }
            }
        }
        rows.sort(Integer::compareTo);
        accepted = List.copyOf(rows);
        return accepted;
    }
}
