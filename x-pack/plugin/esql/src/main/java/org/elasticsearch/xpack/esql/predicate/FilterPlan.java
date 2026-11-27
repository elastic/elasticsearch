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
    private final CandidateCoverage coverage;
    private final Projection projection;
    private final CandidateCursor cursor;
    private final RowPredicate residual;
    private List<SourceRow> accepted;

    FilterPlan(
        CandidateConstraint constraint,
        CandidateCoverage coverage,
        Projection projection,
        CandidateCursor cursor,
        RowPredicate residual
    ) {
        this.constraint = constraint;
        this.coverage = coverage;
        this.projection = projection;
        this.cursor = cursor;
        this.residual = residual;
    }

    public CandidateConstraint constraint() {
        return constraint;
    }

    /** How completely the candidate stream covers the whole predicate, not the leaf that produced it. */
    public CandidateCoverage coverage() {
        return coverage;
    }

    public Projection projection() {
        return projection;
    }

    /**
     * Rows whose residual result is true. This materializes the whole cursor so a test can compare row identity. A
     * downstream operator has to consume batches and stop early instead of calling this.
     */
    public List<SourceRow> acceptedRows() {
        if (accepted != null) {
            return accepted;
        }
        List<SourceRow> rows = new ArrayList<>();
        for (SelectionBatch batch = cursor.nextBatch(); batch != null; batch = cursor.nextBatch()) {
            PredicateResult result = residual.evaluate(batch, batch.candidates());
            for (int i = 0; i < batch.positionCount(); i++) {
                if (result.trues().get(i)) {
                    rows.add(batch.sourceRow(i));
                }
            }
        }
        rows.sort(FilterPlan::compareRows);
        accepted = List.copyOf(rows);
        return accepted;
    }

    /** Order within one row space is by id. Ids from different spaces stay distinct and are grouped by space. */
    private static int compareRows(SourceRow left, SourceRow right) {
        int space = Integer.compare(System.identityHashCode(left.space()), System.identityHashCode(right.space()));
        return space != 0 ? space : Integer.compare(left.id(), right.id());
    }
}
