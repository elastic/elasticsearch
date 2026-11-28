/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.List;

/**
 * Predicates the source may use to produce candidates. A superset constraint still needs verification in the residual
 * predicate. An exact constraint does not.
 */
public record CandidateConstraint(List<PredicateExpr> predicates) {
    public CandidateConstraint {
        predicates = List.copyOf(predicates);
    }

    public static CandidateConstraint none() {
        return new CandidateConstraint(List.of());
    }
}
