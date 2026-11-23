/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.Optional;

/**
 * A source's account of what it can do with a predicate, and how the engine asks it for candidates. Lucene, Parquet, and
 * a remote system each implement this. The engine does not see their physical algorithms.
 */
public interface PredicateSource {
    PredicateCapability analyze(PredicateExpr predicate);

    CandidateCursor open(CandidateConstraint constraint, Projection projection);

    /** A per-row refinement, present only when {@link #analyze} reports {@link SourceOperation#REFINE} exactly. */
    Optional<PredicateRefiner> refiner(PredicateExpr predicate);
}
