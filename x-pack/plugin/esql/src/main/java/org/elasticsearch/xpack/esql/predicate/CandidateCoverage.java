/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/**
 * What the candidate stream of a whole filter plan covers. A guarantee about one leaf does not transfer to the plan.
 * {@code category == "electronics"} can be an exact true-set and still only a superset of
 * {@code category == "electronics" AND price * quantity > 100}. One side of an {@code OR} does not cover the plan at all.
 * <p>
 * If {@code M} is the true rows of the whole predicate and {@code P} is the rows the cursor returns, a filtering plan
 * requires {@code M ⊆ P}. {@link #COMPLETE} and {@link #SUPERSET} both satisfy that. {@link #EXACT} means {@code P}
 * is already {@code M}. The guarantee is fixed when the source execution is opened, including from the shard's own
 * capabilities, and then reused for every batch.
 */
public enum CandidateCoverage {
    /** Nothing was proved. The cursor returns every row in scope. */
    COMPLETE,
    /** Every dropped row cannot satisfy the whole predicate. Retained rows still need the residual. */
    SUPERSET,
    /** The cursor is exactly the true rows of the whole predicate. The residual does not change that set. */
    EXACT
}
