/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * What the final physical plan did with a decision, so that a decision that was planned but not executed is visible.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum OutcomeStatus {
    /** The final physical plan runs the chosen alternative. */
    APPLIED,
    /** A consumer rejected the decision for a recorded reason and the plan runs the consumer's choice. */
    OVERRIDDEN,
    /** The final physical plan disagrees with the decision and no override was recorded. */
    MISMATCH,
    /** No physical node matched the decision's key. */
    NOT_OBSERVED,
    /** Two decision points produced the same key with different choices, so neither was published. */
    CONFLICT;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
