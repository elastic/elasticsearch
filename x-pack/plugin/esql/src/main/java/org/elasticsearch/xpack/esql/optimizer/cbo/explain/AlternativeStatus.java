/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * Why an enumerated alternative other than the chosen one did not win its decision.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum AlternativeStatus {
    /** Its counterfactual plan scored worse than the chosen plan by more than the tie epsilon. */
    LOST_ON_COST,
    /** Its score was within the tie epsilon and a tie-break rule preferred the chosen plan. */
    LOST_ON_TIE,
    /** It failed a memory fit check and was pruned before cost comparison. */
    REJECTED_FIT,
    /** No complete plan exists when the decision is restricted to this alternative. */
    NO_PLAN,
    /** The counterfactual search budget ran out before this alternative was costed. */
    NOT_COSTED;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
