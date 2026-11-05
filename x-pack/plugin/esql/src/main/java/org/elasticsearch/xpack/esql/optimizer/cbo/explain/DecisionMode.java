/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * How the optimizer arrived at a decision, so a reader knows whether the cost comparison actually drove the choice.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum DecisionMode {
    /** Two or more alternatives were costed and the cheapest feasible one won. */
    COST_BASED,
    /** A pragma fixed the alternative. */
    FORCED,
    /**
     * Statistics the comparison needs are unknown, so cost cannot pick. Aggregates keep the distribution their input already
     * has; other operators keep the incumbent.
     */
    GATED,
    /** Only one alternative is legal for this plan shape, feature gate, or transport version. */
    INELIGIBLE,
    /** Every enumerated alternative failed a fit check and a safe fallback was picked. */
    FIT_FALLBACK;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
