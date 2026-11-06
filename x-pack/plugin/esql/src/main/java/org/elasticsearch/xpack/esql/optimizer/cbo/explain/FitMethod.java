/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * How a fit check compared estimated memory demand against its budget, which determines how to read its result.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum FitMethod {
    /** The check passes when the probability that demand fits the budget reaches a threshold. */
    PROBABILITY,
    /** The check passes when the demand's upper bound fits the budget. */
    UPPER_BOUND;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
