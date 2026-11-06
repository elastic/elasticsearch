/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * Why an alternative was never enumerated for a decision, as opposed to enumerated and beaten.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum ExclusionCode {
    /** Another alternative was forced by a pragma. */
    PRAGMA_FORCED,
    /** A pragma disables this alternative. */
    PRAGMA_DISABLED,
    /** Shuffle execution is off, rejected for this plan, or not supported by every node. */
    SHUFFLE_UNAVAILABLE,
    /** The plan shape does not support this alternative. */
    NOT_ELIGIBLE,
    /** Statistics the comparison needs are unknown. */
    UNKNOWN_STATISTICS;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
