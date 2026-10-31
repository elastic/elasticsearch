/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * The first rule that separated two plans whose scores were within the tie epsilon, listed in the order the search applies them.
 * New values are only ever appended because the ordinal is the wire format.
 */
public enum TieBreakRule {
    /** The incumbent plan is kept. */
    INCUMBENT,
    /** The plan with the lower peak memory per lane wins. */
    PEAK_LANE_BYTES,
    /** The plan with the lower peak memory per node wins. */
    PEAK_NODE_BYTES,
    /** The plan with fewer shuffle edges wins. */
    SHUFFLE_EDGES,
    /** The plan enumerated first wins. */
    ENUMERATION_ORDER;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
