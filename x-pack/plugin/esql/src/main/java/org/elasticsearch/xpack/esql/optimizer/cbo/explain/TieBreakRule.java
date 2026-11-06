/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import java.util.Locale;

/**
 * The first rule that separated two plans whose scores were within the tie epsilon. New values are only ever appended because
 * the ordinal is the wire format, so the search applies {@link #BROADCAST_PREFERRED} first and then the others in declaration
 * order.
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
    ENUMERATION_ORDER,
    /** A subquery join that broadcasts its build side wins over materializing or partitioning it. */
    BROADCAST_PREFERRED;

    /** The name used for this value in the profile output. */
    public String displayName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
