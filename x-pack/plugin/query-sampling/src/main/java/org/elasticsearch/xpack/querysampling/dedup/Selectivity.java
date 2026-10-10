/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

/**
 * How much of the vectors of the indices a query is allowed to return, which is what the filters of the query leave of
 * them. Searches with filters can do very differently from those without: the narrower the filter, the harder it is for
 * the index to find the neighbours that pass it, and an average over all queries hides it when they are the few.
 */
public enum Selectivity {
    /** The query has no filter. */
    UNFILTERED,
    /** The filters leave less than a twentieth of the vectors. */
    LOW,
    /** The filters leave from a twentieth to half of the vectors. */
    MEDIUM,
    /** The filters leave at least half of the vectors. */
    HIGH;

    /**
     * The share of the vectors that the filters of a query leave, from which {@link #LOW}, {@link #MEDIUM} or {@link #HIGH}
     * follows.
     */
    public static Selectivity of(double fraction) {
        if (fraction >= 0.5) {
            return HIGH;
        }
        return fraction >= 0.05 ? MEDIUM : LOW;
    }
}
