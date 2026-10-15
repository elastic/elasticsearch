/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

/** The share of vectors the filters of a query leave; narrow filters are harder on the index and hide in averages. */
public enum Selectivity {
    /** The query has no filter. */
    UNFILTERED,
    /** The filters leave less than a twentieth of the vectors. */
    LOW,
    /** The filters leave from a twentieth to half of the vectors. */
    MEDIUM,
    /** The filters leave at least half of the vectors. */
    HIGH;

    public static Selectivity of(double fraction) {
        if (fraction >= 0.5) {
            return HIGH;
        }
        return fraction >= 0.05 ? MEDIUM : LOW;
    }
}
