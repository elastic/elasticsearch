/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

/**
 * Base class for cardinality related algorithms.
 */
abstract class AbstractCardinalityAlgorithm {

    public static final int MIN_PRECISION = 4;
    public static final int MAX_PRECISION = 18;

    protected final int p;

    AbstractCardinalityAlgorithm(int precision) {
        if (precision < MIN_PRECISION) {
            throw new IllegalArgumentException("precision must be >= 4");
        }
        if (precision > MAX_PRECISION) {
            throw new IllegalArgumentException("precision must be <= 18");
        }
        p = precision;
    }

    /** Precision of the algorithm */
    public int precision() {
        return p;
    }

    /** Returns the current computed cardinality */
    public abstract long cardinality(long bucketOrd);

    static long linearCounting(long m, long v) {
        return Math.round(m * Math.log((double) m / v));
    }
}
