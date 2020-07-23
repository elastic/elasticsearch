/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    public abstract long cardinality();

    static long linearCounting(long m, long v) {
        return Math.round(m * Math.log((double) m / v));
    }
}
