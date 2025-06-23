/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A shared limiter used by multiple drivers to collect hits in parallel without exceeding the output limit.
 * For example, if the query `FROM test-1,test-2 | LIMIT 100` is run with two drivers, and one driver (e.g., querying `test-1`)
 * has collected 60 hits, then the other driver querying `test-2` should collect at most 40 hits.
 */
public class Limiter {
    private final int limit;
    private final AtomicInteger collected = new AtomicInteger();

    public static Limiter NO_LIMIT = new Limiter(Integer.MAX_VALUE) {
        @Override
        public int tryAccumulateHits(int numHits) {
            return numHits;
        }

        @Override
        public int remaining() {
            return Integer.MAX_VALUE;
        }
    };

    public Limiter(int limit) {
        this.limit = limit;
    }

    /**
     * Returns the remaining number of hits that can be collected.
     */
    public int remaining() {
        final int remaining = limit - collected.get();
        assert remaining >= 0 : remaining;
        return remaining;
    }

    /**
     * Returns the limit of this limiter.
     */
    public int limit() {
        return limit;
    }

    /**
     * Tries to accumulate hits and returns the number of hits that has been accepted.
     *
     * @param numHits the number of hits to try to accumulate
     * @return the accepted number of hits. If the returned number is less than the numHits,
     * it means the limit has been reached and the difference can be discarded.
     */
    public int tryAccumulateHits(int numHits) {
        while (true) {
            int curVal = collected.get();
            if (curVal >= limit) {
                return 0;
            }
            final int toAccept = Math.min(limit - curVal, numHits);
            if (collected.compareAndSet(curVal, curVal + toAccept)) {
                return toAccept;
            }
        }
    }
}
