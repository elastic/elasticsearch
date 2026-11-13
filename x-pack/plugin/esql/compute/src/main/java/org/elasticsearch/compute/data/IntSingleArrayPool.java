/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

public final class IntSingleArrayPool implements Releasable {
    private int[] pool;
    private long usedBytes;
    private final CircuitBreaker breaker;

    public IntSingleArrayPool(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    public int[] getOrAllocate(int size) {
        int[] arr = this.pool;
        this.pool = null;
        if (arr == null || arr.length < size) {
            final long newBytes = RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * size
            );
            if (newBytes > usedBytes) {
                breaker.addEstimateBytesAndMaybeBreak(newBytes - usedBytes, "int[]");
                usedBytes = newBytes;
            }
            arr = new int[size];
        }
        return arr;
    }

    public void put(int[] arr) {
        // the pool can be tolerated with races, where we just allocate a new array
        this.pool = arr;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-usedBytes);
    }
}
