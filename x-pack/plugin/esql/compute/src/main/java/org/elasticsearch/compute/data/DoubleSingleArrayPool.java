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

public final class DoubleSingleArrayPool implements Releasable {
    private double[] pool;
    private long usedBytes;
    private final CircuitBreaker breaker;

    public DoubleSingleArrayPool(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    public double[] getOrAllocate(int size) {
        double[] arr = this.pool;
        this.pool = null;
        if (arr == null || arr.length < size) {
            final long newBytes = RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * size
            );
            if (newBytes > usedBytes) {
                breaker.addEstimateBytesAndMaybeBreak(newBytes - usedBytes, "double[]");
                usedBytes = newBytes;
            }
            arr = new double[size];
        }
        return arr;
    }

    public void put(double[] arr) {
        // the pool can be tolerated with races, where we just allocate a new array
        this.pool = arr;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-usedBytes);
    }
}
