/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;

public final class SingleArrayPool implements BlockLoader.ArrayPool {
    private final CircuitBreaker breaker;
    private final Runnable onClose;
    private long[] longs;
    private int[] ints;
    private double[] doubles;
    private long longBytes;
    private long intBytes;
    private long doubleBytes;

    public SingleArrayPool(CircuitBreaker breaker, Runnable onClose) {
        this.breaker = breaker;
        this.onClose = onClose;
    }

    public long[] getOrAllocateLongs(int size) {
        long[] arr = this.longs;
        this.longs = null;
        if (arr == null || arr.length < size) {
            longBytes = reserve(longBytes, Long.BYTES, size, "long[]");
            arr = new long[size];
        }
        return arr;
    }

    public int[] getOrAllocateInts(int size) {
        int[] arr = this.ints;
        this.ints = null;
        if (arr == null || arr.length < size) {
            intBytes = reserve(intBytes, Integer.BYTES, size, "int[]");
            arr = new int[size];
        }
        return arr;
    }

    public double[] getOrAllocateDoubles(int size) {
        double[] arr = this.doubles;
        this.doubles = null;
        if (arr == null || arr.length < size) {
            doubleBytes = reserve(doubleBytes, Double.BYTES, size, "double[]");
            arr = new double[size];
        }
        return arr;
    }

    // the pool can be tolerated with races, where we just allocate a new array
    public void putLongs(long[] arr) {
        this.longs = arr;
    }

    public void putInts(int[] arr) {
        this.ints = arr;
    }

    public void putDoubles(double[] arr) {
        this.doubles = arr;
    }

    private long reserve(long usedBytes, int elementBytes, int size, String label) {
        final long newBytes = RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) elementBytes * size
        );
        if (newBytes > usedBytes) {
            breaker.addEstimateBytesAndMaybeBreak(newBytes - usedBytes, label);
            return newBytes;
        }
        return usedBytes;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-(longBytes + intBytes + doubleBytes));
        onClose.run();
    }
}
