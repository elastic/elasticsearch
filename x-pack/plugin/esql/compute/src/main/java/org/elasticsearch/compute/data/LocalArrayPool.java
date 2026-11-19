/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;

/**
 * A pool of arrays for a single driver
 */
final class LocalArrayPool implements Releasable {
    static final int SLOTS = 32;
    static final long MIN_ARRAY_BYTES = ByteSizeValue.ofKb(2).getBytes();
    static final long MAX_ARRAY_BYTES = ByteSizeValue.ofKb(256).getBytes();
    static final long MAX_IDLE_BYTES = ByteSizeValue.ofMb(2).getBytes();

    private final LocalCircuitBreaker breaker;
    private final long[][] longs = new long[SLOTS][];
    private final int[][] ints = new int[SLOTS][];
    private final double[][] doubles = new double[SLOTS][];
    private int longCount;
    private int intCount;
    private int doubleCount;
    private long idleBytes;

    LocalArrayPool(LocalCircuitBreaker breaker) {
        this.breaker = breaker;
    }

    long[] pollLongs(int size) {
        assert breaker.assertSingleThread();
        if (longCount == 0 || poolable(usedBytesForLongs(size)) == false) {
            return null;
        }
        final long[] arr = longs[--longCount];
        longs[longCount] = null;
        final long bytes = usedBytesForLongs(arr.length);
        idleBytes -= bytes;
        assert idleBytes >= 0 : idleBytes;
        if (arr.length >= size) {
            return arr;
        }
        breaker.addWithoutBreaking(-bytes);
        return null;
    }

    void recycleLongs(long[] arr) {
        assert breaker.assertSingleThread();
        assert breaker.assertNotClosed();

        final long bytes = usedBytesForLongs(arr.length);
        if (longCount < SLOTS && accept(bytes)) {
            longs[longCount++] = arr;
        } else {
            breaker.addWithoutBreaking(-bytes);
        }
    }

    int[] pollInts(int size) {
        assert breaker.assertSingleThread();

        if (intCount == 0 || poolable(usedBytesForInts(size)) == false) {
            return null;
        }
        final int[] arr = ints[--intCount];
        ints[intCount] = null;
        final long bytes = usedBytesForInts(arr.length);
        idleBytes -= bytes;
        assert idleBytes >= 0 : idleBytes;
        if (arr.length >= size) {
            return arr;
        }
        breaker.addWithoutBreaking(-bytes);
        return null;
    }

    void recycleInts(int[] arr) {
        assert breaker.assertSingleThread();
        assert breaker.assertNotClosed();

        final long bytes = usedBytesForInts(arr.length);
        if (intCount < SLOTS && accept(bytes)) {
            ints[intCount++] = arr;
        } else {
            breaker.addWithoutBreaking(-bytes);
        }
    }

    double[] pollDoubles(int size) {
        assert breaker.assertSingleThread();

        if (doubleCount == 0 || poolable(usedBytesForDoubles(size)) == false) {
            return null;
        }
        final double[] arr = doubles[--doubleCount];
        doubles[doubleCount] = null;
        final long bytes = usedBytesForDoubles(arr.length);
        idleBytes -= bytes;
        assert idleBytes >= 0 : idleBytes;
        if (arr.length >= size) {
            return arr;
        }
        breaker.addWithoutBreaking(-bytes);
        return null;
    }

    void recycleDoubles(double[] arr) {
        assert breaker.assertSingleThread();
        assert breaker.assertNotClosed();

        final long bytes = usedBytesForDoubles(arr.length);
        if (doubleCount < SLOTS && accept(bytes)) {
            doubles[doubleCount++] = arr;
        } else {
            breaker.addWithoutBreaking(-bytes);
        }
    }

    @Override
    public void close() {
        assert breaker.assertSingleThread();

        breaker.addWithoutBreaking(-idleBytes);
        idleBytes = 0;
        Arrays.fill(longs, null);
        Arrays.fill(ints, null);
        Arrays.fill(doubles, null);
        longCount = 0;
        intCount = 0;
        doubleCount = 0;
    }

    long idleBytes() {
        return idleBytes;
    }

    private static boolean poolable(long bytes) {
        return MIN_ARRAY_BYTES <= bytes && bytes <= MAX_ARRAY_BYTES;
    }

    private boolean accept(long bytes) {
        if (poolable(bytes) == false) {
            return false;
        }
        final long totalBytes = idleBytes + bytes;
        if (totalBytes > MAX_IDLE_BYTES) {
            return false;
        }
        idleBytes += bytes;
        return true;
    }

    static long usedBytesForLongs(long elements) {
        return elements * Long.BYTES;
    }

    static long usedBytesForInts(long elements) {
        return elements * Integer.BYTES;
    }

    static long usedBytesForDoubles(long elements) {
        return elements * Double.BYTES;
    }
}
