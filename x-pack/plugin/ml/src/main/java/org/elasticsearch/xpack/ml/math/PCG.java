/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.math;

/**
 * Permuted congruential generator style random number generator
 */
public final class PCG {

    private static final long MULTIPLIER = 6364136223846793005L;

    private long state = 0x4d595df4d0f33173L;
    private long increment = 1442695040888963407L;

    public PCG(long stream) {
        // The increment must be odd.
        increment = stream << 1 | 1;
    }

    /**
     * Different streams provide uncorrelated (with high probability)
     * distinct sequences of random numbers. This is useful to create multiple, uncorrelated random values from the same seed
     * @param seed the seed for the random number generation
     * @param stream the stream for this generator
     **/
    public PCG(long seed, long stream) {
        // The increment must be odd.
        state = seed + increment;
        increment = stream << 1 | 1;
        nextInt();
    }

    /**
     * @return Next random int between Integer.MIN_VALUE, Integer.MAX_VALUE
     */
    public int nextInt() {
        long x = state;
        int count = (int) (x >>> 59);
        state = x * MULTIPLIER + increment;
        x ^= x >>> 18;
        return rotr32((int) (x >>> 27), count);
    }

    private int rotr32(int x, int r) {
        return x >>> r | x << (-r & 31);
    }
}
