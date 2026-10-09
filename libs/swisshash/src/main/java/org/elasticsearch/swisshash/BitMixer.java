/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

/**
 * Bit mixing utilities. The purpose of these methods is to evenly
 * distribute key space over int32 range.
 * */
final class BitMixer {

    private BitMixer() {}

    /**
     * MH3's plain finalization step.
     */
    static int mix32(int k) {
        k = (k ^ (k >>> 16)) * 0x85ebca6b;
        k = (k ^ (k >>> 13)) * 0xc2b2ae35;
        return k ^ (k >>> 16);
    }

    /**
     * Computes David Stafford variant 9 of 64bit mix function (MH3
     * finalization step, with different shifts and constants).
     *
     * <p> Variant 9 is picked because it contains two 32-bit shifts
     * which could be possibly optimized into better machine code.
     */
    static long mix64(long z) {
        z = (z ^ (z >>> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >>> 29)) * 0xfc12c5b19d3259e9L;
        return z ^ (z >>> 32);
    }

    /**
     * Multiplicative (Fibonacci) hashing: a single multiply by an odd
     * constant close to 2^64 / golden-ratio spreads sequential and random
     * keys alike into its high bits, so we only need those bits rather
     * than the full multi-round avalanche {@link #mix64}/{@link #mix32}
     * provide.
     */
    static int mix(long key) {
        return (int) ((key * 0x9E3779B97F4A7C15L) >>> 32);
    }
}
