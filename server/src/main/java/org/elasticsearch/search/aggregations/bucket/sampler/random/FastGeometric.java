/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import java.util.Arrays;
import java.util.function.IntSupplier;

/**
 * Specialized class for randomly sampling values from the geometric distribution
 */
public class FastGeometric {
    private static final float[] LOOKUP_TABLE_FOR_FAST_LOG_2;
    private static final int BITS = 13;
    private static final int SHIFT = 23 - BITS;

    // E[log_2(U[0,1])] = [x log(x) - x]_0^1 / log(2) = -1.0 / log(2). The difference
    // between sum{ fastLog2((float)(j)) - log2RngRange } over all distinct integers
    // with n bits and this is the bias that the approximate log function introduces.
    private static final float[] BIASES = new float[] { -0.49294917f / 256.0f, 4.7501511f / 65536.0f, 31.557305f / 4294967296.0f };
    private static final int[] PERMITTED_BITS_PER_SAMPLE = new int[] { 8, 16, 32 };
    private static final float[] PERMITTED_SAMPLE_RANGES = new float[] { 256.0f, 65536.0f, 4294967296.0f };
    static {
        // Notes:
        // 1) The shift is the maximum mantissa / bins.
        // 2) The exponent is set to 126, which is 0 in two's complement.
        // 3) JVM float representation is [sign|exponent|mantissa].
        LOOKUP_TABLE_FOR_FAST_LOG_2 = new float[1 << BITS + 1];
        int dx = 1 << SHIFT;
        int x = 0x3F800000 + (dx / 2);
        float log2 = (float) Math.log(2.0);
        for (int i = 0; i < LOOKUP_TABLE_FOR_FAST_LOG_2.length; i++) {
            LOOKUP_TABLE_FOR_FAST_LOG_2[i] = (float) Math.log(Float.intBitsToFloat(x)) / log2;
            x += dx;
        }
    }

    // see https://www1.icsi.berkeley.edu/pubs/techreports/TR-07-002.pdf.
    //
    // Note this is log base 2.
    private static float fastLog2(float x) {
        assert x >= 0f : "Error generating fast log as provided value was less than 0";
        int xBits = Float.floatToIntBits(x);
        int log2x = ((xBits >>> 23) & 0xFF) - 127;
        int xMantissa = xBits & 0x7FFFFF;
        return LOOKUP_TABLE_FOR_FAST_LOG_2[xMantissa >>> SHIFT] + log2x;
    }

    private final IntSupplier rng;
    private int currentSample;
    private final int[] currentSamples;
    private final float norm;
    private final float rngRange;
    private final float log2RngRange;
    private final float bias;
    private final int mask;
    private final int bitsPerSample;
    private final int needToGenerate;

    /**
     * @param randomGen random integer supplier
     * @param p the probability of success, must be between (0.0-1.0)
     */
    public FastGeometric(IntSupplier randomGen, double p) {
        if (p <= 0.0 || p >= 1.0) {
            throw new IllegalArgumentException("[p] must be between 0.0 and 1.0, exclusive, was [" + p + "]");
        }
        this.rng = randomGen;
        int bits = (int) (1.5 * Math.log(Math.log(0.01) / Math.log(1.0 - p) - 1.0) / Math.log(2.0));
        int pos = Arrays.binarySearch(PERMITTED_BITS_PER_SAMPLE, bits);
        pos = pos < 0 ? Math.min(-pos - 1, PERMITTED_BITS_PER_SAMPLE.length - 1) : pos;
        bitsPerSample = PERMITTED_BITS_PER_SAMPLE[pos];
        needToGenerate = 32 / bitsPerSample;
        mask = (Integer.MAX_VALUE >>> (32 - bitsPerSample - 1));
        currentSample = needToGenerate - 1;
        currentSamples = new int[needToGenerate];
        norm = (float) (Math.log(2.0) / Math.log(1.0 - p));
        rngRange = PERMITTED_SAMPLE_RANGES[pos];
        log2RngRange = fastLog2(PERMITTED_SAMPLE_RANGES[pos]);
        bias = BIASES[pos];
    }

    /**
     * @return The next random sample from the geometric distribution
     */
    public int next() {
        currentSample++;
        // Branch prediction should be essentially perfect here so this should be free.
        if (currentSample == needToGenerate) {
            generate();
            currentSample = 0;
        }
        return currentSamples[currentSample] + 1;
    }

    private void generate() {
        int rn = rng.getAsInt();

        // Branch prediction should be essentially perfect here so this should be free.
        if (needToGenerate == 1) {
            float u = 0.5f * rngRange + (float) (rn) + 0.5f;
            currentSamples[0] = (int) (norm * (fastLog2(u) - log2RngRange));
        } else {
            for (int i = 0, shift = bitsPerSample; shift <= 32; i++, shift += bitsPerSample) {
                float u = (float) ((rn >>> shift) & mask) + 0.5f;
                currentSamples[i] = (int) (norm * (fastLog2(u) - log2RngRange + bias));
            }
        }
    }

}
