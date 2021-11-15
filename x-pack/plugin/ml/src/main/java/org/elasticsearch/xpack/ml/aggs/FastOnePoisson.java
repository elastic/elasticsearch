/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs;

import java.util.Arrays;

public class FastOnePoisson {

    private static final double[] CDF;
    private static final double[] COND_CDF;
    private static final int[] BUCKET_SAMPLES;
    private static final double[] CORRECTIONS;
    private static final int NEED_TO_GENERATE = 4;
    private static final double RNG_RANGE = 4294967296.0; // = 2^32
    static {
        CDF = new double[12];
        double f = Math.exp(-1.0);
        CDF[0] = f;
        for (int i = 1; i < 12; i++) {
            f /= (i);
            CDF[i] = CDF[i - 1] + f;
        }

        {
            f = Math.exp(-1.0);
            double F = f;
            int i = 0;
            for (; F < 255.0 / 256.0; i++) {
                f /= (i + 1);
                F += f;
            }
            COND_CDF = new double[12];
            COND_CDF[i] = F - 255.0 / 256.0;
            for (i++; i < COND_CDF.length; i++) {
                f /= (i);
                COND_CDF[i] = COND_CDF[i - 1] + f;
            }
        }
        for (int i = 0; i < COND_CDF.length; i++) {
            COND_CDF[i] /= COND_CDF[COND_CDF.length - 1];
        }

        BUCKET_SAMPLES = new int[256];
        for (int i = 0; i < BUCKET_SAMPLES.length; ++i) {
            BUCKET_SAMPLES[i] = sample(CDF, ((double) (i) + 0.5) / 256.0);
        }

        CORRECTIONS = new double[4];
        CORRECTIONS[0] = 256.0 * CDF[0] - 94.0;
        CORRECTIONS[1] = 256.0 * CDF[1] - 188.0;
        CORRECTIONS[2] = 256.0 * CDF[2] - 235.0;
        CORRECTIONS[3] = 256.0 * CDF[3] - 251.0;
    }

    private final PCG rng;
    private int currentSample = NEED_TO_GENERATE - 1;
    private final int[] currentSamples = new int[NEED_TO_GENERATE];

    public FastOnePoisson(PCG pcg) {
        rng = pcg;
    }

    public int next() {
        currentSample++;
        // Branch prediction should be essentially perfect here so this should be free.
        if (currentSample == NEED_TO_GENERATE) {
            generate();
            currentSample = 0;
        }
        return currentSamples[currentSample];
    }

    private void generate() {
        final int rn = rng.nextInt();
        currentSamples[0] = sample(rn & 255);
        currentSamples[1] = sample((rn >> 8) & 255);
        currentSamples[2] = sample((rn >> 16) & 255);
        currentSamples[3] = sample((rn >> 24) & 255);
    }

    // Fast 32 bit accurate Poisson sampling.
    private int sample(int bucket) {
        switch (bucket) {
            case 94: {
                final double u01 = 0.5 + (double) (rng.nextInt()) / RNG_RANGE;
                return u01 < CORRECTIONS[0] ? 0 : 1;
            }
            case 188: {
                final double u01 = 0.5 + (double) (rng.nextInt()) / RNG_RANGE;
                return u01 < CORRECTIONS[1] ? 1 : 2;
            }
            case 235: {
                final double u01 = 0.5 + (double) (rng.nextInt()) / RNG_RANGE;
                return u01 < CORRECTIONS[2] ? 2 : 3;
            }
            case 251: {
                final double u01 = 0.5 + (double) (rng.nextInt()) / RNG_RANGE;
                return u01 < CORRECTIONS[3] ? 3 : 4;
            }
            case 255: {
                // Fallback to expensive method but cost is amortised away.
                final double u01 = 0.5 + (double) (rng.nextInt()) / RNG_RANGE;
                return sample(COND_CDF, u01);
            }
            default:
                return BUCKET_SAMPLES[bucket];
        }
    }

    private static int sample(double[] cdf, double u01) {
        final int pos = Arrays.binarySearch(cdf, u01);
        return pos < 0 ? -pos - 1 : pos;
    }

}
