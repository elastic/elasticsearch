/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import java.util.Arrays;
import java.util.function.Supplier;

public class FastPoisson {

    private static final double[] cdf;
    private static final double[] condCdf;
    private static final int[] bucketSamples;
    private static final double[] corrections;
    private static final int needToGenerate = 4;
    private static final double rngRange = 4294967296.0; // = 2^32
    static {
        cdf = new double[12];
        double f = Math.exp(-1.0);
        cdf[0] = f;
        for (int i = 1; i < 12; i++) {
            f /= (double)(i);
            cdf[i] = cdf[i - 1] + f;
        }

        {
            f = Math.exp(-1.0);
            double F = f;
            int i = 0;
            for (/**/; F < 255.0 / 256.0; i++) {
                f /= (double)(i + 1);
                F += f;
            }
            condCdf = new double[12];
            condCdf[i] = F - 255.0 / 256.0;
            for (i++; i < condCdf.length; i++) {
                f /= (double)(i);
                condCdf[i] = condCdf[i - 1] + f;
            }
        }
        for (int i = 0; i < condCdf.length; i++) {
            condCdf[i] /= condCdf[condCdf.length - 1];
        }

        bucketSamples = new int[256];
        for (int i = 0; i < bucketSamples.length; ++i) {
            bucketSamples[i] = sample(cdf, ((double)(i) + 0.5) / 256.0);
        }

        corrections = new double[4];
        corrections[0] = 256.0 * cdf[0] -  94.0;
        corrections[1] = 256.0 * cdf[1] - 188.0;
        corrections[2] = 256.0 * cdf[2] - 235.0;
        corrections[3] = 256.0 * cdf[3] - 251.0;
    }

    private Supplier<Integer> randomIntSupplier;
    private int currentSample = needToGenerate - 1;
    private int[] currentSamples = new int[needToGenerate];

    public FastPoisson(Supplier<Integer> randomIntSupplier) {
        this.randomIntSupplier = randomIntSupplier;
    }

    public int next() {
        currentSample++;
        // Branch prediction should be essentially perfect here so this should be free.
        if (currentSample == needToGenerate) {
            generate();
            currentSample = 0;
        }
        return currentSamples[currentSample];
    }

    private void generate() {
        final int rn = randomIntSupplier.get();
        currentSamples[0] = sample( rn        & 255);
        currentSamples[1] = sample((rn >> 8)  & 255);
        currentSamples[2] = sample((rn >> 16) & 255);
        currentSamples[3] = sample((rn >> 24) & 255);
    }

    // Fast 32 bit accurate Poisson sampling.
    private int sample(int bucket) {
        switch (bucket) {
            case 94: {
                final double u01 = 0.5 + (double)(randomIntSupplier.get()) / rngRange;
                return u01 < corrections[0] ? 0 : 1;
            }
            case 188: {
                final double u01 = 0.5 + (double)(randomIntSupplier.get()) / rngRange;
                return u01 < corrections[1] ? 1 : 2;
            }
            case 235: {
                final double u01 = 0.5 + (double)(randomIntSupplier.get()) / rngRange;
                return u01 < corrections[2] ? 2 : 3;
            }
            case 251: {
                final double u01 = 0.5 + (double)(randomIntSupplier.get()) / rngRange;
                return u01 < corrections[3] ? 3 : 4;
            }
            case 255: {
                // Fallback to expensive method but cost is amortised away.
                final double u01 = 0.5 + (double)(randomIntSupplier.get()) / rngRange;
                return sample(condCdf, u01);
            }
            default:
                return bucketSamples[bucket];
        }
    }

    static private int sample(double[] cdf, double u01) {
        final int pos = Arrays.binarySearch(cdf, u01);
        return pos < 0 ? -pos - 1 : pos;
    }
}
