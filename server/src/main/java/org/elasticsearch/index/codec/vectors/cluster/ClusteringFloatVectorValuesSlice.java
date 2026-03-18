/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

final class ClusteringFloatVectorValuesSlice extends ClusteringFloatVectorValues {

    private final ClusteringFloatVectorValues allValues;
    private final int[] slice;

    ClusteringFloatVectorValuesSlice(ClusteringFloatVectorValues allValues, int[] slice) {
        assert slice != null;
        assert slice.length <= allValues.size();
        this.allValues = allValues;
        this.slice = slice;
    }

    ClusteringFloatVectorValuesSlice(ClusteringFloatVectorValues allValues, int size) {
        assert size <= allValues.size();
        this.allValues = allValues;
        // TODO maybe use bigArrays?
        this.slice =  new int[size];
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        return this.allValues.vectorValue(this.slice[ord]);
    }

    @Override
    public int dimension() {
        return this.allValues.dimension();
    }

    @Override
    public int size() {
        return slice.length;
    }

    @Override
    public int ordToDoc(int ord) {
        return this.slice[ord];
    }

    @Override
    public ClusteringFloatVectorValuesSlice copy() throws IOException {
        return new ClusteringFloatVectorValuesSlice(this.allValues.copy(), this.slice);
    }

    /**
     * Update the slice by resampling its indices. The resampling uses reservoir sampling.
     * This avoids re-allocating the underlying arrays
     * @param seed the random seed
     */
    public void updateRandomSlice(long seed) {
        innerReservoirSample(allValues.size(), seed, slice);
        // sort to prevent random backwards access weirdness
        Arrays.sort(slice);
    }

    /**
     * Create a new random slice by sampling indices using reservoir sampling.
     * @param origin the input vectors to sample
     * @param k the number of samples
     * @param seed the random seed
     */
    static ClusteringFloatVectorValues createRandomSlice(ClusteringFloatVectorValues origin, int k, long seed) {
        if (k >= origin.size()) {
            return origin;
        }
        // TODO maybe use bigArrays?
        int[] samples = reservoirSample(origin.size(), k, seed);
        // sort to prevent random backwards access weirdness
        Arrays.sort(samples);
        return new ClusteringFloatVectorValuesSlice(origin, samples);
    }

    /**
     * Sample k elements from n elements according to reservoir sampling algorithm.
     *
     * @param n number of elements
     * @param k number of samples
     * @param seed random seed
     * @return array of k samples
     */
    static int[] reservoirSample(int n, int k, long seed) {
        int[] reservoir = new int[k];
        innerReservoirSample(n, seed, reservoir);
        return reservoir;
    }

    /**
     * Sample k elements from n elements according to reservoir sampling algorithm.
     *
     * @param n number of elements
     * @param seed random seed
     * @param reservoir array of samples
     */
    private static void innerReservoirSample(int n, long seed, int[] reservoir) {
        Random rnd = new Random(seed);
        for (int i = 0; i < reservoir.length; i++) {
            reservoir[i] = i;
        }
        for (int i = reservoir.length; i < n; i++) {
            int j = rnd.nextInt(i + 1);
            if (j < reservoir.length) {
                reservoir[j] = i;
            }
        }
    }
}
