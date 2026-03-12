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
        Random rnd = new Random(seed);
        int[] reservoir = new int[k];
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }
        for (int i = k; i < n; i++) {
            int j = rnd.nextInt(i + 1);
            if (j < k) {
                reservoir[j] = i;
            }
        }
        return reservoir;
    }
}
