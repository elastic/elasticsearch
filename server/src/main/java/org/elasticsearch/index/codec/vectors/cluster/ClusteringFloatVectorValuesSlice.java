/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public final class ClusteringFloatVectorValuesSlice extends ClusteringFloatVectorValues {

    private final ClusteringFloatVectorValues allValues;
    private final IntToIntFunction ordTranslator;
    private final int size;

    public ClusteringFloatVectorValuesSlice(ClusteringFloatVectorValues allValues, IntToIntFunction ordTranslator, int size) {
        assert ordTranslator != null;
        assert allValues.size() >= size;
        this.allValues = allValues;
        this.ordTranslator = ordTranslator;
        this.size = size;
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        return this.allValues.vectorValue(ordTranslator.apply(ord));
    }

    @Override
    public int dimension() {
        return this.allValues.dimension();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int ordToDoc(int ord) {
        return ordTranslator.apply(ord);
    }

    @Override
    public ClusteringFloatVectorValuesSlice copy() throws IOException {
        return new ClusteringFloatVectorValuesSlice(this.allValues.copy(), this.ordTranslator, size);
    }

    static ClusteringFloatVectorValues createRandomSlice(ClusteringFloatVectorValues origin, int k, long seed) {
        if (k >= origin.size()) {
            return origin;
        }
        // TODO maybe use bigArrays?
        int[] samples = reservoirSample(origin.size(), k, seed);
        // sort to prevent random backwards access weirdness
        Arrays.sort(samples);
        return new ClusteringFloatVectorValuesSlice(origin, i -> samples[i], k);
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
