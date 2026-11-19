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
import java.util.function.IntUnaryOperator;

/**
 * A slice (subset) of a {@link ClusteringVectorValues} that maps ordinals through a translation function.
 * This is the generic replacement for {@link ClusteringFloatVectorValuesSlice}.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
public final class ClusteringVectorValuesSlice<V> implements ClusteringVectorValues<V> {

    private final ClusteringVectorValues<V> allValues;
    private final IntUnaryOperator ordTranslator;
    private final int size;

    public ClusteringVectorValuesSlice(ClusteringVectorValues<V> allValues, IntUnaryOperator ordTranslator, int size) {
        assert ordTranslator != null;
        assert allValues.size() >= size;
        this.allValues = allValues;
        this.ordTranslator = ordTranslator;
        this.size = size;
    }

    @Override
    public V vectorValue(int ord) throws IOException {
        return this.allValues.vectorValue(ordTranslator.applyAsInt(ord));
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
        return ordTranslator.applyAsInt(ord);
    }

    @Override
    public ClusteringVectorValuesSlice<V> copy() throws IOException {
        return new ClusteringVectorValuesSlice<>(this.allValues.copy(), this.ordTranslator, size);
    }

    /**
     * Create a new random slice by sampling indices using reservoir sampling.
     *
     * @param origin the input vectors to sample
     * @param k      the number of samples
     * @param seed   the random seed
     */
    public static <V> ClusteringVectorValues<V> createRandomSlice(ClusteringVectorValues<V> origin, int k, long seed) {
        if (k >= origin.size()) {
            return origin;
        }
        int[] samples = reservoirSample(origin.size(), k, seed);
        Arrays.sort(samples);
        return new ClusteringVectorValuesSlice<>(origin, i -> samples[i], k);
    }

    /**
     * Create a new random slice by sampling indices using reservoir sampling.
     *
     * @param origin  the input vectors to sample
     * @param k       the number of samples
     * @param seed    the random seed
     * @param samples where to store the sampled indices
     */
    public static <V> ClusteringVectorValues<V> createRandomSlice(ClusteringVectorValues<V> origin, int k, long seed, int[] samples) {
        if (k >= origin.size()) {
            return origin;
        }
        innerReservoirSample(origin.size(), seed, samples);
        Arrays.sort(samples);
        return new ClusteringVectorValuesSlice<>(origin, i -> samples[i], k);
    }

    static int[] reservoirSample(int n, int k, long seed) {
        int[] reservoir = new int[k];
        innerReservoirSample(n, seed, reservoir);
        return reservoir;
    }

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
