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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.test.ESTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Shared test data generators for k-means clustering tests.
 */
final class KMeansTestData {

    private KMeansTestData() {}

    /**
     * Generate float vectors clustered around random centroids with uniform noise.
     * Centroids are in {@code [0, 100]} and noise is uniform {@code [-5, 5]}.
     */
    static KMeansFloatVectorValues generateFloatData(int nSamples, int nDims, int nClusters) {
        Random rng = random();
        List<float[]> vectors = new ArrayList<>(nSamples);
        float[][] centroids = new float[nClusters][nDims];
        // Generate random centroids
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = rng.nextFloat() * 100;
            }
        }
        // Generate data points around centroids
        for (int i = 0; i < nSamples; i++) {
            int cluster = rng.nextInt(nClusters);
            float[] vector = new float[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = centroids[cluster][j] + rng.nextFloat() * 10 - 5;
            }
            vectors.add(vector);
        }
        return KMeansFloatVectorValues.build(vectors, null, nDims);
    }

    /**
     * Generate float vectors clustered around random centroids with Gaussian noise.
     * Centroids are in {@code [0, 1]} and noise is {@code N(0, stdDev)}.
     */
    static KMeansFloatVectorValues generateFloatDataWithStdDev(int nSamples, int nDims, int nClusters, float stdDev) {
        Random rng = random();
        List<float[]> vectors = new ArrayList<>(nSamples);
        float[][] centroids = new float[nClusters][nDims];
        // Generate random centroids
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = rng.nextFloat();
            }
        }
        // Generate data points around centroids
        for (int i = 0; i < nSamples; i++) {
            int cluster = rng.nextInt(nClusters);
            float[] vector = new float[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = centroids[cluster][j] + (float) rng.nextGaussian() * stdDev;
            }
            vectors.add(vector);
        }
        return KMeansFloatVectorValues.build(vectors, null, nDims);
    }

    /**
     * Generate byte vectors clustered around random centroids.
     * Centroids are random bytes and noise is uniform {@code [-10, 10]} clamped to byte range.
     */
    static KMeansByteVectorValues generateByteData(int nSamples, int nDims, int nClusters) {
        Random rng = random();
        List<byte[]> vectors = new ArrayList<>(nSamples);
        byte[][] centroids = new byte[nClusters][nDims];
        // Generate random centroids
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = (byte) randomIntBetween(-128, 127);
            }
        }
        // Generate data points around centroids
        for (int i = 0; i < nSamples; i++) {
            int cluster = rng.nextInt(nClusters);
            byte[] vector = new byte[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = (byte) Math.clamp(centroids[cluster][j] + randomIntBetween(-10, 10), -128, 127);
            }
            vectors.add(vector);
        }
        return KMeansByteVectorValues.build(vectors, null, nDims);
    }

    /**
     * Performs basic k-means clustering, modifying {@code centroids} appropriately
     *
     * @param vectors the vectors to cluster
     * @param kMeans the k-means algorithm to use
     * @param centroids the initialized centroids to be shifted using k-means
     */
    public static <V> void runKMeans(ClusteringVectorValues<V> vectors, KMeansLocal<V> kMeans, V[] centroids) throws IOException {
        kMeans.cluster(vectors, new KMeansResult<>(centroids, new int[vectors.size()]));
    }

}
