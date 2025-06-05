/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm
 */
class KMeans {

    final int sampleSize;
    final int maxIterations;

    KMeans(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
    }

    /**
     * uses a Reservoir Sampling approach to picking the initial centroids which are subsequently expected
     * to be used by a clustering algorithm
     *
     * @param vectors used to pick an initial set of random centroids
     * @param centroidCount the total number of centroids to pick
     * @return randomly selected centroids that are the min of centroidCount and sampleSize
     * @throws IOException is thrown if vectors is inaccessible
     */
    static float[][] pickInitialCentroids(FloatVectorValues vectors, int centroidCount) throws IOException {
        Random random = new Random(42L);
        int centroidsSize = Math.min(vectors.size(), centroidCount);
        float[][] centroids = new float[centroidsSize][vectors.dimension()];
        for (int i = 0; i < vectors.size(); i++) {
            float[] vector;
            if (i < centroidCount) {
                vector = vectors.vectorValue(i);
                System.arraycopy(vector, 0, centroids[i], 0, vector.length);
            } else if (random.nextDouble() < centroidCount * (1.0 / i)) {
                int c = random.nextInt(centroidCount);
                vector = vectors.vectorValue(i);
                System.arraycopy(vector, 0, centroids[c], 0, vector.length);
            }
        }
        return centroids;
    }

    private boolean stepLloyd(
        FloatVectorValues vectors,
        float[][] centroids,
        float[][] nextCentroids,
        int[] assignments,
        int sampleSize,
        ClusteringAugment augment
    ) throws IOException {
        boolean changed = false;
        int dim = vectors.dimension();
        int[] centroidCounts = new int[centroids.length];

        for (int i = 0; i < nextCentroids.length; i++) {
            Arrays.fill(nextCentroids[i], 0.0f);
        }

        for (int i = 0; i < sampleSize; i++) {
            float[] vector = vectors.vectorValue(i);
            int bestCentroidOffset = getBestCentroidOffset(centroids, vector, i, augment);
            if (assignments[i] != bestCentroidOffset) {
                changed = true;
            }
            assignments[i] = bestCentroidOffset;
            centroidCounts[bestCentroidOffset]++;
            for (short d = 0; d < dim; d++) {
                nextCentroids[bestCentroidOffset][d] += vector[d];
            }
        }

        for (int clusterIdx = 0; clusterIdx < centroids.length; clusterIdx++) {
            if (centroidCounts[clusterIdx] > 0) {
                float countF = (float) centroidCounts[clusterIdx];
                for (short d = 0; d < dim; d++) {
                    centroids[clusterIdx][d] = nextCentroids[clusterIdx][d] / countF;
                }
            }
        }

        return changed;
    }

    int getBestCentroidOffset(float[][] centroids, float[] vector, int vectorIdx, ClusteringAugment augment) {
        int bestCentroidOffset = -1;
        float minDsq = Float.MAX_VALUE;
        for (int j = 0; j < centroids.length; j++) {
            float dsq = VectorUtil.squareDistance(vector, centroids[j]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = j;
            }
        }
        return bestCentroidOffset;
    }

    /**
     * cluster using a lloyd k-means algorithm
     *
     * @param vectors the vectors to cluster
     * @param kMeansIntermediate the output object to populate which minimally includes centroids,
     *                     but may include assignments and soar assignments as well; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     * @throws IOException is thrown if vectors is inaccessible
     */
    void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
        cluster(vectors, kMeansIntermediate, new ClusteringAugment());
    }

    void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, ClusteringAugment augment) throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();

        if (k == 1 || k >= n) {
            return;
        }

        int[] assignments = new int[n];
        float[][] nextCentroids = new float[centroids.length][vectors.dimension()];
        for (int i = 0; i < maxIterations; i++) {
            if (stepLloyd(vectors, centroids, nextCentroids, assignments, sampleSize, augment) == false) {
                break;
            }
        }
        stepLloyd(vectors, centroids, nextCentroids, assignments, vectors.size(), augment);
    }

    /**
     * helper that calls {@link KMeans#cluster(FloatVectorValues, KMeansIntermediate)} given a set of initialized centroids
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(FloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations) throws IOException {
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids);
        KMeans kMeans = new KMeans(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }

    /**
     * Provides additional information when computing distances between vectors and centroids such as neighborhood
     */
    static class ClusteringAugment {}
}
