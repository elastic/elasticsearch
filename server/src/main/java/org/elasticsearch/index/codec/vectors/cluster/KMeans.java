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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm
 */
public class KMeans {

    final int sampleSize;
    final int maxIterations;

    KMeans(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
    }

    /**
     * uses a FORGY approach to picking the initial centroids which are subsequently expected to be used by a clustering algorithm
     *
     * @param vectors used to pick an initial set of random centroids
     * @param sampleSize the total number of vectors to be used as part of the sample for centroids
     * @param centroidCount the total number of centroids to pick
     * @return randomly selected centroids that are the min of centroidCount and sampleSize
     * @throws IOException is thrown if vectors is inaccessible
     */
    public static float[][] pickInitialCentroids(FloatVectorValues vectors, int sampleSize, int centroidCount) throws IOException {
        // Choose data points as random ensuring we have distinct points where possible
        List<Integer> candidates = new ArrayList<>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            candidates.add(i);
        }
        Collections.shuffle(candidates, new Random(42L));

        float[][] centroids = new float[centroidCount][vectors.dimension()];
        int centroidIdx = 0;
        for (int i = 0; i < candidates.size() && centroidIdx < centroidCount; i++) {
            int cand = candidates.get(i);
            float[] vector = vectors.vectorValue(cand);
            boolean goodCandidate = true;
            if (((candidates.size() - i) - (centroidCount - centroidIdx)) > 0) {
                for (int j = 0; j < centroidIdx; j++) {
                    if ((VectorUtil.squareDistance(vector, centroids[j]) > 0.0f) == false) {
                        goodCandidate = false;
                        break;
                    }
                }
            }
            if (goodCandidate) {
                System.arraycopy(vector, 0, centroids[centroidIdx], 0, vector.length);
                centroidIdx++;
            }
        }
        return centroids;
    }

    private boolean stepLloyd(
        FloatVectorValues vectors,
        float[][] centroids,
        short[] assignments,
        int sampleSize,
        ClusteringAugment augment
    ) throws IOException {
        boolean changed = false;
        int dim = vectors.dimension();
        long[] centroidCounts = new long[centroids.length];
        float[][] nextCentroids = new float[centroids.length][dim];

        for (int i = 0; i < sampleSize; i++) {
            float[] vector = vectors.vectorValue(i);
            short bestCentroidOffset = getBestCentroidOffset(centroids, vector, i, augment);
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
                for (int d = 0; d < dim; d++) {
                    centroids[clusterIdx][d] = nextCentroids[clusterIdx][d] / countF;
                }
            }
        }

        return changed;
    }

    short getBestCentroidOffset(float[][] centroids, float[] vector, int vectorIdx, ClusteringAugment augment) {
        short bestCentroidOffset = -1;
        float minDsq = Float.MAX_VALUE;
        for (short j = 0; j < centroids.length; j++) {
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
     * @param kMeansResult the output object to populate which minimally includes centroids,
     *                     but may include assignments and soar assignments as well; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     * @throws IOException is thrown if vectors is inaccessible
     */
    void cluster(FloatVectorValues vectors, KMeansResult kMeansResult) throws IOException {
        cluster(vectors, kMeansResult, new ClusteringAugment());
    }

    void cluster(FloatVectorValues vectors, KMeansResult kMeansResult, ClusteringAugment augment) throws IOException {
        float[][] centroids = kMeansResult.centroids();
        int k = centroids.length;
        int n = vectors.size();

        if (k == 1 || k >= n) {
            return;
        }

        short[] assignments = new short[n];
        for (int i = 0; i < maxIterations; i++) {
            if (stepLloyd(vectors, centroids, assignments, sampleSize, augment) == false) {
                break;
            }
        }
        stepLloyd(vectors, centroids, assignments, vectors.size(), augment);
    }

    /**
     * helper that calls {@link KMeans#cluster(FloatVectorValues, KMeansResult)} given a set of initialized centroids
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(FloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations) throws IOException {
        KMeansResult kMeansResult = new KMeansResult(centroids);
        KMeans kMeans = new KMeans(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansResult);
    }

    /**
     * Provides additional information when computing distances between vectors and centroids such as neighborhood
     */
    static class ClusteringAugment {}
}
