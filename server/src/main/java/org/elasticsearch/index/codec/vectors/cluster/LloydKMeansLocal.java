/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;
import java.util.Arrays;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
abstract class LloydKMeansLocal extends KMeansLocal {

    private final int sampleSize;
    private final int maxIterations;

    LloydKMeansLocal(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
    }

    /** Number of workers to use for parallelism **/
    protected abstract int numWorkers();

    /** assign to each vector the closest centroid **/
    protected abstract boolean stepLloyd(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction translateOrd,
        float[][] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException;

    /** assign to each vector the soar assignment **/
    protected abstract void assignSpilled(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException;

    /** compute the neighborhoods for the given centroids and clustersPerNeighborhood */
    protected abstract NeighborHood[] computeNeighborhoods(float[][] centroids, int clustersPerNeighborhood) throws IOException;

    @Override
    protected void innerCluster(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, NeighborHood[] neighborhoods)
        throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();
        int[] assignments = kMeansIntermediate.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }
        IntToIntFunction ordTranslator = i -> i;
        ClusteringFloatVectorValues sampledVectors = vectors;
        if (sampleSize < n) {
            sampledVectors = ClusteringFloatVectorValuesSlice.createRandomSlice(vectors, sampleSize, 42L);
            ordTranslator = sampledVectors::ordToDoc;
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }
        int[] centroidCounts = new int[centroids.length];
        for (int i = 0; i < maxIterations; i++) {
            // This is potentially sampled, so we need to translate ordinals
            if (stepLloyd(sampledVectors, ordTranslator, centroids, centroidChangedSlices, assignments, neighborhoods)) {
                sampledVectors.updateCentroids(centroids, ordTranslator, centroidChangedSlices, centroidCounts, assignments);
            } else {
                break;
            }
        }
        // If we were sampled, do a once over the full set of vectors to finalize the centroids
        if (sampleSize < n || maxIterations == 0) {
            // No ordinal translation needed here, we are using the full set of vectors
            if (stepLloyd(vectors, i -> i, centroids, centroidChangedSlices, assignments, neighborhoods)) {
                sampledVectors.updateCentroids(centroids, ordTranslator, centroidChangedSlices, centroidCounts, assignments);
            }
        }
    }

    /**
     * helper that calls {@link LloydKMeansLocal#cluster(ClusteringFloatVectorValues, KMeansIntermediate)} given a set of initialized
     * centroids, this call is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(ClusteringFloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations)
        throws IOException {
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, new int[vectors.size()], vectors::ordToDoc);
        LloydKMeansLocal kMeans = new LloydKMeansLocalSerial(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }
}
