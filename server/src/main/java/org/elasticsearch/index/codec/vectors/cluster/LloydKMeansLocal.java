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

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntUnaryOperator;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
abstract class LloydKMeansLocal<V> extends KMeansLocal<V> {

    private final int sampleSize;
    private final int maxIterations;

    LloydKMeansLocal(CentroidOps<V> ops, int sampleSize, int maxIterations) {
        super(ops);
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
    }

    /** assign to each vector the closest centroid */
    protected abstract boolean stepLloyd(
        ClusteringVectorValues<V> vectors,
        IntUnaryOperator translateOrd,
        V[] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException;

    @Override
    protected void innerCluster(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult, NeighborHood[] neighborhoods)
        throws IOException {
        V[] centroids = kMeansResult.centroids();
        int k = centroids.length;
        int n = vectors.size();
        int[] assignments = kMeansResult.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }
        IntUnaryOperator ordTranslator = IntUnaryOperator.identity();
        ClusteringVectorValues<V> sampledVectors = vectors;
        if (sampleSize < n) {
            sampledVectors = ClusteringVectorValuesSlice.createRandomSlice(vectors, sampleSize, 42L);
            ordTranslator = sampledVectors::ordToDoc;
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }
        int[] centroidCounts = new int[centroids.length];
        CentroidOps.AccumulatorState<V> accumulatorState = ops.newAccumulatorState(centroids, k, vectors.dimension());
        for (int i = 0; i < maxIterations; i++) {
            // This is potentially sampled, so we need to translate ordinals
            if (stepLloyd(sampledVectors, ordTranslator, centroids, centroidChangedSlices, assignments, neighborhoods)) {
                CentroidAssignment.updateCentroids(
                    sampledVectors,
                    centroids,
                    ordTranslator,
                    centroidChangedSlices,
                    centroidCounts,
                    assignments,
                    accumulatorState
                );
            } else {
                break;
            }
        }
        // If we were sampled, do a once over the full set of vectors to finalize the centroids
        if (sampleSize < n || maxIterations == 0) {
            // No ordinal translation needed here, we are using the full set of vectors
            if (stepLloyd(vectors, IntUnaryOperator.identity(), centroids, centroidChangedSlices, assignments, neighborhoods)) {
                CentroidAssignment.updateCentroids(
                    sampledVectors,
                    centroids,
                    ordTranslator,
                    centroidChangedSlices,
                    centroidCounts,
                    assignments,
                    accumulatorState
                );
            }
        }
    }
}
