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
class BalancedKMeansLocalSerial extends BalancedKMeansLocal {

    BalancedKMeansLocalSerial(int sampleSize, int maxIterations) { super(sampleSize, maxIterations); }

    @Override
    protected int numWorkers() { return 1; }

    @Override
    protected void computeDistances(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[][] centroids,
        float[][] distances
    ) throws IOException {
        vectors.computeSquaredDistances(0, vectors.size(), centroids, ordTranslator, distances);
    }

    @Override
    protected void assign(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[][] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException {
        assert centroidChangedSlices.length == 1;
        stepLloydSlice(vectors, ordTranslator, centroids, centroidChangedSlices[0], assignments, neighborHoods, 0, vectors.size());
    }


    @Override
    protected void assignSpilled(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        assignSpilledSlice(vectors, kmeansIntermediate, neighborhoods, soarLambda, 0, vectors.size());
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(float[][] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(centroids, clustersPerNeighborhood);
    }
}
