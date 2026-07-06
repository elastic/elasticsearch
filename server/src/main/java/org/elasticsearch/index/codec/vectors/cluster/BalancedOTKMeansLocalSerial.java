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
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;

import java.io.IOException;
import java.util.function.IntUnaryOperator;

/**
 * Single threaded implementation of mini-batch optimal transport k-means.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
class BalancedOTKMeansLocalSerial<V> extends BalancedOTKMeansLocal<V> {

    private final Soar<V> soar;

    BalancedOTKMeansLocalSerial(CentroidOps<V> ops, int sampleSize, int maxIterations, float soarLambda) {
        super(ops, sampleSize, maxIterations);
        soar = soarLambda < 0 ? Soar.none() : Soar.ofSerial(ops, soarLambda);
    }

    @Override
    protected int numWorkers() {
        return 1;
    }

    @Override
    protected void assign(
        ClusteringVectorValues<V> vectors,
        IntUnaryOperator ordTranslator,
        V[] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException {
        assert centroidChangedSlices.length == 1;
        stepLloydSlice(vectors, ops, ordTranslator, centroids, centroidChangedSlices[0], assignments, neighborHoods, 0, vectors.size());
    }

    @Override
    protected OverspillAssignments assignSpilled(
        ClusteringVectorValues<V> vectors,
        KMeansResult<V> kMeansResult,
        NeighborHood[] neighborhoods
    ) throws IOException {
        return soar.assignSpilled(vectors, kMeansResult, neighborhoods);
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(V[] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(ops, centroids, clustersPerNeighborhood);
    }
}
