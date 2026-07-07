/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;

import java.io.IOException;
import java.util.function.IntUnaryOperator;

/**
 * Concurrent implementation of Lloyd's k-means.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
class LloydKMeansLocalConcurrent<V> extends LloydKMeansLocal<V> {

    final TaskExecutor executor;
    final int numWorkers;
    final Soar<V> soar;

    LloydKMeansLocalConcurrent(
        CentroidOps<V> ops,
        TaskExecutor executor,
        int numWorkers,
        int sampleSize,
        int maxIterations,
        float soarLambda
    ) {
        super(ops, sampleSize, maxIterations);
        this.executor = executor;
        this.numWorkers = numWorkers;
        this.soar = soarLambda < 0 ? Soar.none() : Soar.ofConcurrent(executor, numWorkers, ops, soarLambda);
    }

    @Override
    protected int numWorkers() {
        return numWorkers;
    }

    @Override
    protected boolean stepLloyd(
        ClusteringVectorValues<V> vectors,
        IntUnaryOperator ordTranslator,
        V[] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException {
        return stepLloydSliceConcurrent(
            executor,
            numWorkers,
            vectors,
            ops,
            ordTranslator,
            centroids,
            centroidChangedSlices,
            assignments,
            neighborHoods
        );
    }

    @Override
    protected OverspillAssignments assignSpilled(
        ClusteringVectorValues<V> vectors,
        KMeansResult<V> kmeansResult,
        NeighborHood[] neighborhoods
    ) throws IOException {
        return soar.assignSpilled(vectors, kmeansResult, neighborhoods);
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(V[] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(ops, executor, numWorkers, centroids, clustersPerNeighborhood);
    }
}
