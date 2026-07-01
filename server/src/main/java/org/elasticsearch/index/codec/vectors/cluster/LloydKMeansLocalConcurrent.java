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
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;

/**
 * Concurrent implementation of Lloyd's k-means.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
class LloydKMeansLocalConcurrent<V> extends LloydKMeansLocal<V> {

    final TaskExecutor executor;
    final int numWorkers;

    LloydKMeansLocalConcurrent(CentroidOps<V> ops, TaskExecutor executor, int numWorkers, int sampleSize, int maxIterations) {
        super(ops, sampleSize, maxIterations);
        this.executor = executor;
        this.numWorkers = numWorkers;
    }

    @Override
    protected int numWorkers() {
        return numWorkers;
    }

    @Override
    protected boolean stepLloyd(
        ClusteringVectorValues<V> vectors,
        IntToIntFunction ordTranslator,
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
    protected int[] assignSpilled(
        ClusteringVectorValues<V> vectors,
        KMeansIntermediate<V> kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        return assignSpilledConcurrent(executor, numWorkers, vectors, ops, kmeansIntermediate, neighborhoods, soarLambda);
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(V[] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(ops, executor, numWorkers, centroids, clustersPerNeighborhood);
    }
}
