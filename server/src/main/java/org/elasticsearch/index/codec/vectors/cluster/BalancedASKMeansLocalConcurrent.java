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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Concurrent implementation of k-means with L2 regularization over the cluster sizes.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
class BalancedASKMeansLocalConcurrent<V> extends BalancedASKMeansLocal<V> {

    final TaskExecutor executor;
    final int numWorkers;

    BalancedASKMeansLocalConcurrent(CentroidOps<V> ops, TaskExecutor executor, int numWorkers, int sampleSize, int maxIterations) {
        super(ops, sampleSize, maxIterations);
        this.executor = executor;
        this.numWorkers = numWorkers;
    }

    @Override
    protected int numWorkers() {
        return numWorkers;
    }

    @Override
    protected void assign(
        ClusteringVectorValues<V> vectors,
        IntToIntFunction ordTranslator,
        V[] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException {
        assert numWorkers == centroidChangedSlices.length;
        final int len = vectors.size() / numWorkers;
        final List<Callable<Boolean>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? vectors.size() : (i + 1) * len;
            final FixedBitSet centroidChangedSlice = centroidChangedSlices[i];
            runners.add(
                () -> stepLloydSlice(
                    vectors.copy(),
                    ops,
                    ordTranslator,
                    centroids,
                    centroidChangedSlice,
                    assignments,
                    neighborHoods,
                    start,
                    end
                )
            );
        }
        executor.invokeAll(runners);
    }

    @Override
    protected void assignSpilled(
        ClusteringVectorValues<V> vectors,
        KMeansIntermediate<V> kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        final int len = vectors.size() / numWorkers;
        final List<Callable<Void>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? vectors.size() : (i + 1) * len;
            runners.add(() -> {
                assignSpilledSlice(vectors.copy(), ops, kmeansIntermediate, neighborhoods, soarLambda, start, end);
                return null;
            });
        }
        executor.invokeAll(runners);
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(V[] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(executor, numWorkers, ops.toFloatCentroids(centroids), clustersPerNeighborhood);
    }
}
