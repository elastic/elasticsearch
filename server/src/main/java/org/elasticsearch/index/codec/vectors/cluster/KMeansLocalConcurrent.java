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
 * concurrent implementation of k-means
 */
class KMeansLocalConcurrent extends KMeansLocal {

    final TaskExecutor executor;
    final int numWorkers;

    KMeansLocalConcurrent(TaskExecutor executor, int numWorkers, int sampleSize, int maxIterations) {
        super(sampleSize, maxIterations);
        this.executor = executor;
        this.numWorkers = numWorkers;
    }

    @Override
    protected int numWorkers() {
        return numWorkers;
    }

    @Override
    protected boolean stepLloyd(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[][] centroids,
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
                () -> stepLloydSlice(vectors.copy(), ordTranslator, centroids, centroidChangedSlice, assignments, neighborHoods, start, end)
            );
        }
        final List<Boolean> hasChanges = executor.invokeAll(runners);
        return hasChanges.stream().anyMatch(Boolean::booleanValue);
    }

    @Override
    protected void assignSpilled(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        final int len = vectors.size() / numWorkers;
        final List<Callable<Void>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? vectors.size() : (i + 1) * len;
            runners.add(() -> {
                assignSpilledSlice(vectors.copy(), kmeansIntermediate, neighborhoods, soarLambda, start, end);
                return null;
            });
        }
        executor.invokeAll(runners);
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(float[][] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(executor, numWorkers, centroids, clustersPerNeighborhood);
    }
}
