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
 * concurrent implementation of Lloyd's k-means
 */
class LloydKMeansLocalConcurrent extends LloydKMeansLocal {

    final TaskExecutor executor;
    final int numWorkers;

    LloydKMeansLocalConcurrent(TaskExecutor executor, int numWorkers, int sampleSize, int maxIterations) {
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
        return stepLloydSliceConcurrent(
            executor,
            numWorkers,
            vectors,
            ordTranslator,
            centroids,
            centroidChangedSlices,
            assignments,
            neighborHoods
        );
    }

    @Override
    protected void assignSpilled(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        assignSpilledConcurrent(executor, numWorkers, vectors, kmeansIntermediate, neighborhoods, soarLambda);
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(float[][] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(executor, numWorkers, centroids, clustersPerNeighborhood);
    }
}
