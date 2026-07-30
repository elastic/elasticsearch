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
import java.util.function.IntUnaryOperator;

/**
 * Single threaded implementation of Lloyd's k-means.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
class LloydKMeansLocalSerial<V> extends LloydKMeansLocal<V> {

    LloydKMeansLocalSerial(CentroidOps<V> ops, int sampleSize, int maxIterations) {
        super(ops, sampleSize, maxIterations);
    }

    @Override
    protected int numWorkers() {
        return 1;
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
        assert centroidChangedSlices.length == 1;
        return stepLloydSlice(
            vectors,
            ops,
            ordTranslator,
            centroids,
            centroidChangedSlices[0],
            assignments,
            neighborHoods,
            0,
            vectors.size()
        );
    }

    @Override
    protected NeighborHood[] computeNeighborhoods(V[] centroids, int clustersPerNeighborhood) throws IOException {
        return NeighborHood.computeNeighborhoods(ops, centroids, clustersPerNeighborhood);
    }
}
