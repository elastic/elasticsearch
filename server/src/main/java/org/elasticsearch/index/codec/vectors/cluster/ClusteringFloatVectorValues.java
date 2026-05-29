/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;

import java.io.IOException;

/**
 * A {@link FloatVectorValues} that also implements {@link ClusteringVectorValues} for {@code float[]},
 * so that the same instance can be used with the generic k-means infrastructure.
 */
//<<<<<<< reduce-diskbbq-merge-cost-tiered-strategy
public abstract sealed class ClusteringFloatVectorValues extends FloatVectorValues permits KMeansFloatVectorValues,
    ClusteringFloatVectorValuesSlice, ConcatenatedClusteringFloatVectorValues {

    // the minimum distance that is considered to be "far enough" to a centroid in order to compute the soar distance.
    // For vectors that are closer than this distance to the centroid don't get spilled because they are well represented
    // by the centroid itself. In many cases, it indicates a degenerated distribution, e.g the cluster is composed of the
    // many equal vectors.
    private static final float SOAR_MIN_DISTANCE = 1e-16f;
    private static final int PREFIX_MIN_DIMENSIONS = 128;
    private static final float PREFIX_LENGTH_RATIO = 0.5f;
    // we require all prefixes to be a multiple 64, we want to take best advantage of vectorization
    private static final int PREFIX_MULTIPLE = 64;
    private static final int PREFIX_TOPK_SIZE = 4;
//=======
public abstract sealed class ClusteringFloatVectorValues extends FloatVectorValues implements ClusteringVectorValues<float[]> permits
    KMeansFloatVectorValues, ClusteringFloatVectorValuesSlice {
//>>>>>>> main

    @Override
    public abstract ClusteringFloatVectorValues copy() throws IOException;

    @Override
    public abstract int ordToDoc(int ord);
}
