/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;

import java.io.IOException;
import java.util.Arrays;

/**
 * An interface for that supply centroids.
 */
public interface CentroidSupplier {

    int size();

    float[] centroid(int centroidOrdinal) throws IOException;

    default KMeansResult secondLevelClusters() throws IOException {
        return null;
    }

    KMeansFloatVectorValues asKmeansFloatVectorValues() throws IOException;

    static CentroidSupplier empty(int dims) {
        return fromArray(new float[0][dims], KMeansResult.EMPTY, dims);
    }

    static CentroidSupplier fromArray(float[][] centroids, KMeansResult secondLevelClusters, int dims) {
        return new CentroidSupplier() {
            @Override
            public int size() {
                return centroids.length;
            }

            @Override
            public float[] centroid(int centroidOrdinal) {
                return centroids[centroidOrdinal];
            }

            @Override
            public KMeansResult secondLevelClusters() {
                return secondLevelClusters;
            }

            @Override
            public KMeansFloatVectorValues asKmeansFloatVectorValues() {
                return KMeansFloatVectorValues.build(Arrays.asList(centroids), null, dims);
            }
        };
    }
}
