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

import java.io.IOException;
import java.util.Arrays;

/**
 * An interface for accessing centroids
 */
public interface CentroidSupplier {

    /**
     * Number of centroids
     */
    int size();

    /**
     * Gets a specific centroid by ordinal
     */
    float[] centroid(int centroidOrdinal) throws IOException;

    /**
     * Any indexing information that may be available
     */
    CentroidIndex centroidIndex();

    /**
     * Slices information
     */
    default CentroidSlices slices() throws IOException {
        return null;
    }

    /**
     * Accesses the centroids as a {@link KMeansFloatVectorValues}
     */
    KMeansFloatVectorValues asKmeansFloatVectorValues() throws IOException;

    static CentroidSupplier empty(int dims) {
        return fromArray(new float[0][dims], CentroidIndex.NO_INDEX, dims);
    }

    static CentroidSupplier fromArray(float[][] centroids, CentroidIndex centroidIndex, int dims) {
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
            public CentroidIndex centroidIndex() {
                return centroidIndex;
            }

            @Override
            public KMeansFloatVectorValues asKmeansFloatVectorValues() {
                return KMeansFloatVectorValues.build(Arrays.asList(centroids), null, dims);
            }
        };
    }

}
