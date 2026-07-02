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

    /**
     * Returns the byte centroid for the given ordinal, or null if centroids are not byte-backed.
     */
    default byte[] byteCentroid(int centroidOrdinal) throws IOException {
        return null;
    }

    default KMeansResult<float[]> secondLevelClusters() throws IOException {
        return null;
    }

    default CentroidSlices slices() throws IOException {
        return null;
    }

    KMeansFloatVectorValues asKmeansFloatVectorValues() throws IOException;

    static CentroidSupplier empty(int dims) {
        return fromArray(new float[0][dims], KMeansResult.emptyFloat(), dims);
    }

    static CentroidSupplier fromArray(float[][] centroids, KMeansResult<float[]> secondLevelClusters, int dims) {
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
            public KMeansResult<float[]> secondLevelClusters() {
                return secondLevelClusters;
            }

            @Override
            public KMeansFloatVectorValues asKmeansFloatVectorValues() {
                return KMeansFloatVectorValues.build(Arrays.asList(centroids), null, dims);
            }
        };
    }

    static CentroidSupplier fromByteArray(byte[][] byteCentroids, KMeansResult<float[]> secondLevelClusters, int dims) {
        return new CentroidSupplier() {
            // Single reusable scratch buffer for on-demand byte→float widening.
            // The returned float[] is only valid until the next call to centroid().
            private final float[] scratch = new float[dims];

            @Override
            public int size() {
                return byteCentroids.length;
            }

            @Override
            public float[] centroid(int centroidOrdinal) {
                byte[] src = byteCentroids[centroidOrdinal];
                for (int d = 0; d < dims; d++) {
                    scratch[d] = src[d];
                }
                return scratch;
            }

            @Override
            public byte[] byteCentroid(int centroidOrdinal) {
                return byteCentroids[centroidOrdinal];
            }

            @Override
            public KMeansResult<float[]> secondLevelClusters() {
                return secondLevelClusters;
            }

            @Override
            public KMeansFloatVectorValues asKmeansFloatVectorValues() {
                return KMeansFloatVectorValues.buildFromBytes(Arrays.asList(byteCentroids), null, dims, false);
            }
        };
    }

}
