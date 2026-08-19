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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
     * Returns the byte centroid for the given ordinal, or null if centroids are not byte-backed.
     */
    default byte[] byteCentroid(int centroidOrdinal) throws IOException {
        return null;
    }

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

    /**
     * Creates a float-backed centroid supplier.
     *
     * @param centroids     the float centroid arrays
     * @param centroidIndex the centroid index structure
     * @param dims          the vector dimension
     */
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

    /**
     * Creates a byte-backed centroid supplier. The {@link #centroid(int)} method widens byte
     * centroids to float on demand using a reusable scratch buffer. The {@link #byteCentroid(int)}
     * method returns the raw byte centroid.
     *
     * @param byteCentroids the byte centroid arrays
     * @param centroidIndex   the centroid index structure
     * @param dims          the vector dimension
     */
    static CentroidSupplier fromByteArray(byte[][] byteCentroids, CentroidIndex centroidIndex, int dims) {
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
            public CentroidIndex centroidIndex() {
                return centroidIndex;
            }

            @Override
            public KMeansFloatVectorValues asKmeansFloatVectorValues() {
                List<float[]> floatCentroids = new ArrayList<>(byteCentroids.length);
                for (byte[] bc : byteCentroids) {
                    float[] fc = new float[dims];
                    for (int d = 0; d < dims; d++) {
                        fc[d] = bc[d];
                    }
                    floatCentroids.add(fc);
                }
                return KMeansFloatVectorValues.build(floatCentroids, null, dims);
            }
        };
    }

}
