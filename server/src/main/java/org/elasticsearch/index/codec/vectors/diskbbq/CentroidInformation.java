/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

/**
 * Holds the result of centroid calculation: centroids and their assignments.
 *
 * @param <V> the array type for centroids ({@code float[]} or {@code byte[]})
 */
public record CentroidInformation<V>(V[] centroids, CentroidAssignments centroidAssignments) {

    @SuppressWarnings("unchecked")
    public CentroidInformation(int dims, float[][] centroids, int[] assignments, OverspillAssignments overspillAssignments) {
        this(
            (V[]) centroids,
            new CentroidAssignments(centroids.length, assignments, overspillAssignments, computeGlobalCentroidFromFloats(dims, centroids))
        );
    }

    @SuppressWarnings("unchecked")
    public CentroidInformation(
        int dims,
        float[][] centroids,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        CentroidSlices centroidSlices
    ) {
        this(
            (V[]) centroids,
            new CentroidAssignments(
                centroids.length,
                assignments,
                overspillAssignments,
                computeGlobalCentroidFromFloats(dims, centroids),
                centroidSlices
            )
        );
    }

    /**
     * Creates a CentroidInformation for byte-backed centroids.
     */
    public static CentroidInformation<byte[]> ofBytes(
        int dims,
        byte[][] centroids,
        int[] assignments,
        OverspillAssignments overspillAssignments
    ) {
        return new CentroidInformation<>(
            centroids,
            new CentroidAssignments(centroids.length, assignments, overspillAssignments, computeGlobalCentroidFromBytes(dims, centroids))
        );
    }

    /**
     * Creates a CentroidInformation for byte-backed centroids with slice information.
     */
    public static CentroidInformation<byte[]> ofBytes(
        int dims,
        byte[][] centroids,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        CentroidSlices centroidSlices
    ) {
        return new CentroidInformation<>(
            centroids,
            new CentroidAssignments(
                centroids.length,
                assignments,
                overspillAssignments,
                computeGlobalCentroidFromBytes(dims, centroids),
                centroidSlices
            )
        );
    }

    public int numCentroids() {
        return centroids.length;
    }

    public float[] globalCentroid() {
        return centroidAssignments.globalCentroid();
    }

    public int[] assignments() {
        return centroidAssignments.assignments();
    }

    public OverspillAssignments overspillAssignments() {
        return centroidAssignments.overspillAssignments();
    }

    public CentroidSlices centroidSlices() {
        return centroidAssignments.centroidSlices();
    }

    /**
     * Returns the centroids as float arrays. If the centroids are already {@code float[]},
     * they are returned directly. If they are {@code byte[]}, each is widened to float.
     */
    @SuppressWarnings("unchecked")
    public float[][] floatCentroids() {
        if (centroids.length == 0) {
            return new float[0][];
        }
        if (centroids[0] instanceof float[]) {
            return (float[][]) centroids;
        }
        // byte[] centroids: widen to float
        float[][] result = new float[centroids.length][];
        for (int i = 0; i < centroids.length; i++) {
            byte[] byteCentroid = (byte[]) centroids[i];
            float[] floatCentroid = new float[byteCentroid.length];
            for (int j = 0; j < byteCentroid.length; j++) {
                floatCentroid[j] = byteCentroid[j];
            }
            result[i] = floatCentroid;
        }
        return result;
    }

    private static float[] computeGlobalCentroidFromFloats(int dims, float[][] centroids) {
        final float[] globalCentroid = new float[dims];
        for (float[] centroid : centroids) {
            assert centroid.length == dims;
            for (int j = 0; j < centroid.length; j++) {
                globalCentroid[j] += centroid[j];
            }
        }
        for (int j = 0; j < globalCentroid.length; j++) {
            globalCentroid[j] /= centroids.length;
        }
        return globalCentroid;
    }

    private static float[] computeGlobalCentroidFromBytes(int dims, byte[][] centroids) {
        final float[] globalCentroid = new float[dims];
        for (byte[] centroid : centroids) {
            assert centroid.length == dims;
            for (int j = 0; j < centroid.length; j++) {
                globalCentroid[j] += centroid[j];
            }
        }
        for (int j = 0; j < globalCentroid.length; j++) {
            globalCentroid[j] /= centroids.length;
        }
        return globalCentroid;
    }
}
