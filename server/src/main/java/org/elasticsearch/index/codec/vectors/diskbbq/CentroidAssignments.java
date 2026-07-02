/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import java.util.Arrays;

/**
 * Holds centroid data produced by clustering. The type parameter {@code V} is either
 * {@code float[]} for float-backed vectors or {@code byte[]} for byte-backed vectors.
 * The {@link #globalCentroid} is always stored as {@code float[]} regardless of vector type.
 */
public record CentroidAssignments<V>(
    int numCentroids,
    V[] centroids,
    int[] assignments,
    OverspillAssignments overspillAssignments,
    float[] globalCentroid,
    CentroidSlices centroidSlices
) {

    @SuppressWarnings("unchecked")
    public CentroidAssignments(int dims, float[][] centroids, int[] assignments, OverspillAssignments overspillAssignments) {
        this(centroids.length, (V[]) centroids, assignments, overspillAssignments, computeGlobalCentroidFromFloats(dims, centroids), null);
        assert assignments.length == overspillAssignments.size() || overspillAssignments.size() == 0
            : "assignments and overspillAssignments must have the same length";
    }

    @SuppressWarnings("unchecked")
    public CentroidAssignments(
        int dims,
        float[][] centroids,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        CentroidSlices centroidSlices
    ) {
        this(
            centroids.length,
            (V[]) centroids,
            assignments,
            overspillAssignments,
            computeGlobalCentroidFromFloats(dims, centroids),
            centroidSlices
        );
        assert assignments.length == overspillAssignments.size() || overspillAssignments.size() == 0
            : "assignments and overspillAssignments must have the same length";
        assert centroidSlices == null || Arrays.stream(centroidSlices.sliceNumVectors()).sum() == assignments.length;
        assert centroidSlices == null || CentroidSlices.assertSliceOffsets(centroidSlices.sliceOffsets(), centroids.length);
    }

    public static CentroidAssignments<byte[]> ofBytes(int dims, byte[][] centroids, int[] assignments, int[] overspillAssignments) {
        return new CentroidAssignments<>(
            centroids.length,
            centroids,
            assignments,
            new SoarAssignments(overspillAssignments),
            computeGlobalCentroidFromBytes(dims, centroids),
            null
        );
    }

    public static CentroidAssignments<byte[]> ofBytes(
        int dims,
        byte[][] centroids,
        int[] assignments,
        int[] overspillAssignments,
        CentroidSlices centroidSlices
    ) {
        float[] globalCentroid = computeGlobalCentroidFromBytes(dims, centroids);
        assert assignments.length == overspillAssignments.length || overspillAssignments.length == 0
            : "assignments and overspillAssignments must have the same length";
        assert centroidSlices == null || Arrays.stream(centroidSlices.sliceNumVectors()).sum() == assignments.length;
        assert centroidSlices == null || CentroidSlices.assertSliceOffsets(centroidSlices.sliceOffsets(), centroids.length);
        return new CentroidAssignments<>(
            centroids.length,
            centroids,
            assignments,
            new SoarAssignments(overspillAssignments),
            globalCentroid,
            centroidSlices
        );
    }

    /**
     * Returns the centroids widened to {@code float[][]}. If the centroids are already float-backed,
     * this is an identity operation. For byte-backed centroids, each byte value is widened to float.
     */
    @SuppressWarnings("unchecked")
    public float[][] floatCentroids() {
        if (centroids.length == 0) {
            return new float[0][];
        }
        if (centroids[0] instanceof float[]) {
            return (float[][]) centroids;
        }
        // byte[][] -> float[][]
        byte[][] byteCentroids = (byte[][]) centroids;
        float[][] result = new float[byteCentroids.length][];
        for (int i = 0; i < byteCentroids.length; i++) {
            result[i] = new float[byteCentroids[i].length];
            for (int j = 0; j < byteCentroids[i].length; j++) {
                result[i][j] = byteCentroids[i][j];
            }
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
