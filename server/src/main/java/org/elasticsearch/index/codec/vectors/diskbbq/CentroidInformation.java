/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;

/**
 * Holds the centroids and centroid assignments for a field.
 * <p>
 * The centroids are held separately so they can be pushed to off-heap whilst keeping the centroid assignments in memory.
 * <p>
 * The type parameter {@code V} is the centroid vector type: {@code float[]} for float fields,
 * {@code byte[]} for byte fields with native byte clustering.
 * The global centroid is always {@code float[]} regardless of {@code V} because it is the
 * arithmetic mean of centroids, which is not representable as {@code byte[]}.
 *
 * @param centroids Centroids.
 * @param centroidAssignments Centroid assignments.
 */
public record CentroidInformation<V>(V[] centroids, CentroidAssignments centroidAssignments) {

    /**
     * Creates a {@code CentroidInformation<byte[]>} from byte centroids.
     * The global centroid is computed as the float mean of the byte centroids.
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
     * Creates a {@code CentroidInformation<float[]>} from float centroids.
     */
    public static CentroidInformation<float[]> ofFloat(
        int dims,
        float[][] centroids,
        int[] assignments,
        OverspillAssignments overspillAssignments
    ) {
        return new CentroidInformation<>(
            centroids,
            new CentroidAssignments(centroids.length, assignments, overspillAssignments, computeGlobalCentroid(dims, centroids))
        );
    }

    /**
     * Creates a {@code CentroidInformation<float[]>} from float centroids with centroid slices.
     */
    public static CentroidInformation<float[]> ofFloat(
        int dims,
        float[][] centroids,
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
                computeGlobalCentroid(dims, centroids),
                centroidSlices
            )
        );
    }

    /**
     * Creates a {@code CentroidInformation<V>} from centroids of any type, using the provided
     * {@link CentroidOps} to compute the float global centroid.
     */
    public static <V> CentroidInformation<V> of(
        int dims,
        V[] centroids,
        int[] assignments,
        OverspillAssignments overspill,
        CentroidSlices centroidSlices,
        CentroidOps<V> ops
    ) {
        float[] globalCentroid = ops.computeFloatGlobalCentroid(centroids, dims);
        if (centroidSlices != null) {
            return new CentroidInformation<>(
                centroids,
                new CentroidAssignments(centroids.length, assignments, overspill, globalCentroid, centroidSlices)
            );
        }
        return new CentroidInformation<>(centroids, new CentroidAssignments(centroids.length, assignments, overspill, globalCentroid));
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

    private static float[] computeGlobalCentroid(int dims, float[][] centroids) {
        final float[] globalCentroid = new float[dims];
        // TODO: push this logic into vector util?
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
            for (int j = 0; j < dims; j++) {
                globalCentroid[j] += centroid[j];
            }
        }
        for (int j = 0; j < dims; j++) {
            globalCentroid[j] /= centroids.length;
        }
        return globalCentroid;
    }
}
