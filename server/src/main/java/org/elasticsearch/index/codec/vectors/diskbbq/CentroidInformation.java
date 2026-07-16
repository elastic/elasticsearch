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
 * Holds the centroids and centroid assignments for a field.
 * <p>
 * The centroids are held separately so they can be pushed to off-heap whilst keeping the centroid assignments in memory.
 * @param centroids Centroids.
 * @param centroidAssignments Centroid assignments.
 */
public record CentroidInformation(float[][] centroids, CentroidAssignments centroidAssignments) {

    public CentroidInformation(int dims, float[][] centroids, int[] assignments, OverspillAssignments overspillAssignments) {
        this(
            centroids,
            new CentroidAssignments(centroids.length, assignments, overspillAssignments, computeGlobalCentroid(dims, centroids))
        );
    }

    public CentroidInformation(
        int dims,
        float[][] centroids,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        CentroidSlices centroidSlices
    ) {
        this(
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
}
