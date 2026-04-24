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

public record CentroidAssignments(
    int numCentroids,
    float[][] centroids,
    int[] assignments,
    int[] overspillAssignments,
    float[] globalCentroid,
    CentroidSlices centroidSlices
) {

    public CentroidAssignments(int dims, float[][] centroids, int[] assignments, int[] overspillAssignments) {
        this(centroids.length, centroids, assignments, overspillAssignments, computeGlobalCentroid(dims, centroids), null);
        assert assignments.length == overspillAssignments.length || overspillAssignments.length == 0
            : "assignments and overspillAssignments must have the same length";

    }

    public CentroidAssignments(
        int dims,
        float[][] centroids,
        int[] assignments,
        int[] overspillAssignments,
        CentroidSlices centroidSlices
    ) {
        this(centroids.length, centroids, assignments, overspillAssignments, computeGlobalCentroid(dims, centroids), centroidSlices);
        assert assignments.length == overspillAssignments.length || overspillAssignments.length == 0
            : "assignments and overspillAssignments must have the same length";
        assert centroidSlices == null || Arrays.stream(centroidSlices.sliceNumVectors()).sum() == assignments.length;
        assert centroidSlices == null || CentroidSlices.assertSliceOffsets(centroidSlices.sliceOffsets(), numCentroids);
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
