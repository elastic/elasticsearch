/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

public record CentroidAssignments(
    int numCentroids,
    float[][] centroids,
    int[] assignments,
    int[] overspillAssignments,
    float[] globalCentroid
) {

    public CentroidAssignments(int dims, float[][] centroids, int[] assignments, int[] overspillAssignments) {
        this(centroids.length, centroids, assignments, overspillAssignments, computeWeightedGlobalCentroid(dims, centroids, assignments));
        assert assignments.length == overspillAssignments.length || overspillAssignments.length == 0
            : "assignments and overspillAssignments must have the same length";

    }

    private static float[] computeWeightedGlobalCentroid(int dims, float[][] centroids, int[] assignments) {
        // compute cluster sizes from assignments
        int[] clusterSizes = new int[centroids.length];
        for (int assignment : assignments) {
            clusterSizes[assignment]++;
        }

        final float[] globalCentroid = new float[dims];
        long totalVectors = 0;

        // weight each centroid by its cluster size
        for (int i = 0; i < centroids.length; i++) {
            float[] centroid = centroids[i];
            assert centroid.length == dims;
            int weight = clusterSizes[i];
            totalVectors += weight;

            for (int j = 0; j < centroid.length; j++) {
                globalCentroid[j] += centroid[j] * weight;
            }
        }

        // normalize by total number of vectors
        if (totalVectors > 0) {
            for (int j = 0; j < globalCentroid.length; j++) {
                globalCentroid[j] /= totalVectors;
            }
        }

        return globalCentroid;
    }
}
