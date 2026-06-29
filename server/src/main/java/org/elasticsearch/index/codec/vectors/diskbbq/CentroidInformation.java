/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import static org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter.computeGlobalCentroid;

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

    public CentroidSlices centroidSlices() {
        return centroidAssignments.centroidSlices();
    }
}
