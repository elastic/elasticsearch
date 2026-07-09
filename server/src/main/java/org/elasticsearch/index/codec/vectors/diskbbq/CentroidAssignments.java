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
    int[] assignments,
    OverspillAssignments overspillAssignments,
    float[] globalCentroid,
    CentroidSlices centroidSlices
) {

    public CentroidAssignments(int numCentroids, int[] assignments, OverspillAssignments overspillAssignments, float[] globalCentroid) {
        this(numCentroids, assignments, overspillAssignments, globalCentroid, null);
    }

    public CentroidAssignments {
        assert assignments.length == overspillAssignments.size() || overspillAssignments.size() == 0
            : "assignments and overspillAssignments must have the same length";
        assert centroidSlices == null || Arrays.stream(centroidSlices.sliceNumVectors()).sum() == assignments.length;
        assert centroidSlices == null || CentroidSlices.assertSliceOffsets(centroidSlices.sliceOffsets(), numCentroids);
    }
}
