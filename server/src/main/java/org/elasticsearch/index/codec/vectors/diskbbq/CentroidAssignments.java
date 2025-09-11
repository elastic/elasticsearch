/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

record CentroidAssignments(int numCentroids, float[][] centroids, int[] assignments, int[] overspillAssignments) {

    CentroidAssignments(float[][] centroids, int[] assignments, int[] overspillAssignments) {
        this(centroids.length, centroids, assignments, overspillAssignments);
        assert assignments.length == overspillAssignments.length || overspillAssignments.length == 0
            : "assignments and overspillAssignments must have the same length";
    }
}
