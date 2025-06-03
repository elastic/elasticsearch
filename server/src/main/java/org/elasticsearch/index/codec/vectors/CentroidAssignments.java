/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

final class CentroidAssignments {

    private final int numCentroids;
    private final float[][] cachedCentroids;
    private final int[] assignments;
    private final int[] soarAssignments;

    private CentroidAssignments(int numCentroids, float[][] cachedCentroids, int[] assignments, int[] soarAssignments) {
        this.numCentroids = numCentroids;
        this.cachedCentroids = cachedCentroids;
        this.assignments = assignments;
        this.soarAssignments = soarAssignments;
    }

    CentroidAssignments(float[][] centroids, int[] assignments, int[] soarAssignments) {
        this(centroids.length, centroids, assignments, soarAssignments);
    }

    CentroidAssignments(int numCentroids, int[] assignments, int[] soarAssignments) {
        this(numCentroids, null, assignments, soarAssignments);
    }

    // Getters and setters
    public int numCentroids() {
        return numCentroids;
    }

    public float[][] cachedCentroids() {
        return cachedCentroids;
    }

    public int[] assignments() {
        return assignments;
    }

    public int[] soarAssignments() {
        return soarAssignments;
    }
}
