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
    private final int[][] assignmentsByCluster;

    private CentroidAssignments(int numCentroids, float[][] cachedCentroids, int[][] assignmentsByCluster) {
        this.numCentroids = numCentroids;
        this.cachedCentroids = cachedCentroids;
        this.assignmentsByCluster = assignmentsByCluster;
    }

    CentroidAssignments(float[][] centroids, int[][] assignmentsByCluster) {
        this(centroids.length, centroids, assignmentsByCluster);
    }

    CentroidAssignments(int numCentroids, int[][] assignmentsByCluster) {
        this(numCentroids, null, assignmentsByCluster);
    }

    // Getters and setters
    public int numCentroids() {
        return numCentroids;
    }

    public float[][] cachedCentroids() {
        return cachedCentroids;
    }

    public int[][] assignmentsByCluster() {
        return assignmentsByCluster;
    }
}
