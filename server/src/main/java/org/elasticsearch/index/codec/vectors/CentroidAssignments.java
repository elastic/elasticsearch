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

    private final int numParentCentroids;
    private final int numCentroids;
    private final float[][] cachedCentroids;
    private final int[][] assignmentsByCluster;

    private CentroidAssignments(int numParentCentroids, int numCentroids, float[][] cachedCentroids, int[][] assignmentsByCluster) {
        this.numParentCentroids = numParentCentroids;
        this.numCentroids = numCentroids;
        this.cachedCentroids = cachedCentroids;
        this.assignmentsByCluster = assignmentsByCluster;
    }

    CentroidAssignments(int numParentCentroids, float[][] centroids, int[][] assignmentsByCluster) {
        this(numParentCentroids, centroids.length, centroids, assignmentsByCluster);
    }

    CentroidAssignments(int numParentCentroids, int numCentroids, int[][] assignmentsByCluster) {
        this(numParentCentroids, numCentroids, null, assignmentsByCluster);
    }

    public int numCentroids() {
        return numCentroids;
    }

    public int numParentCentroids() {
        return numParentCentroids;
    }

    public float[][] cachedCentroids() {
        return cachedCentroids;
    }

    public int[][] assignmentsByCluster() {
        return assignmentsByCluster;
    }
}
