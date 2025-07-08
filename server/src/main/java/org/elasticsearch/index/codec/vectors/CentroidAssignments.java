/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

record CentroidAssignments(int numCentroids, float[][] centroids, int[][] assignmentsByCluster) {

    CentroidAssignments(float[][] centroids, int[][] assignmentsByCluster) {
        this(centroids.length, centroids, assignmentsByCluster);
        assert centroids.length == assignmentsByCluster.length;
    }
}
