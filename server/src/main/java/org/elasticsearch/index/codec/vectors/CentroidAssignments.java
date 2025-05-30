/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

record CentroidAssignments(int numCentroids, float[][] cachedCentroids, short[] assignments, short[] soarAssignments) {

    CentroidAssignments(float[][] centroids, short[] assignments, short[] soarAssignments) {
        this(centroids.length, centroids, assignments, soarAssignments);
    }

    CentroidAssignments(int numCentroids, short[] assignments, short[] soarAssignments) {
        this(numCentroids, null, assignments, soarAssignments);
    }
}
