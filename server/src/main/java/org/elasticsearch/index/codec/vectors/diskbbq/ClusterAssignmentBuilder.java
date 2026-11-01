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

/**
 * Builds the cluster-to-vector mapping from primary and overspill assignments.
 * Shared between the BBQ and ASH posting list writers.
 *
 * @param assignmentsByCluster per-cluster arrays of vector ordinals, indexed by centroid ordinal
 * @param maxPostingListSize   the size of the largest cluster
 */
public record ClusterAssignmentBuilder(int[][] assignmentsByCluster, int maxPostingListSize) {

    /**
     * Builds cluster assignments from primary assignments and overspill.
     *
     * @param assignments          primary centroid assignment per vector ordinal
     * @param overspillAssignments additional centroid assignments per vector ordinal (SOAR)
     * @param nClusters            total number of centroids
     * @return the cluster-to-vector mapping and max posting list size
     */
    public static ClusterAssignmentBuilder build(int[] assignments, OverspillAssignments overspillAssignments, int nClusters) {
        int nVectors = assignments.length;

        // Count vectors per cluster (primary + overspill)
        int[] centroidVectorCount = new int[nClusters];
        for (int i = 0; i < nVectors; i++) {
            centroidVectorCount[assignments[i]]++;
            for (var it = overspillAssignments.getAssignmentsFor(i); it.hasNext();) {
                centroidVectorCount[it.nextInt()]++;
            }
        }

        // Allocate per-cluster arrays
        int maxPostingListSize = 0;
        int[][] assignmentsByCluster = new int[nClusters][];
        for (int c = 0; c < nClusters; c++) {
            int size = centroidVectorCount[c];
            maxPostingListSize = Math.max(maxPostingListSize, size);
            assignmentsByCluster[c] = new int[size];
        }
        Arrays.fill(centroidVectorCount, 0);

        // Fill per-cluster arrays
        for (int i = 0; i < nVectors; i++) {
            int c = assignments[i];
            assignmentsByCluster[c][centroidVectorCount[c]++] = i;
            for (var it = overspillAssignments.getAssignmentsFor(i); it.hasNext();) {
                int s = it.nextInt();
                assignmentsByCluster[s][centroidVectorCount[s]++] = i;
            }
        }

        return new ClusterAssignmentBuilder(assignmentsByCluster, maxPostingListSize);
    }
}
