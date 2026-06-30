/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.SoarAssignments;

import java.util.Arrays;
import java.util.List;

public record KMeansWithOverspill<V>(KMeansResult<V> result, @Nullable OverspillAssignments overspill) {

    public V[] centroids() {
        return result.centroids();
    }

    public int[] assignments() {
        return result.assignments();
    }

    public int[] soarAssignments() {
        return overspill != null ? ((SoarAssignments) overspill).assignments() : new int[0];
    }

    /**
     * Merge multiple clustering results into a single result by concatenating centroids
     * in the provided order and reindexing assignments to the merged centroid layout.
     * Soar assignments are offset the same way; if a result has no soar assignments,
     * the merged result uses {@code -1} for those positions.
     */
    public static <V> KMeansWithOverspill<V> merge(List<KMeansWithOverspill<V>> results, CentroidOps<V> ops) {
        int numCentroids = 0;
        int numAssignments = 0;
        for (KMeansWithOverspill<V> result : results) {
            numCentroids += result.centroids().length;
            numAssignments += result.assignments().length;
        }
        V[] centroids = ops.newCentroidArrayShallow(numCentroids);
        int[] assignments = new int[numAssignments];
        int[] spillAssignments = new int[numAssignments];
        int centroidOffset = 0;
        int assignmentOffset = 0;
        for (KMeansWithOverspill<V> result : results) {
            V[] resultCentroids = result.centroids();
            int[] resultAssignments = result.assignments();
            int[] resultSoarAssignments = result.soarAssignments();
            ops.arrayCopy(resultCentroids, 0, centroids, centroidOffset, resultCentroids.length);
            for (int i = 0; i < resultAssignments.length; i++) {
                assignments[assignmentOffset + i] = resultAssignments[i] + centroidOffset;
            }
            if (resultSoarAssignments.length > 0) {
                for (int i = 0; i < resultAssignments.length; i++) {
                    int soarAssignment = resultSoarAssignments[i];
                    spillAssignments[assignmentOffset + i] = soarAssignment == -1 ? -1 : soarAssignment + centroidOffset;
                }
            } else {
                Arrays.fill(spillAssignments, assignmentOffset, assignmentOffset + resultAssignments.length, -1);
            }
            centroidOffset += resultCentroids.length;
            assignmentOffset += resultAssignments.length;
        }
        return new KMeansWithOverspill<>(new KMeansResult<>(centroids, assignments), new SoarAssignments(spillAssignments));
    }
}
