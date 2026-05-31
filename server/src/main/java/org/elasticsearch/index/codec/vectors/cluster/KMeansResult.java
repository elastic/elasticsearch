/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;

import java.util.Arrays;
import java.util.List;

/**
 * Output object for clustering (partitioning) a set of vectors.
 *
 * @param <V> the array type for centroids ({@code float[]} or {@code byte[]})
 */
public class KMeansResult<V> {
    private V[] centroids;
    private final int[] assignments;
    private int[] clusterCounts;
    private int[] soarAssignments;

    private static final KMeansResult<float[]> FLOAT_EMPTY = new KMeansResult<>(new float[0][], new int[0], new int[0]) {
        @Override
        public float[] getCentroid(int vectorOrdinal) {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <V> KMeansResult<V> empty(CentroidOps<V> ops) {
        return (KMeansResult<V>) FLOAT_EMPTY;
    }

    /**
     * Returns the float-typed empty result for use in legacy diskbbq code.
     */
    public static KMeansResult<float[]> emptyFloat() {
        return FLOAT_EMPTY;
    }

    public static KMeansResult<float[]> singleCluster(float[] centroid, int numVectors) {
        return new KMeansResult<>(new float[][] { centroid }, new int[numVectors], new int[0]);
    }

    KMeansResult(V[] centroids, int[] assignments, int[] soarAssignments) {
        assert centroids != null;
        assert assignments != null;
        assert soarAssignments != null;
        this.centroids = centroids;
        this.assignments = assignments;
        this.soarAssignments = soarAssignments;
        clusterCounts = new int[centroids.length];
    }

    public V getCentroid(int vectorOrdinal) {
        if (centroids.length == 0) {
            return null;
        }
        return centroids[assignments[vectorOrdinal]];
    }

    public V[] centroids() {
        return centroids;
    }

    void setCentroids(V[] centroids, int[] clusterCounts) {
        this.centroids = centroids;
        this.clusterCounts = clusterCounts;
    }

    @SuppressWarnings("unchecked")
    public CentroidSupplier centroidsSupplier() {
        // TODO: update this in a subsequent PR to support byte[] as well
        float[][] floatCentroids = (float[][]) centroids;
        int dims = floatCentroids.length > 0 ? floatCentroids[0].length : 0;
        return CentroidSupplier.fromArray(floatCentroids, FLOAT_EMPTY, dims);
    }

    public int[] assignments() {
        return assignments;
    }

    public int[] clusterCounts() {
        return clusterCounts;
    }

    void setSoarAssignments(int[] soarAssignments) {
        this.soarAssignments = soarAssignments;
    }

    public int[] soarAssignments() {
        return soarAssignments;
    }

    /**
     * Merge multiple clustering results into a single result by concatenating centroids
     * in the provided order and reindexing assignments to the merged centroid layout.
     * Soar assignments are offset the same way; if a result has no soar assignments,
     * the merged result uses {@code -1} for those positions.
     */
    public static <V> KMeansResult<V> merge(List<KMeansResult<V>> results, CentroidOps<V> ops) {
        int numCentroids = 0;
        int numAssignments = 0;
        for (KMeansResult<V> result : results) {
            numCentroids += result.centroids().length;
            numAssignments += result.assignments().length;
        }
        V[] centroids = ops.newCentroidArrayShallow(numCentroids);
        int[] assignments = new int[numAssignments];
        int[] spillAssignments = new int[numAssignments];
        int centroidOffset = 0;
        int assignmentOffset = 0;
        for (KMeansResult<V> result : results) {
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
        return new KMeansResult<>(centroids, assignments, spillAssignments);
    }
}
