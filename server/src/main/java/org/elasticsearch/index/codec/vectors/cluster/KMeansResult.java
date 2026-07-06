/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

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

    private static final KMeansResult<float[]> FLOAT_EMPTY = new KMeansResult<>(new float[0][], new int[0]) {
        @Override
        public float[] getCentroid(int vectorOrdinal) {
            return null;
        }
    };
    private static final KMeansResult<byte[]> BYTE_EMPTY = new KMeansResult<>(new byte[0][], new int[0]) {
        @Override
        public byte[] getCentroid(int vectorOrdinal) {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <V> KMeansResult<V> empty(CentroidOps<V> ops) {
        if (ops instanceof CentroidOps.FloatOps) {
            return (KMeansResult<V>) FLOAT_EMPTY;
        } else {
            return (KMeansResult<V>) BYTE_EMPTY;
        }
    }

    /**
     * Returns the float-typed empty result for use in legacy diskbbq code.
     */
    public static KMeansResult<float[]> emptyFloat() {
        return FLOAT_EMPTY;
    }

    public static KMeansResult<float[]> singleCluster(float[] centroid, int numVectors) {
        return new KMeansResult<>(new float[][] { centroid }, new int[numVectors]);
    }

    KMeansResult(V[] centroids, int[] assignments) {
        assert centroids != null;
        assert assignments != null;
        this.centroids = centroids;
        this.assignments = assignments;
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

    public int[] assignments() {
        return assignments;
    }

    public int[] clusterCounts() {
        return clusterCounts;
    }

    /**
     * Merge multiple clustering results into a single result by concatenating centroids
     * in the provided order and reindexing assignments to the merged centroid layout.
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
        int centroidOffset = 0;
        int assignmentOffset = 0;
        for (KMeansResult<V> result : results) {
            V[] resultCentroids = result.centroids();
            int[] resultAssignments = result.assignments();
            ops.arrayCopy(resultCentroids, 0, centroids, centroidOffset, resultCentroids.length);
            for (int i = 0; i < resultAssignments.length; i++) {
                assignments[assignmentOffset + i] = resultAssignments[i] + centroidOffset;
            }
            centroidOffset += resultCentroids.length;
            assignmentOffset += resultAssignments.length;
        }
        return new KMeansResult<>(centroids, assignments);
    }
}
