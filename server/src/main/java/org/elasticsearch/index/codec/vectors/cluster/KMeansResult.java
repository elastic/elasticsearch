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

/**
 * Output object for clustering (partitioning) a set of vectors
 */
public class KMeansResult {
    private float[][] centroids;
    private final int[] assignments;
    private int[] soarAssignments;
    public static KMeansResult EMPTY = new KMeansResult(new float[0][], new int[0], new int[0]) {
        @Override
        public float[] getCentroid(int vectorOrdinal) {
            return null;
        }

        @Override
        public CentroidSupplier centroidsSupplier() {
            return CentroidSupplier.empty(0);
        }

        @Override
        public int[] assignments() {
            return new int[0];
        }

        @Override
        public int[] soarAssignments() {
            return new int[0];
        }
    };

    public static KMeansResult singleCluster(float[] centroid, int numVectors) {
        return new KMeansResult(new float[][] { centroid }, new int[numVectors], new int[0]);
    }

    KMeansResult(float[][] centroids, int[] assignments, int[] soarAssignments) {
        assert centroids != null;
        assert assignments != null;
        assert soarAssignments != null;
        this.centroids = centroids;
        this.assignments = assignments;
        this.soarAssignments = soarAssignments;
    }

    public float[] getCentroid(int vectorOrdinal) {
        return centroids[assignments[vectorOrdinal]];
    }

    public float[][] centroids() {
        return centroids;
    }

    public CentroidSupplier centroidsSupplier() {
        return CentroidSupplier.fromArray(centroids, EMPTY, centroids[0].length);
    }

    void setCentroids(float[][] centroids) {
        this.centroids = centroids;
    }

    public int[] assignments() {
        return assignments;
    }

    void setSoarAssignments(int[] soarAssignments) {
        this.soarAssignments = soarAssignments;
    }

    public int[] soarAssignments() {
        return soarAssignments;
    }
}
