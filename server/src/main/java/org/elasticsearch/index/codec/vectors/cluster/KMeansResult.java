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
public class KMeansResult implements Clusters {
    private float[][] centroids;
    private final int[] assignments;
    private int[] soarAssignments;

    KMeansResult(float[][] centroids, int[] assignments, int[] soarAssignments) {
        assert centroids != null;
        assert assignments != null;
        assert soarAssignments != null;
        this.centroids = centroids;
        this.assignments = assignments;
        this.soarAssignments = soarAssignments;
    }

    @Override
    public float[] getCentroid(int vectorOrdinal) {
        return centroids[assignments[vectorOrdinal]];
    }

    public float[][] centroids() {
        return centroids;
    }

    @Override
    public CentroidSupplier centroidsSupplier() {
        return CentroidSupplier.fromArray(centroids, Clusters.EMPTY, centroids[0].length);
    }

    void setCentroids(float[][] centroids) {
        this.centroids = centroids;
    }

    @Override
    public int[] assignments() {
        return assignments;
    }

    void setSoarAssignments(int[] soarAssignments) {
        this.soarAssignments = soarAssignments;
    }

    @Override
    public int[] secondaryAssignments() {
        return soarAssignments;
    }
}
