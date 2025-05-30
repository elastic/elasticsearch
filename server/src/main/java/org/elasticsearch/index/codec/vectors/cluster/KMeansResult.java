/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

/**
 * Output object for clustering (partitioning) a set of vectors
 */
public class KMeansResult {
    private float[][] centroids;
    private final short[] assignments;
    private final int[] assignmentOrds;
    private short[] soarAssignments;

    KMeansResult(float[][] centroids, short[] assignments, int[] assignmentOrds, short[] soarAssignments) {
        assert centroids != null;
        assert assignments != null;
        assert assignmentOrds != null;
        assert soarAssignments != null;
        this.centroids = centroids;
        this.assignments = assignments;
        this.assignmentOrds = assignmentOrds;
        this.soarAssignments = soarAssignments;
    }

    KMeansResult(float[][] centroids, short[] assignments, int[] assignmentOrdinals) {
        this(centroids, assignments, assignmentOrdinals, new short[0]);
    }

    KMeansResult() {
        this(new float[0][0], new short[0], new int[0], new short[0]);
    }

    KMeansResult(float[][] centroids) {
        this(centroids, new short[0], new int[0], new short[0]);
    }

    KMeansResult(float[][] centroids, short[] assignments) {
        this(centroids, assignments, new int[0], new short[0]);
    }

    public float[][] centroids() {
        return centroids;
    }

    public void setCentroids(float[][] centroids) {
        this.centroids = centroids;
    }

    public short[] assignments() {
        return assignments;
    }

    public int[] assignmentOrds() {
        return assignmentOrds;
    }

    public short[] soarAssignments() {
        return soarAssignments;
    }

    public void setSoarAssignments(short[] soarAssignments) {
        this.soarAssignments = soarAssignments;
    }

}
