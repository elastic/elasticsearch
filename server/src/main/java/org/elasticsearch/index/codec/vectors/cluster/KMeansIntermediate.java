/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.hnsw.IntToIntFunction;

/**
 * Intermediate object for clustering (partitioning) a set of vectors
 */
class KMeansIntermediate extends KMeansResult {
    private final IntToIntFunction assignmentOrds;

    KMeansIntermediate(float[][] centroids, int[] assignments, IntToIntFunction assignmentOrds, int[] soarAssignments) {
        super(centroids, assignments, soarAssignments);
        assert assignmentOrds != null;
        this.assignmentOrds = assignmentOrds;
    }

    KMeansIntermediate(float[][] centroids, int[] assignments, IntToIntFunction assignmentOrdinals) {
        this(centroids, assignments, assignmentOrdinals, new int[0]);
    }

    KMeansIntermediate() {
        this(new float[0][0], new int[0], i -> i, new int[0]);
    }

    KMeansIntermediate(float[][] centroids) {
        this(centroids, new int[0], i -> i, new int[0]);
    }

    KMeansIntermediate(float[][] centroids, int[] assignments) {
        this(centroids, assignments, i -> i, new int[0]);
    }

    public int ordToDoc(int ord) {
        return assignmentOrds.apply(ord);
    }
}
