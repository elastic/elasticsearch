/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import java.util.function.IntUnaryOperator;

/**
 * Intermediate object for clustering (partitioning) a set of vectors.
 *
 * @param <V> the array type for centroids ({@code float[]} or {@code byte[]})
 */
class KMeansIntermediate<V> extends KMeansResult<V> {
    private final IntUnaryOperator assignmentOrds;

    KMeansIntermediate(V[] centroids, int[] assignments, IntUnaryOperator assignmentOrds) {
        super(centroids, assignments);
        assert assignmentOrds != null;
        this.assignmentOrds = assignmentOrds;
    }

    public static <V> KMeansIntermediate<V> empty(CentroidOps<V> ops) {
        return new KMeansIntermediate<>(ops.newCentroidArray(0, 0), new int[0], IntUnaryOperator.identity());
    }

    public int ordToDoc(int ord) {
        return assignmentOrds.applyAsInt(ord);
    }
}
