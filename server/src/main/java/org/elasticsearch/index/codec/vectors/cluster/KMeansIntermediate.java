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
 * Intermediate object for clustering (partitioning) a set of vectors.
 *
 * @param <V> the array type for centroids ({@code float[]} or {@code byte[]})
 */
class KMeansIntermediate<V> extends KMeansResult<V> {
    private final IntToIntFunction assignmentOrds;

    KMeansIntermediate(V[] centroids, int[] assignments, IntToIntFunction assignmentOrds) {
        super(centroids, assignments);
        assert assignmentOrds != null;
        this.assignmentOrds = assignmentOrds;
    }

    public static <V> KMeansIntermediate<V> empty(CentroidOps<V> ops) {
        return new KMeansIntermediate<>(ops.newCentroidArray(0, 0), new int[0], i -> i);
    }

    KMeansIntermediate(V[] centroids, int[] assignments) {
        this(centroids, assignments, i -> i);
    }

    public int ordToDoc(int ord) {
        return assignmentOrds.apply(ord);
    }
}
