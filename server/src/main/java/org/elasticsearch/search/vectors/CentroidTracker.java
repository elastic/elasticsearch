/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.SparseFixedBitSet;

import java.util.Objects;

/**
 * Tracks which centroids have been visited during IVF vector search.
 *
 * <p>Used in post-filtering mode to avoid re-visiting the same centroids
 * across recursive search passes when trying to collect k results.
 */
public final class CentroidTracker {
    private final BitSet visited;

    public CentroidTracker(int maxDoc) {
        this.visited = new SparseFixedBitSet(maxDoc);
    }

    /**
     * Marks a centroid as visited.
     *
     * @param ord the centroid ordinal
     */
    public void markVisited(int ord) {
        visited.set(ord);
    }

    /**
     * Checks if a centroid has already been visited.
     *
     * @param ord the centroid ordinal
     * @return true if the centroid was previously visited
     */
    public boolean alreadyVisited(int ord) {
        return visited.get(ord);
    }

    /**
     * Returns an iterator over visited centroid ordinals.
     *
     * @return iterator over visited centroids, or null if none visited
     */
    public DocIdSetIterator iterator() {
        return visited.cardinality() > 0 ? new BitSetIterator(visited, 0) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CentroidTracker that = (CentroidTracker) o;
        return Objects.equals(visited, that.visited);
    }

    @Override
    public int hashCode() {
        return Objects.hash(visited);
    }
}
