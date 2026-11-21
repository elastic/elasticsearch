/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import java.util.function.Supplier;

/**
 * Transformations of a query vector computed by one segment's reader and reused by the other segments of the same query.
 * <p>
 * Some IVF formats transform the query before scoring a segment, such as ASH projecting it through the segment's
 * projection matrix. When segments share what the transformation depends on, computing it once per query rather than once
 * per segment saves its cost on every other segment. The format decides what is cached; the query decides how long: it
 * creates one {@link #perQuery()} instance, like {@link SharedMinCompetitiveScore}, and hands it only to the searches that
 * use the query's own vector: those of the query's leaves, which may run concurrently, and of the named slices a
 * {@link IVFKnnFloatSlicedVectorQuery} searches one after another within a leaf. Every other search gets {@link #NONE}.
 * <p>
 * An instance holds the transformations of a single vector, so keys need to capture everything else a transformation
 * depends on, but not the vector.
 */
public interface QueryTransformCache {

    /** Caches nothing: every transformation is computed by the search that needs it. */
    QueryTransformCache NONE = new QueryTransformCache() {
        @Override
        public <V> V getSharedQuery(Object key, Supplier<V> supplier) {
            return supplier.get();
        }
    };

    /**
     * Returns the transformed query for {@code key}, computing it with {@code supplier} unless it is cached and another
     * search of the query already has. Keys must implement equals and hashCode, and each kind of key must be used with a
     * single type of value. Suppliers for the same key must compute the same value, which must not be modified.
     */
    <V> V getSharedQuery(Object key, Supplier<V> supplier);

    /** Returns a new instance that computes each transformation once, to share across the searches of one query. */
    static QueryTransformCache perQuery() {
        return new PerQueryTransformCache();
    }
}
