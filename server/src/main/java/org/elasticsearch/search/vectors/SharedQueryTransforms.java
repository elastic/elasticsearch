/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.Assertions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Transformations of a query vector computed by one segment's reader and reused by the other segments of the same query.
 * <p>
 * Some IVF formats transform the query before scoring a segment, such as ASH projecting it through the segment's
 * projection matrix. When segments share what the transformation depends on, computing it once per query rather than once
 * per segment saves its cost on every other segment. The format decides what is cached; the query decides how long: one
 * instance is created per query, like {@link SharedMinCompetitiveScore}.
 * <p>
 * The instance is handed only to the searches that use the query's own vector: those of the query's leaves, which may run
 * concurrently, and of the named slices a {@link IVFKnnFloatSlicedVectorQuery} searches one after another within a leaf.
 * It holds the transformations of that single vector, so keys need to capture everything else the transformation depends
 * on, but not the vector. Handing it to a search with another vector, such as one preconditioned for its segment, would
 * give that search the transformations of the wrong vector; assertions check that this does not happen.
 * <p>
 * A transformation is computed at most once per key, outside the map's lock: leaves that need it while it is being computed
 * wait for it, while leaves that need other keys do not. Only the first caller's supplier is kept for a key, which is also
 * the one retried if it throws, so all suppliers for a key must compute the same value. Values must not be modified once
 * computed.
 */
public final class SharedQueryTransforms {

    /** Key of a transformation whose value has type {@code V}. Implementations must implement equals and hashCode. */
    public interface Key<V> {}

    private final ConcurrentHashMap<Key<?>, CachedSupplier<?>> transforms = new ConcurrentHashMap<>();
    // The vector the transformations are of, only tracked when assertions are enabled
    private final AtomicReference<float[]> query = Assertions.ENABLED ? new AtomicReference<>() : null;

    /**
     * Returns the transformation of {@code query} for {@code key}, computing it with {@code supplier} if no leaf of this
     * query has yet.
     *
     * @param query the vector to transform, which must be the same instance for every call
     */
    public <V> V computeIfAbsent(float[] query, Key<V> key, Supplier<V> supplier) {
        assert isTheQuery(query) : "query transformations shared with a search that uses another vector";
        CachedSupplier<?> cached = transforms.computeIfAbsent(key, k -> CachedSupplier.wrap(supplier));
        // a key of type Key<V> is only ever mapped to a supplier of V, by the line above
        @SuppressWarnings("unchecked")
        V value = (V) cached.get();
        return value;
    }

    private boolean isTheQuery(float[] query) {
        float[] bound = this.query.compareAndExchange(null, query);
        return bound == null || bound == query;
    }

    /**
     * Returns the number of distinct transformations requested, for tests. Public as the tests of the formats that use
     * it are in other packages.
     */
    public int size() {
        return transforms.size();
    }
}
