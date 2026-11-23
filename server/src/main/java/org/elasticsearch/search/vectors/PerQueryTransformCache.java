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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * {@link QueryTransformCache} that computes each transformation at most once, outside the map's lock: searches that need
 * it while it is being computed wait for it, while searches that need other keys do not. Only the first caller's supplier
 * is kept for a key, which is also the one retried if it throws.
 */
final class PerQueryTransformCache implements QueryTransformCache {

    private final ConcurrentHashMap<Object, CachedSupplier<?>> transforms = new ConcurrentHashMap<>();

    @Override
    public <V> V getSharedQuery(Object key, Supplier<V> supplier) {
        CachedSupplier<?> cached = transforms.computeIfAbsent(key, k -> CachedSupplier.wrap(supplier));
        // each kind of key is only ever used with one type of value, see QueryTransformCache#getSharedQuery
        @SuppressWarnings("unchecked")
        V value = (V) cached.get();
        return value;
    }
}
