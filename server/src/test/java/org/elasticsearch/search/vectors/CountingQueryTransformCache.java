/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * {@link QueryTransformCache#perQuery()} that counts the transformations it computes, so tests can check how many
 * times a query was transformed across the searches sharing it.
 */
public final class CountingQueryTransformCache implements QueryTransformCache {

    private final QueryTransformCache delegate = QueryTransformCache.perQuery();
    private final AtomicInteger computed = new AtomicInteger();

    @Override
    public <V> V getSharedQuery(Object key, Supplier<V> supplier) {
        return delegate.getSharedQuery(key, () -> {
            computed.incrementAndGet();
            return supplier.get();
        });
    }

    /** Returns the number of transformations computed so far. */
    public int computed() {
        return computed.get();
    }
}
