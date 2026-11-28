/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * One {@code _reload_search_analyzers} request on one node, shared by every index it reloads there so that a resource several
 * indices use is read once ({@link #loadOnce}). Shard recovery passes {@code null}.
 */
public final class ReloadToken {
    private final Map<Object, CompletableFuture<Object>> loaded = new ConcurrentHashMap<>();

    /** Runs {@code loader} for the first caller with {@code key}; later callers get its result or exception. */
    @SuppressWarnings("unchecked") // each key is only ever loaded by a loader of one type
    public <T> T loadOnce(Object key, Supplier<T> loader) {
        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> existing = loaded.putIfAbsent(key, mine);
        if (existing == null) {
            try {
                T value = loader.get();
                mine.complete(value);
                return value;
            } catch (RuntimeException e) {
                mine.completeExceptionally(e);
                throw e;
            }
        }
        try {
            return (T) existing.join();
        } catch (CompletionException e) {
            throw e.getCause() instanceof RuntimeException r ? r : e;
        }
    }
}
