/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.xpack.core.inference.action.GetInferenceDiagnosticsAction;

/**
 * Base class for inference caches that report runtime diagnostics through the
 * {@link  GetInferenceDiagnosticsAction} response. Subclasses provide the {@link Cache}
 * instance at construction time and implement {@link #cacheEnabled()} to gate access behind
 * their specific feature flag. Safe defaults (count = 0, empty stats) are returned automatically
 * when {@link #cacheEnabled()} is false.
 */
public abstract class DiagnosticsCache<V> {

    static final Cache.Stats EMPTY = new Cache.Stats(0, 0, 0);

    protected final Cache<InferenceIdAndProject, V> cache;

    protected DiagnosticsCache(Cache<InferenceIdAndProject, V> cache) {
        this.cache = cache;
    }

    public abstract boolean cacheEnabled();

    public final Cache.Stats stats() {
        return cacheEnabled() ? cache.stats() : EMPTY;
    }

    public final int cacheCount() {
        return cacheEnabled() ? cache.count() : 0;
    }
}
