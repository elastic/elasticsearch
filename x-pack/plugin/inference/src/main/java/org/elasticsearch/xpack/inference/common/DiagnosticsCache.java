/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.cache.Cache;

/**
 * Surface implemented by inference caches that report runtime diagnostics through the
 * {@code GetInferenceDiagnosticsAction} response. Implementations are expected to return
 * safe defaults (count = 0, empty stats) when {@link #cacheEnabled()} is false.
 */
public interface DiagnosticsCache {
    boolean cacheEnabled();

    int cacheCount();

    Cache.Stats stats();
}
