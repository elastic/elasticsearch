/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;

import java.util.concurrent.Executor;

/**
 * Discovers parallelizable splits for an external data source.
 * File-based sources produce one split per file; connector-based sources
 * may use their own split discovery logic.
 */
public interface SplitProvider {

    SplitDiscoveryResult discoverSplits(SplitDiscoveryContext context);

    /**
     * Asynchronously discovers splits. The calling thread must return without waiting for object-store IO.
     * <p>
     * The default wraps {@link #discoverSplits(SplitDiscoveryContext)} on {@code executor} so connectors
     * and tests keep a one-line implementation. That wrap still occupies one executor thread for the whole
     * call — fine for a connector that is already in-memory or for a test thread, but not for a multi-file
     * footer fan-out. {@code FileSplitProvider} overrides this with a non-joining {@code ThrottledIterator}
     * so {@code SEARCH} and {@code esql_external_io} callers are not pinned in a gather latch.
     */
    default void discoverSplitsAsync(SplitDiscoveryContext context, Executor executor, ActionListener<SplitDiscoveryResult> listener) {
        executor.execute(() -> {
            try {
                listener.onResponse(discoverSplits(context));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    SplitProvider SINGLE = ctx -> SplitDiscoveryResult.EMPTY;
}
