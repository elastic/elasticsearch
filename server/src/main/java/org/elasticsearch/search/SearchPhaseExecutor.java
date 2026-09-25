/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Runs a shard search phase body while driving the {@link SearchOperationListener} protocol: the pre hook, then exactly one of the
 * success or failure hooks. The failure hook fires for any {@link Throwable}, so a shard counter cannot be left dangling by an error.
 */
public final class SearchPhaseExecutor {

    private SearchPhaseExecutor() {}

    public static <E extends Exception> void executeDfsPhase(
        SearchOperationListener listener,
        SearchContext searchContext,
        CheckedRunnable<E> body
    ) throws E {
        listener.onPreDfsPhase(searchContext);
        long start = System.nanoTime();
        try {
            body.run();
        } catch (Throwable t) {
            listener.onFailedDfsPhase(searchContext, t);
            throw t;
        }
        listener.onDfsPhase(searchContext, System.nanoTime() - start);
    }

    public static <E extends Exception> void executeQueryPhase(
        SearchOperationListener listener,
        SearchContext searchContext,
        CheckedRunnable<E> body
    ) throws E {
        listener.onPreQueryPhase(searchContext);
        long start = System.nanoTime();
        try {
            body.run();
        } catch (Throwable t) {
            listener.onFailedQueryPhase(searchContext, t);
            throw t;
        }
        listener.onQueryPhase(searchContext, System.nanoTime() - start);
    }

    public static <E extends Exception> void executeFetchPhase(
        SearchOperationListener listener,
        SearchContext searchContext,
        CheckedRunnable<E> body
    ) throws E {
        listener.onPreFetchPhase(searchContext);
        long start = System.nanoTime();
        try {
            body.run();
        } catch (Throwable t) {
            listener.onFailedFetchPhase(searchContext, t);
            throw t;
        }
        listener.onFetchPhase(searchContext, System.nanoTime() - start);
    }
}
