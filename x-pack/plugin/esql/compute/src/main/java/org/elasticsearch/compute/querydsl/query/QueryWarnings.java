/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.querydsl.query;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Bridges per-driver {@link Warnings} into {@link Query queries} that need warnings.
 * <p>
 *     ESQL attaches warnings to the {@link Driver}, so each {@link Warnings} has to be
 *     created from a {@link Driver}. Lucene's {@link Query}/{@link Weight}/{@link BulkScorer}
 *     don't give us a good way to bind anything per-{@link Driver} <strong>and</strong>
 *     properly cache {@link Weight}s. So this wedges them into place using a very contained
 *     {@link CloseableThreadLocal}.
 * </p>
 */
public final class QueryWarnings implements Releasable {

    /**
     * Per-thread binding: the {@link DriverContext} (for lazy {@link Warnings} creation) and the
     * per-driver map that accumulates those Warnings across multiple
     * {@link LuceneOperator#getOutput} calls. The {@link DriverContext} is {@code null} when
     * the caller supplies pre-built Warnings rather than requesting lazy creation.
     */
    private record ThreadState(@Nullable DriverContext dc, IdentityHashMap<Query, Warnings> map) {}

    private final CloseableThreadLocal<ThreadState> perThreadWarnings = new CloseableThreadLocal<>();

    /**
     * Bind this driver's warnings map and its {@link DriverContext} to the calling thread. Returns a
     * {@link Releasable} that clears the binding; the caller must close it -- even if the guarded
     * Lucene call throws -- so a thread is never left pointing at another driver's state.
     * <p>
     *     A {@link Warnings} is created lazily (via {@code dc}) the first time
     *     {@link #registerException} is called for a given {@link Query} on this driver. Passing an
     *     already-populated {@code map} lets accumulated Warnings persist across multiple
     *     {@code getOutput()} calls on the same driver.
     * </p>
     *
     * @throws IllegalStateException if this thread already has a binding, which would mean a driver
     *                                is reentering Lucene from within its own synchronous call
     */
    public Releasable bind(DriverContext dc, IdentityHashMap<Query, Warnings> map) {
        return doBind(new ThreadState(dc, map));
    }

    /**
     * Bind a pre-built {@code warnings} map to the calling thread, without a {@link DriverContext}.
     * Used when the caller has already constructed the per-query {@link Warnings} instances and
     * needs no lazy creation -- for example,
     * {@link org.elasticsearch.compute.operator.lookup.QueryList} which builds a private, one-off
     * bridge for a single non-shared query.
     *
     * @throws IllegalStateException if this thread already has a binding
     */
    public Releasable bind(Map<? extends Query, Warnings> prebuilt) {
        return doBind(new ThreadState(null, new IdentityHashMap<>(prebuilt)));
    }

    private Releasable doBind(ThreadState state) {
        if (perThreadWarnings.get() != null) {
            throw new IllegalStateException("QueryWarnings is already bound on thread [" + Thread.currentThread().getName() + "]");
        }
        perThreadWarnings.set(state);
        return () -> perThreadWarnings.set(null);
    }

    /**
     * Called by {@link SingleValueMatchQuery} to register a multi-value warning against whatever
     * driver's state is currently bound to the calling thread. If this is the first time this
     * {@code query} has fired on the current driver, a fresh {@link Warnings} is created via the
     * bound {@link DriverContext}.
     *
     * @throws IllegalStateException if no state is bound on this thread, or the bound state has no
     *                                {@link DriverContext} and no pre-built entry for {@code query}
     *                                -- both indicate a caller failed to bind before running the query
     */
    void registerException(SingleValueMatchQuery query, Class<? extends Exception> exceptionClass, String message) {
        ThreadState state = perThreadWarnings.get();
        if (state == null) {
            throw new IllegalStateException("no warnings bound on thread [" + Thread.currentThread().getName() + "] for [" + query + "]");
        }
        Warnings w = state.map().computeIfAbsent(query, q -> {
            if (state.dc() == null) {
                throw new IllegalStateException(
                    "no warnings registered for [" + query + "] on thread [" + Thread.currentThread().getName() + "]"
                );
            }
            return state.dc().createWarnings(query.source());
        });
        w.registerException(exceptionClass, message);
    }

    @Override
    public void close() {
        perThreadWarnings.close();
    }
}
