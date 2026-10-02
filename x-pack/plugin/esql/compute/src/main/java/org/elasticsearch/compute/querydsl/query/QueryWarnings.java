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
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Bridges per-driver {@link Warnings} into {@link Query queries} that need warnings.
 * <p>
 *     ESQL attaches warnings to the {@link Driver}, so each {@link Warnings} has to be
 *     created from a {@link Driver}. Lucene's {@link Query}/{@link Weight}/{@link BulkScorer}
 *     don't give us a good way to bind anything per-{@link Driver} <strong>and</strong>
 *     properly cache {@link Weight}s: a shard's {@link Weight}/{@link org.apache.lucene.search.Scorer}
 *     is built once and shared across every driver/slice that scores that shard, and Lucene gives no
 *     other channel to correlate "which driver is calling right now". So this wedges that information
 *     into place using thread identity.
 * </p>
 * <p>
 *     There are exactly two eternal instances of this class for the whole JVM: {@link #NOOP}, which
 *     discards everything, and {@link #EMIT}, which does the thread-based bind/dispatch described
 *     above. Neither is ever constructed per-request; both live for the process lifetime, exactly like
 *     any other static singleton. Because of that, a plain {@link ThreadLocal} is fine for
 *     {@link #perThreadWarnings}: there's no per-instance lifecycle to leak, since bind/unbind is
 *     always tightly scoped around one synchronous call and the singleton itself is never discarded.
 * </p>
 */
public class QueryWarnings {
    /**
     * A no-op instance that silently discards all warnings. Use this when a
     * {@link SearchExecutionContext} must carry a {@link QueryWarnings} but
     * the caller intentionally does not want warnings emitted - for example,
     * the lookup-join and detached remote-fetch paths.
     */
    public static final QueryWarnings NOOP = new QueryWarnings() {
        @Override
        public Releasable bind(DriverContext dc, IdentityHashMap<Query, Warnings> map) {
            return () -> {};
        }

        @Override
        public Releasable bind(Map<? extends Query, Warnings> prebuilt) {
            return () -> {};
        }

        @Override
        public Releasable bindDiscarding() {
            return () -> {};
        }

        @Override
        void registerException(SingleValueMatchQuery query, Class<? extends Exception> exceptionClass, String message) {}
    };

    /**
     * The singleton bridge used everywhere warnings actually need to be emitted; see the class
     * Javadoc for why there's exactly one of these.
     */
    public static final QueryWarnings EMIT = new QueryWarnings();

    /**
     * Per-thread binding: the {@link DriverContext} (for lazy {@link Warnings} creation) and the
     * per-driver map that accumulates those Warnings across multiple
     * {@link LuceneOperator#getOutput} calls. The {@link DriverContext} is {@code null} when
     * the caller supplies pre-built Warnings rather than requesting lazy creation.
     */
    private record ThreadState(@Nullable DriverContext dc, @Nullable IdentityHashMap<Query, Warnings> map, boolean discard) {}

    private final ThreadLocal<ThreadState> perThreadWarnings = new ThreadLocal<>();

    private QueryWarnings() {}

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
        return doBind(new ThreadState(dc, map, false));
    }

    /**
     * Bind a pre-built {@code warnings} map to the calling thread, without a {@link DriverContext}.
     * Used when the caller has already constructed the per-query {@link Warnings} instances and
     * needs no lazy creation.
     *
     * @throws IllegalStateException if this thread already has a binding
     */
    public Releasable bind(Map<? extends Query, Warnings> prebuilt) {
        return doBind(new ThreadState(null, new IdentityHashMap<>(prebuilt), false));
    }

    /**
     * Bind a placeholder on the calling thread that silently discards any warnings raised while it's
     * bound, without a {@link DriverContext} or a prebuilt map. Used when {@link Query}s reachable from
     * this bridge (e.g. a {@link SingleValueMatchQuery} nested inside a kNN query's filter) may fire
     * outside a driver's lifecycle -- for example while eagerly evaluating a filter during
     * {@code IndexSearcher#rewrite}, which can happen before any {@link DriverContext}/driver exists to
     * own the warning (see {@code LuceneSliceQueue#create}). There is currently no well-defined owner
     * for warnings raised in that window, so they're dropped rather than attributed to a driver.
     *
     * @throws IllegalStateException if this thread already has a binding
     */
    public Releasable bindDiscarding() {
        return doBind(new ThreadState(null, null, true));
    }

    private Releasable doBind(ThreadState state) {
        if (perThreadWarnings.get() != null) {
            throw new IllegalStateException("QueryWarnings is already bound on thread [" + Thread.currentThread().getName() + "]");
        }
        perThreadWarnings.set(state);
        return perThreadWarnings::remove;
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
        if (state.discard()) {
            return;
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
}
