/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.querydsl.query;

import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;

import java.util.Map;

/**
 * Bridges per-driver {@link Warnings} into {@link SingleValueMatchQuery}.
 * <p>
 *     A {@link SingleValueMatchQuery}'s Lucene {@code Weight}/{@code Scorer} tree is built once per
 *     shard/query and can be reused by every driver that scans that shard for that query (e.g. sibling
 *     {@code DOC}-partitioned slices of the same shard). Because of that sharing, a
 *     {@link SingleValueMatchQuery} instance has no way to know, from its own fields, which driver's
 *     thread is currently running it -- so it can't just hold a {@code Warnings} field directly.
 * </p>
 * <p>
 *     Instead, one {@link SingleValueQueryWarnings} instance is created per local query execution
 *     (see {@code ComputeService#runCompute}) and threaded down into every
 *     {@link SingleValueMatchQuery} built for that execution. Each driver
 *     ({@code org.elasticsearch.compute.lucene.query.LuceneOperator}) owns its own
 *     {@code IdentityHashMap<SingleValueMatchQuery, Warnings>}, and binds it to this bridge's
 *     {@link CloseableThreadLocal} immediately before -- and unbinds it immediately after -- each
 *     synchronous call into Lucene on its own thread. {@link SingleValueMatchQuery} then resolves
 *     "my" {@link Warnings} by looking itself up (by identity) in whatever map is currently bound to
 *     the calling thread.
 * </p>
 * <p>
 *     <strong>Behavior change / cap:</strong> before this bridge existed, every
 *     {@link SingleValueMatchQuery} was built with a single, hardcoded
 *     {@code DriverContext.WarningsMode.COLLECT} {@code Warnings} instance shared by every driver that
 *     ever ran that query, so the {@code Warnings} internal cap on the number of recorded warnings
 *     ({@code Warnings.MAX_ADDED_WARNINGS}) applied once, globally, across the whole search. Now each
 *     driver builds and owns its own {@code Warnings} per query node (see the cap check in
 *     {@code org.elasticsearch.compute.lucene.query.LuceneOperator#populateSingleValueQueryWarnings}),
 *     so the cap is enforced per driver, per query node. In the worst case (many drivers touching the
 *     same multi-valued field), more total warnings can now be surfaced to the user than before.
 * </p>
 */
public final class SingleValueQueryWarnings implements Releasable {

    private final CloseableThreadLocal<Map<SingleValueMatchQuery, Warnings>> perThreadWarnings = new CloseableThreadLocal<>();

    /**
     * Bind {@code warnings} as the map used to resolve {@link SingleValueMatchQuery} warnings on the
     * calling thread. Must be paired with a matching {@link #clear()} -- even if the guarded Lucene
     * call throws -- so a thread is never left pointing at another driver's map.
     *
     * @throws IllegalStateException if this thread already has a map bound, which would mean a driver
     *                                is reentering Lucene from within its own synchronous call
     */
    public void set(Map<SingleValueMatchQuery, Warnings> warnings) {
        if (perThreadWarnings.get() != null) {
            throw new IllegalStateException(
                "SingleValueQueryWarnings is already bound on thread [" + Thread.currentThread().getName() + "]"
            );
        }
        perThreadWarnings.set(warnings);
    }

    /**
     * Unbind whatever map {@link #set} bound on the calling thread.
     */
    public void clear() {
        perThreadWarnings.set(null);
    }

    /**
     * Called by {@link SingleValueMatchQuery} to register a multi-value warning against whatever
     * driver's map is currently bound to the calling thread.
     *
     * @throws IllegalStateException if no map is bound on this thread, or the bound map has no entry
     *                                for {@code query} -- both indicate a caller failed to bind the
     *                                map (via {@link #set}) before running this query
     */
    void registerException(SingleValueMatchQuery query, Class<? extends Exception> exceptionClass, String message) {
        Map<SingleValueMatchQuery, Warnings> warnings = perThreadWarnings.get();
        if (warnings == null) {
            throw new IllegalStateException("no warnings bound on thread [" + Thread.currentThread().getName() + "] for [" + query + "]");
        }
        Warnings w = warnings.get(query);
        if (w == null) {
            throw new IllegalStateException(
                "no warnings registered for [" + query + "] on thread [" + Thread.currentThread().getName() + "]"
            );
        }
        w.registerException(exceptionClass, message);
    }

    @Override
    public void close() {
        perThreadWarnings.close();
    }
}
