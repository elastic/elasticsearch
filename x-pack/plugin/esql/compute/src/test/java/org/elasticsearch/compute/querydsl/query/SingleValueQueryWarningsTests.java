/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.querydsl.query;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link QueryWarnings} bridge itself, independent of any real Lucene
 * scoring. {@link SingleValueMatchQuery} instances are only used here as identity keys (equality is
 * only ever based on field name -- see {@link SingleValueMatchQuery#equals}), so a bare stub field data
 * is enough; none of these tests exercise real Lucene search.
 * <p>
 *     There is exactly one non-{@code NOOP} instance of {@link QueryWarnings} in the whole JVM --
 *     {@link QueryWarnings#EMIT} -- so these tests share it rather than constructing independent
 *     instances. Every test here binds and unbinds within a single {@code try}-with-resources block,
 *     which keeps the shared singleton clean for the next test.
 * </p>
 */
public class SingleValueQueryWarningsTests extends ESTestCase {

    private SingleValueMatchQuery newQuery(QueryWarnings bridge, String fieldName, String message) {
        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn(fieldName);
        return new SingleValueMatchQuery(fieldData, bridge, new TestWarningsSource("test"), message);
    }

    public void testRegisterDelegatesToBoundWarnings() {
        QueryWarnings bridge = QueryWarnings.EMIT;
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        try (Releasable ignored = bridge.bind(Map.of(query, warnings))) {
            bridge.registerException(query, IllegalArgumentException.class, "boom");
        }
        assertWarnings(
            "Line 1:1: evaluation of [test] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: boom"
        );
    }

    public void testRegisterWithoutBindingThrows() {
        QueryWarnings bridge = QueryWarnings.EMIT;
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        expectThrows(IllegalStateException.class, () -> bridge.registerException(query, IllegalArgumentException.class, "boom"));
    }

    public void testRegisterForUnknownQueryThrows() {
        QueryWarnings bridge = QueryWarnings.EMIT;
        SingleValueMatchQuery known = newQuery(bridge, "known", "known");
        SingleValueMatchQuery unknown = newQuery(bridge, "unknown", "unknown");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        try (Releasable ignored = bridge.bind(Map.of(known, warnings))) {
            expectThrows(IllegalStateException.class, () -> bridge.registerException(unknown, IllegalArgumentException.class, "boom"));
        }
    }

    public void testReentrantBindThrows() {
        QueryWarnings bridge = QueryWarnings.EMIT;
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        try (Releasable ignored = bridge.bind(Map.of(query, warnings))) {
            expectThrows(IllegalStateException.class, () -> bridge.bind(Map.of(query, warnings)));
        }
    }

    /**
     * Sequential (non-overlapping) bind/unbind cycles on the shared singleton never leak state:
     * unbinding via the {@link Releasable} returned by {@code bind()} always clears the thread-local
     * so a later, unrelated {@code bind()} call on the same thread succeeds.
     */
    public void testSequentialBindUnbindNeverLeaks() {
        QueryWarnings bridge = QueryWarnings.EMIT;
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        try (Releasable ignored = bridge.bind(Map.of(query, warnings))) {
            // unbinding via close(), below, allows bind() to be called again on the same thread
        }
        try (Releasable ignored = bridge.bind(Map.of(query, warnings))) {
            // no exception
        }
    }

    /**
     * Each thread binds its own map to the same shared {@link QueryWarnings#EMIT} singleton -- exactly
     * as two drivers scanning the same shard/query concurrently would -- and only ever sees its own
     * map, never the other thread's.
     */
    public void testPerThreadIsolation() throws Exception {
        QueryWarnings bridge = QueryWarnings.EMIT;
        SingleValueMatchQuery queryA = newQuery(bridge, "a", "a");
        SingleValueMatchQuery queryB = newQuery(bridge, "b", "b");
        Warnings warningsA = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("a"));
        Warnings warningsB = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("b"));

        CountDownLatch bothBound = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Runnable taskA = () -> {
            try (Releasable ignored = bridge.bind(Map.of(queryA, warningsA))) {
                bothBound.countDown();
                bothBound.await();
                // Must not see queryB's mapping -- registering it here must throw.
                expectThrows(IllegalStateException.class, () -> bridge.registerException(queryB, IllegalArgumentException.class, "x"));
                bridge.registerException(queryA, IllegalArgumentException.class, "x");
            } catch (Throwable t) {
                failure.set(t);
            }
        };
        Runnable taskB = () -> {
            try (Releasable ignored = bridge.bind(Map.of(queryB, warningsB))) {
                bothBound.countDown();
                bothBound.await();
                expectThrows(IllegalStateException.class, () -> bridge.registerException(queryA, IllegalArgumentException.class, "x"));
                bridge.registerException(queryB, IllegalArgumentException.class, "x");
            } catch (Throwable t) {
                failure.set(t);
            }
        };

        Thread t1 = new Thread(taskA);
        Thread t2 = new Thread(taskB);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        // Note: HeaderWarning's response headers live in a per-thread ThreadContext slot, so the
        // warnings registered above (on t1/t2) never surface via assertWarnings() on this (the test
        // runner's) thread -- that's expected and not what this test is checking. What matters here is
        // that registering on the "wrong" query for a thread's bound map always throws, and registering
        // on the "right" one never does, even with two threads racing on the same bridge instance.
    }
}
