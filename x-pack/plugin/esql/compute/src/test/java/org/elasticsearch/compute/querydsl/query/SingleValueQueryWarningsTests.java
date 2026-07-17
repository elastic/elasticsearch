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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link SingleValueQueryWarnings} bridge itself, independent of any real Lucene
 * scoring. {@link SingleValueMatchQuery} instances are only used here as identity keys (equality is
 * only ever based on field name -- see {@link SingleValueMatchQuery#equals}), so a bare stub field data
 * is enough; none of these tests exercise real Lucene search.
 */
public class SingleValueQueryWarningsTests extends ESTestCase {

    private SingleValueMatchQuery newQuery(SingleValueQueryWarnings bridge, String fieldName, String message) {
        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn(fieldName);
        return new SingleValueMatchQuery(fieldData, bridge, new TestWarningsSource("test"), message);
    }

    public void testRegisterDelegatesToBoundWarnings() {
        SingleValueQueryWarnings bridge = new SingleValueQueryWarnings();
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        bridge.set(Map.of(query, warnings));
        bridge.registerException(query, IllegalArgumentException.class, "boom");
        bridge.clear();
        assertWarnings(
            "Line 1:1: evaluation of [test] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: boom"
        );
    }

    public void testRegisterWithoutBindingThrows() {
        SingleValueQueryWarnings bridge = new SingleValueQueryWarnings();
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        expectThrows(IllegalStateException.class, () -> bridge.registerException(query, IllegalArgumentException.class, "boom"));
    }

    public void testRegisterForUnknownQueryThrows() {
        SingleValueQueryWarnings bridge = new SingleValueQueryWarnings();
        SingleValueMatchQuery known = newQuery(bridge, "known", "known");
        SingleValueMatchQuery unknown = newQuery(bridge, "unknown", "unknown");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        bridge.set(Map.of(known, warnings));
        try {
            expectThrows(IllegalStateException.class, () -> bridge.registerException(unknown, IllegalArgumentException.class, "boom"));
        } finally {
            bridge.clear();
        }
    }

    public void testReentrantSetThrows() {
        SingleValueQueryWarnings bridge = new SingleValueQueryWarnings();
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        bridge.set(Map.of(query, warnings));
        try {
            expectThrows(IllegalStateException.class, () -> bridge.set(Map.of(query, warnings)));
        } finally {
            bridge.clear();
        }
    }

    public void testClearAllowsRebinding() {
        SingleValueQueryWarnings bridge = new SingleValueQueryWarnings();
        SingleValueMatchQuery query = newQuery(bridge, "field", "boom");
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
        bridge.set(Map.of(query, warnings));
        bridge.clear();
        // No exception: clear() unbinds, so set() can be called again on the same thread.
        bridge.set(Map.of(query, warnings));
        bridge.clear();
    }

    /**
     * Each thread binds its own map to the same bridge instance -- exactly as two drivers scanning the
     * same shard/query concurrently would -- and only ever sees its own map, never the other thread's.
     */
    public void testPerThreadIsolation() throws Exception {
        SingleValueQueryWarnings bridge = new SingleValueQueryWarnings();
        SingleValueMatchQuery queryA = newQuery(bridge, "a", "a");
        SingleValueMatchQuery queryB = newQuery(bridge, "b", "b");
        Warnings warningsA = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("a"));
        Warnings warningsB = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("b"));

        CountDownLatch bothBound = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Runnable taskA = () -> {
            try {
                bridge.set(Map.of(queryA, warningsA));
                bothBound.countDown();
                bothBound.await();
                // Must not see queryB's mapping -- registering it here must throw.
                expectThrows(IllegalStateException.class, () -> bridge.registerException(queryB, IllegalArgumentException.class, "x"));
                bridge.registerException(queryA, IllegalArgumentException.class, "x");
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                bridge.clear();
            }
        };
        Runnable taskB = () -> {
            try {
                bridge.set(Map.of(queryB, warningsB));
                bothBound.countDown();
                bothBound.await();
                expectThrows(IllegalStateException.class, () -> bridge.registerException(queryA, IllegalArgumentException.class, "x"));
                bridge.registerException(queryB, IllegalArgumentException.class, "x");
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                bridge.clear();
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
        bridge.close();
    }

    public void testCloseIsIdempotentAndSafeWithoutSet() {
        SingleValueQueryWarnings bridge = new SingleValueQueryWarnings();
        bridge.close();
        bridge.close();
    }
}
