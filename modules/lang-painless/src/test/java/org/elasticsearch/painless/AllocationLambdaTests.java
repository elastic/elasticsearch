/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Allocation tracking for lambdas and method references (PR 8). The test context ({@link PainlessTestScript}) does not
 * support cancellation, so static lambdas / references have no script pointer of their own; before this change allocations
 * reached through them leaked. These tests confirm static lambda bodies, constructor references, and static- and
 * unbound-instance-method references to {@code @allocates} targets are charged, while bounded cases still complete.
 */
public class AllocationLambdaTests extends AllocationTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        // Add the @allocates test allowlist so static-method references have a controlled estimator target.
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    public void testStaticLambdaBodyArrayAllocationTrips() {
        // Static lambda invoked via empty Optional; its body array allocation is charged only because #scriptThis is injected.
        assertTripsLimit("return Optional.empty().orElseGet(() -> { return new int[1000000]; });", "1kb");
    }

    public void testStaticLambdaBodyAllocationCounted() {
        // The body allocation reaches the counter, proving the static lambda body reaches the script instance.
        long bytes = allocatedBytes("Optional.empty().orElseGet(() -> { return new int[100]; }); return null;");
        assertTrue("expected the static lambda body allocation to be counted, but only [" + bytes + "] bytes charged", bytes >= 400);
    }

    public void testBoundedStaticLambdaCompletes() {
        // A bounded static lambda body runs to completion well under the limit.
        Object result = compile("int[] a = (int[]) Optional.empty().orElseGet(() -> { return new int[4]; }); return a.length;", "1mb")
            .execute();
        assertEquals(4, result);
    }

    public void testConstructorReferenceChargedPerInvocation() {
        // ArrayList::new is an annotated ctor; the per-invocation charge accumulates across the loop and trips.
        assertTripsLimit("int c(Supplier s) { for (int i = 0; i < 1000000; ++i) { s.get(); } return 1; } return c(ArrayList::new);", "1mb");
    }

    public void testStaticMethodReferenceTripsInSingleCall() {
        // staticAllocating's estimator returns 16 * n; one large-argument call exceeds the limit.
        assertTripsLimit(
            "int c(IntUnaryOperator op) { return op.applyAsInt(1000000); } return c(AllocationEstimatorTestObject::staticAllocating);",
            "1mb"
        );
    }

    public void testStaticMethodReferenceCounted() {
        // Two calls charge 16 * n each, proving the estimator runs with the actual argument on every invocation.
        long bytes = allocatedBytes(
            "int c(IntUnaryOperator op) { return op.applyAsInt(10) + op.applyAsInt(20); } "
                + "c(AllocationEstimatorTestObject::staticAllocating); return null;"
        );
        assertTrue("expected per-invocation static-method-reference charges to be counted, but only [" + bytes + "] charged", bytes >= 480);
    }

    public void testBoundedConstructorReferenceCompletes() {
        // A single constructor-reference invocation stays under the limit and returns normally.
        Object result = compile("int c(Supplier s) { return ((List) s.get()).size(); } return c(ArrayList::new);", "1mb").execute();
        assertEquals(0, result);
    }

    public void testInstanceMethodReferenceTrips() {
        // Unbound instance-method reference (receiver is the first argument); its estimator is huge, so one call trips.
        assertTripsLimit(
            "int c(ToIntFunction f) { return f.applyAsInt(new AllocationEstimatorTestObject()); } "
                + "return c(AllocationEstimatorTestObject::hugeAllocatingInstance);",
            "1mb"
        );
    }

    public void testInstanceMethodReferenceCounted() {
        // constantAllocating charges 48 per call, invoked twice, proving the estimator sees the receiver each time.
        long bytes = allocatedBytes(
            "int c(ToIntFunction f) { AllocationEstimatorTestObject o = new AllocationEstimatorTestObject(); "
                + "return f.applyAsInt(o) + f.applyAsInt(o); } "
                + "c(AllocationEstimatorTestObject::constantAllocating); return null;"
        );
        assertTrue(
            "expected per-invocation instance-method-reference charges to be counted, but only [" + bytes + "] charged",
            bytes >= 96
        );
    }
}
