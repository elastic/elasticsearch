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
 * unbound- and bound-instance-method references to {@code @allocates} targets are charged, while bounded cases still complete.
 * <p>
 * Nesting is a separate axis: an inner construct built <em>inside</em> an outer static lambda body reaches the script through
 * the enclosing lambda's synthetic-method {@code #scriptThis} rather than the top-level script. Depth 2 could be special-cased
 * (script + one synthetic method); the depth-3 rows force the fully recursive propagation (script → outer → middle → inner).
 * Method references are leaves (no body), so the container is always a lambda; the "leaf" column is the innermost construct.
 * The {@code def} counterparts live in {@link AllocationDefLambdaTests}.
 * <table>
 *   <caption>nested lambda / reference allocation coverage</caption>
 *   <tr><th>Shape</th><th>Leaf = lambda body</th><th>Leaf = reference</th></tr>
 *   <tr><td>depth 2 (outer lambda → leaf)</td>
 *       <td>{@link #testNestedStaticLambdaBodyAllocationTrips}</td>
 *       <td>ctor {@link #testNestedConstructorReferenceInLambdaBodyTrips},
 *           static {@link #testNestedStaticMethodReferenceInLambdaBodyTrips},
 *           unbound-instance {@link #testNestedInstanceMethodReferenceInLambdaBodyTrips},
 *           bound-instance {@link #testNestedBoundInstanceMethodReferenceInLambdaBodyTrips}</td></tr>
 *   <tr><td>depth 3 (outer → middle → leaf)</td>
 *       <td>{@link #testTripleNestedStaticLambdaBodyAllocationTrips}</td>
 *       <td>ctor {@link #testTripleNestedConstructorReferenceInLambdaBodyTrips},
 *           static {@link #testTripleNestedStaticMethodReferenceInLambdaBodyTrips},
 *           unbound-instance {@link #testTripleNestedInstanceMethodReferenceInLambdaBodyTrips},
 *           bound-instance {@link #testTripleNestedBoundInstanceMethodReferenceInLambdaBodyTrips}</td></tr>
 * </table>
 * <p>
 * Mixed static/def typing across a nesting boundary (a typed lambda containing a {@code def} inner construct, or the reverse)
 * is covered in {@link AllocationDefLambdaTests}, since those cases all involve {@code def} routing on one side.
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

    public void testBoundInstanceMethodReferenceTrips() {
        // A bound instance-method reference (captured receiver) to an annotated target charges per invocation; the script is
        // captured ahead of the receiver and dropped before the delegate runs. Its huge estimator trips in one call.
        assertTripsLimit(
            "int c(IntSupplier s) { return s.getAsInt(); } "
                + "AllocationEstimatorTestObject o = new AllocationEstimatorTestObject(); return c(o::hugeAllocatingInstance);",
            "1mb"
        );
    }

    public void testBoundInstanceMethodReferenceCounted() {
        // constantAllocating charges 48 per call; two calls through a bound reference are both counted.
        long bytes = allocatedBytes(
            "int c(IntSupplier s) { return s.getAsInt() + s.getAsInt(); } "
                + "AllocationEstimatorTestObject o = new AllocationEstimatorTestObject(); c(o::constantAllocating); return null;"
        );
        assertTrue("expected per-invocation bound instance-method-reference charges to be counted, but only [" + bytes + "]", bytes >= 96);
    }

    public void testBoundReferenceToUnannotatedTargetCompletes() {
        // A bound reference to an unannotated target is not charge-captured and resolves normally.
        Object result = compile("int c(IntSupplier s) { return s.getAsInt(); } String x = 'hello'; return c(x::length);", "1mb").execute();
        assertEquals(5, result);
    }

    public void testNestedStaticLambdaBodyAllocationTrips() {
        // An inner static lambda inside an outer static lambda body: the inner captures #scriptThis from the outer's
        // synthetic method, so its own body allocation is charged when both are invoked.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { return Optional.empty().orElseGet(() -> { return new int[1000000]; }); });",
            "1kb"
        );
    }

    public void testNestedConstructorReferenceInLambdaBodyTrips() {
        // A constructor reference to an annotated target, built and invoked inside an outer static lambda body: its
        // #scriptThis capture resolves against the outer lambda's synthetic-method #scriptThis, and the per-invocation
        // charge accumulates across the loop and trips.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { "
                + "for (int i = 0; i < 1000000; ++i) { Optional.empty().orElseGet(ArrayList::new); } return 1; });",
            "1mb"
        );
    }

    public void testNestedStaticMethodReferenceInLambdaBodyTrips() {
        // A static-method reference (staticAllocating, estimator 16 * n) built and invoked inside an outer static lambda
        // body; its #scriptThis capture resolves against the outer lambda and one large-argument call trips.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { "
                + "return Optional.of(1000000).map(AllocationEstimatorTestObject::staticAllocating); });",
            "1mb"
        );
    }

    public void testNestedInstanceMethodReferenceInLambdaBodyTrips() {
        // An unbound instance-method reference (String::toUpperCase) invoked inside an outer static lambda body; its
        // #scriptThis capture resolves against the outer lambda and the per-invocation recase charge trips across the loop.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { "
                + "for (int i = 0; i < 1000000; ++i) { Optional.of('abcdefghij').map(String::toUpperCase); } return 1; });",
            "1mb"
        );
    }

    public void testNestedBoundInstanceMethodReferenceInLambdaBodyTrips() {
        // A bound instance-method reference (s::concat, captured receiver local to the lambda body) invoked inside an outer
        // static lambda body; the script capture sits ahead of the receiver capture and is dropped before the delegate runs.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { String s = 'abcdefghij'; "
                + "for (int i = 0; i < 1000000; ++i) { Optional.of(s).map(s::concat); } return 1; });",
            "1mb"
        );
    }

    public void testTripleNestedStaticLambdaBodyAllocationTrips() {
        // Depth 3: outer -> middle -> inner static lambda. #scriptThis must propagate script -> outer -> middle -> inner
        // (not special-cased at depth 2), so the innermost body allocation is charged when all three are invoked.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { return Optional.empty().orElseGet(() -> { "
                + "return Optional.empty().orElseGet(() -> { return new int[1000000]; }); }); });",
            "1kb"
        );
    }

    public void testTripleNestedConstructorReferenceInLambdaBodyTrips() {
        // Depth 3 with a reference at the leaf: a constructor reference built inside the innermost of three nested static
        // lambda bodies. Its #scriptThis capture must resolve through two enclosing lambdas; the per-invocation charge trips.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { return Optional.empty().orElseGet(() -> { "
                + "for (int i = 0; i < 1000000; ++i) { Optional.empty().orElseGet(ArrayList::new); } return 1; }); });",
            "1mb"
        );
    }

    public void testTripleNestedStaticMethodReferenceInLambdaBodyTrips() {
        // Depth 3 with a method reference at the leaf: a static-method reference invoked inside the innermost of three nested
        // static lambda bodies, proving the charge machinery threads the script through both enclosing lambdas to a ref.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { return Optional.empty().orElseGet(() -> { "
                + "return Optional.of(1000000).map(AllocationEstimatorTestObject::staticAllocating); }); });",
            "1mb"
        );
    }

    public void testTripleNestedInstanceMethodReferenceInLambdaBodyTrips() {
        // Depth 3 with an unbound instance-method reference (String::toUpperCase) at the leaf, inside three nested static
        // lambda bodies; the per-invocation recase charge trips, proving the script threads through both enclosing lambdas.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { return Optional.empty().orElseGet(() -> { "
                + "for (int i = 0; i < 1000000; ++i) { Optional.of('abcdefghij').map(String::toUpperCase); } return 1; }); });",
            "1mb"
        );
    }

    public void testTripleNestedBoundInstanceMethodReferenceInLambdaBodyTrips() {
        // Depth 3 with a bound instance-method reference (s::concat, captured receiver local to the innermost lambda) at the
        // leaf, inside three nested static lambda bodies; the script capture threads through both enclosing lambdas.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { return Optional.empty().orElseGet(() -> { String s = 'abcdefghij'; "
                + "for (int i = 0; i < 1000000; ++i) { Optional.of(s).map(s::concat); } return 1; }); });",
            "1mb"
        );
    }
}
