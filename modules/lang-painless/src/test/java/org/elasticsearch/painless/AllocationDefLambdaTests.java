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
 * Allocation tracking for {@code def}-typed lambdas and method references (PR 8.5). When the functional-interface target is
 * unknown at compile time (a lambda/reference passed to or resolved through {@code def}) it routes through the runtime
 * reference path, which PR 8 did not cover. Calling a method on a {@code def} receiver makes the lambda argument's target
 * {@code def}.
 * <p>
 * Coverage matrix — every {@code def}-routed lambda/reference form this PR charges, and the tests that pin it. "Trips" drives
 * a limit breach; "Counted" asserts the per-invocation charge amount (that the estimator ran with the right operands through
 * the def routing); "Completes" proves a bounded / unannotated / tracking-off case still resolves normally.
 * <table>
 *   <caption>def lambda / reference allocation coverage</caption>
 *   <tr><th>Form (delegate handle kind)</th><th>Trips</th><th>Counted</th><th>Completes</th></tr>
 *   <tr><td>static lambda body</td>
 *       <td>{@link #testDefStaticLambdaBodyArrayAllocationTrips}</td>
 *       <td>{@link #testDefStaticLambdaBodyAllocationCounted}</td>
 *       <td>{@link #testBoundedDefStaticLambdaCompletes}</td></tr>
 *   <tr><td>constructor ref ({@code H_NEWINVOKESPECIAL})</td>
 *       <td>{@link #testDefConstructorReferenceChargedPerInvocation}</td>
 *       <td>—</td>
 *       <td>{@link #testDefReferenceNotChargedWhenTrackingOff}</td></tr>
 *   <tr><td>static-method ref ({@code H_INVOKESTATIC})</td>
 *       <td>{@link #testDefStaticMethodReferenceTripsInSingleCall}</td>
 *       <td>{@link #testDefStaticMethodReferenceCounted}</td>
 *       <td>—</td></tr>
 *   <tr><td>unbound instance-method ref ({@code H_INVOKEVIRTUAL}/{@code H_INVOKEINTERFACE})</td>
 *       <td>{@link #testDefInstanceMethodReferenceChargedPerInvocation}</td>
 *       <td>{@link #testDefInstanceMethodReferenceCounted}</td>
 *       <td>{@link #testDefReferenceToUnannotatedTargetCompletes}</td></tr>
 *   <tr><td>bound instance-method ref (typed receiver)</td>
 *       <td>{@link #testDefBoundInstanceMethodReferenceChargedPerInvocation}</td>
 *       <td>{@link #testDefBoundInstanceMethodReferenceCounted}</td>
 *       <td>—</td></tr>
 * </table>
 * <p>
 * Nesting is a separate axis: an inner construct built <em>inside</em> an outer def lambda body must still reach the script,
 * because its {@code #scriptThis} capture resolves against the enclosing lambda's synthetic method rather than the top-level
 * script. Depth 2 could be special-cased (script + one synthetic method); the depth-3 rows force the fully recursive
 * propagation (script → outer → middle → inner). Method references are leaves (no body), so the enclosing container is always
 * a lambda; the "leaf" column is the innermost allocating construct.
 * <table>
 *   <caption>nested def lambda / reference allocation coverage</caption>
 *   <tr><th>Shape</th><th>Leaf = lambda body</th><th>Leaf = reference</th></tr>
 *   <tr><td>depth 2 (outer lambda → leaf)</td>
 *       <td>{@link #testNestedDefStaticLambdaBodyAllocationTrips}</td>
 *       <td>ctor {@link #testNestedDefConstructorReferenceInLambdaBodyTrips},
 *           static {@link #testNestedDefStaticMethodReferenceInLambdaBodyTrips},
 *           unbound-instance {@link #testNestedDefInstanceMethodReferenceInLambdaBodyTrips},
 *           bound-instance {@link #testNestedDefBoundInstanceMethodReferenceInLambdaBodyTrips}</td></tr>
 *   <tr><td>depth 3 (outer → middle → leaf)</td>
 *       <td>{@link #testTripleNestedDefStaticLambdaBodyAllocationTrips}</td>
 *       <td>ctor {@link #testTripleNestedDefConstructorReferenceInLambdaBodyTrips},
 *           static {@link #testTripleNestedDefStaticMethodReferenceInLambdaBodyTrips},
 *           unbound-instance {@link #testTripleNestedDefInstanceMethodReferenceInLambdaBodyTrips},
 *           bound-instance {@link #testTripleNestedDefBoundInstanceMethodReferenceInLambdaBodyTrips}</td></tr>
 * </table>
 * <p>
 * Mixed typing across a nesting boundary is its own case: a typed lambda captures {@code #scriptThis} via the {@code
 * FunctionRef} path while a {@code def} reference captures via the encoding path, so nesting one inside the other is the only
 * place those two capture mechanisms compose. Both directions are covered with a reference leaf and a lambda leaf.
 * <table>
 *   <caption>mixed static/def typing across a nesting boundary</caption>
 *   <tr><th>Outer → inner</th><th>Leaf = lambda body</th><th>Leaf = reference</th></tr>
 *   <tr><td>typed outer → def inner</td>
 *       <td>{@link #testMixedTypedOuterLambdaDefInnerLambdaTrips},
 *           completes {@link #testMixedTypedOuterLambdaDefInnerLambdaCompletes}</td>
 *       <td>{@link #testMixedTypedOuterLambdaDefInnerConstructorReferenceTrips}</td></tr>
 *   <tr><td>def outer → typed inner</td>
 *       <td>{@link #testMixedDefOuterLambdaTypedInnerLambdaTrips}</td>
 *       <td>{@link #testMixedDefOuterLambdaTypedInnerConstructorReferenceTrips}</td></tr>
 *   <tr><td>depth 3: typed → typed → def inner</td>
 *       <td>{@link #testDepth3MixedTypedTypedDefInnerLambdaTrips}</td>
 *       <td>—</td></tr>
 * </table>
 * <p>
 * One instance-method-reference form is deliberately <em>not</em> covered here: a bound reference whose receiver is itself
 * {@code def} ({@code def s = obj; s::method}). It routes through the dynamic {@code REFERENCE} bootstrap, which dispatches on
 * the receiver capture and conflicts with the charge machinery's script capture; it is deferred to PR 8.6 (this PR ships the
 * enabling {@code scriptCaptureIndex} machinery). Until then it is partially backstopped by the loop-statement counter.
 */
public class AllocationDefLambdaTests extends AllocationTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        // Add the @allocates test allowlist so static- and instance-method references have controlled estimator targets
        // (AllocationEstimatorTestObject) whose exact per-invocation charge can be asserted, mirroring AllocationLambdaTests.
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    public void testDefStaticLambdaBodyArrayAllocationTrips() {
        // opt is def, so orElseGet is a def call and the lambda's target is def; its body array allocation is charged.
        assertTripsLimit("def opt = Optional.empty(); return opt.orElseGet(() -> { return new int[1000000]; });", "1kb");
    }

    public void testDefStaticLambdaBodyAllocationCounted() {
        // The body allocation reaches the counter, proving the def static lambda body reaches the script instance.
        long bytes = allocatedBytes("def opt = Optional.empty(); opt.orElseGet(() -> { return new int[100]; }); return null;");
        assertTrue("expected the def static lambda body allocation to be counted, but only [" + bytes + "] bytes charged", bytes >= 400);
    }

    public void testBoundedDefStaticLambdaCompletes() {
        // A bounded def static lambda body runs to completion well under the limit.
        Object result = compile(
            "def opt = Optional.empty(); int[] a = (int[]) opt.orElseGet(() -> { return new int[4]; }); return a.length;",
            "1mb"
        ).execute();
        assertEquals(4, result);
    }

    public void testDefConstructorReferenceChargedPerInvocation() {
        // opt is def, so orElseGet is a def call and ArrayList::new is a def constructor reference to an annotated ctor; the
        // per-invocation charge accumulates across the loop and trips.
        assertTripsLimit(
            "def opt = Optional.empty(); for (int i = 0; i < 1000000; ++i) { opt.orElseGet(ArrayList::new); } return 1;",
            "1mb"
        );
    }

    public void testDefInstanceMethodReferenceChargedPerInvocation() {
        // opt is def, so map is a def call and String::toUpperCase is a def (unbound) instance-method reference to an
        // annotated target; each invocation charges the recase allocation, tripping across the loop.
        assertTripsLimit(
            "def opt = Optional.of('abcdefghij'); for (int i = 0; i < 1000000; ++i) { opt.map(String::toUpperCase); } return 1;",
            "1mb"
        );
    }

    public void testDefInstanceMethodReferenceCounted() {
        // constantAllocating charges a fixed 48 per call; two invocations through a def unbound instance-method reference
        // (receiver is the mapped value) are both counted, proving the estimator runs per invocation through def routing.
        long bytes = allocatedBytes(
            "def a = Optional.of(new AllocationEstimatorTestObject()); def b = Optional.of(new AllocationEstimatorTestObject()); "
                + "a.map(AllocationEstimatorTestObject::constantAllocating); "
                + "b.map(AllocationEstimatorTestObject::constantAllocating); return null;"
        );
        assertTrue("expected per-invocation def unbound instance-method-reference charges to be counted, got [" + bytes + "]", bytes >= 96);
    }

    public void testDefStaticMethodReferenceTripsInSingleCall() {
        // opt is def, so map is a def call and staticAllocating is a def static-method reference; its estimator returns
        // 16 * n and the mapped value is large, so one invocation exceeds the limit.
        assertTripsLimit("def opt = Optional.of(1000000); opt.map(AllocationEstimatorTestObject::staticAllocating); return 1;", "1mb");
    }

    public void testDefStaticMethodReferenceCounted() {
        // Two def static-method-reference invocations charge 16 * n each (n = the mapped value), proving the estimator runs
        // with the actual argument on every invocation through def routing.
        long bytes = allocatedBytes(
            "def a = Optional.of(10); def b = Optional.of(20); "
                + "a.map(AllocationEstimatorTestObject::staticAllocating); "
                + "b.map(AllocationEstimatorTestObject::staticAllocating); return null;"
        );
        assertTrue("expected per-invocation def static-method-reference charges to be counted, got [" + bytes + "]", bytes >= 480);
    }

    public void testDefReferenceToUnannotatedTargetCompletes() {
        // A def reference whose target is not annotated is not charge-captured (pre-filter) and resolves normally.
        Object result = compile("def opt = Optional.of('hello'); return opt.map(String::length).get();", "1mb").execute();
        assertEquals(5, result);
    }

    public void testDefBoundInstanceMethodReferenceChargedPerInvocation() {
        // opt is def, so map is a def call and s::concat is a def bound instance-method reference (typed receiver) to an
        // annotated target; the concat allocation is charged per invocation, tripping across the loop.
        assertTripsLimit(
            "String s = 'abcdefghij'; def opt = Optional.of(s); for (int i = 0; i < 1000000; ++i) { opt.map(s::concat); } return 1;",
            "1mb"
        );
    }

    public void testDefBoundInstanceMethodReferenceCounted() {
        // constantAllocating charges a fixed 48 per call through a def bound instance-method reference (captured typed
        // receiver). The script is captured ahead of the receiver and dropped before the delegate runs; two invocations
        // are both counted, proving the bound-receiver capture layout charges the right amount through def routing.
        long bytes = allocatedBytes(
            "AllocationEstimatorTestObject o = new AllocationEstimatorTestObject(); "
                + "def a = Optional.empty(); def b = Optional.empty(); "
                + "a.orElseGet(o::constantAllocating); b.orElseGet(o::constantAllocating); return null;"
        );
        assertTrue("expected per-invocation def bound instance-method-reference charges to be counted, got [" + bytes + "]", bytes >= 96);
    }

    public void testNestedDefStaticLambdaBodyAllocationTrips() {
        // An inner def static lambda inside an outer def static lambda body; the inner's body allocation is charged when
        // both are invoked, confirming the script reaches a nested def lambda.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def inner = Optional.empty(); "
                + "return inner.orElseGet(() -> { return new int[1000000]; }); });",
            "1kb"
        );
    }

    public void testNestedDefConstructorReferenceInLambdaBodyTrips() {
        // A def constructor reference to an annotated target, built and invoked inside an outer def static lambda body:
        // its #scriptThis capture resolves against the outer lambda, and the per-invocation charge trips across the loop.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { "
                + "for (int i = 0; i < 1000000; ++i) { def inner = Optional.empty(); inner.orElseGet(ArrayList::new); } return 1; });",
            "1mb"
        );
    }

    public void testNestedDefStaticMethodReferenceInLambdaBodyTrips() {
        // A def static-method reference (staticAllocating, estimator 16 * n) built and invoked inside an outer def static
        // lambda body: its #scriptThis capture resolves against the outer lambda, and one large-argument call trips.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { "
                + "def inner = Optional.of(1000000); return inner.map(AllocationEstimatorTestObject::staticAllocating); });",
            "1mb"
        );
    }

    public void testNestedDefInstanceMethodReferenceInLambdaBodyTrips() {
        // A def unbound instance-method reference (String::toUpperCase) invoked inside an outer def static lambda body; its
        // #scriptThis capture resolves against the outer lambda and the per-invocation recase charge trips across the loop.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def inner = Optional.of('abcdefghij'); "
                + "for (int i = 0; i < 1000000; ++i) { inner.map(String::toUpperCase); } return 1; });",
            "1mb"
        );
    }

    public void testNestedDefBoundInstanceMethodReferenceInLambdaBodyTrips() {
        // A def bound instance-method reference (s::concat, captured typed receiver) invoked inside an outer def static
        // lambda body; the script capture sits ahead of the receiver capture and is dropped before the delegate runs.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { String s = 'abcdefghij'; def inner = Optional.of(s); "
                + "for (int i = 0; i < 1000000; ++i) { inner.map(s::concat); } return 1; });",
            "1mb"
        );
    }

    public void testTripleNestedDefStaticLambdaBodyAllocationTrips() {
        // Depth 3: outer -> middle -> inner def static lambda. #scriptThis must propagate script -> outer -> middle -> inner
        // (not special-cased at depth 2), so the innermost body allocation is charged when all three are invoked.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def mid = Optional.empty(); "
                + "return mid.orElseGet(() -> { def inner = Optional.empty(); "
                + "return inner.orElseGet(() -> { return new int[1000000]; }); }); });",
            "1kb"
        );
    }

    public void testTripleNestedDefConstructorReferenceInLambdaBodyTrips() {
        // Depth 3 with a reference at the leaf: a def constructor reference built inside the innermost of three nested def
        // lambda bodies. Its #scriptThis capture must resolve through two enclosing lambdas; the per-invocation charge trips.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def mid = Optional.empty(); "
                + "return mid.orElseGet(() -> { for (int i = 0; i < 1000000; ++i) { "
                + "def inner = Optional.empty(); inner.orElseGet(ArrayList::new); } return 1; }); });",
            "1mb"
        );
    }

    public void testTripleNestedDefStaticMethodReferenceInLambdaBodyTrips() {
        // Depth 3 with a method reference at the leaf: a def static-method reference invoked inside the innermost of three
        // nested def lambda bodies, proving the charge machinery threads the script through both enclosing lambdas to a ref.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def mid = Optional.empty(); "
                + "return mid.orElseGet(() -> { def inner = Optional.of(1000000); "
                + "return inner.map(AllocationEstimatorTestObject::staticAllocating); }); });",
            "1mb"
        );
    }

    public void testTripleNestedDefInstanceMethodReferenceInLambdaBodyTrips() {
        // Depth 3 with an unbound instance-method reference (String::toUpperCase) at the leaf, inside three nested def lambda
        // bodies; the per-invocation recase charge trips, proving the script threads through both enclosing lambdas to the ref.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def mid = Optional.empty(); "
                + "return mid.orElseGet(() -> { def inner = Optional.of('abcdefghij'); "
                + "for (int i = 0; i < 1000000; ++i) { inner.map(String::toUpperCase); } return 1; }); });",
            "1mb"
        );
    }

    public void testTripleNestedDefBoundInstanceMethodReferenceInLambdaBodyTrips() {
        // Depth 3 with a bound instance-method reference (s::concat, captured receiver) at the leaf, inside three nested def
        // lambda bodies; the script capture sits ahead of the receiver capture and threads through both enclosing lambdas.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def mid = Optional.empty(); "
                + "return mid.orElseGet(() -> { String s = 'abcdefghij'; def inner = Optional.of(s); "
                + "for (int i = 0; i < 1000000; ++i) { inner.map(s::concat); } return 1; }); });",
            "1mb"
        );
    }

    public void testMixedTypedOuterLambdaDefInnerConstructorReferenceTrips() {
        // Mixed typing across a nesting boundary: a typed outer static lambda (captures #scriptThis via the FunctionRef path)
        // whose body builds a def constructor reference (captures via the encoding path). Proves the two capture mechanisms
        // compose — the def ref's script capture resolves against the enclosing typed lambda's synthetic #scriptThis.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { def inner = Optional.empty(); "
                + "for (int i = 0; i < 1000000; ++i) { inner.orElseGet(ArrayList::new); } return 1; });",
            "1mb"
        );
    }

    public void testMixedTypedOuterLambdaDefInnerLambdaTrips() {
        // Mixed typing: a typed outer static lambda whose body contains a def inner lambda; the def lambda's script capture
        // resolves against the enclosing typed lambda's synthetic #scriptThis, so the innermost body allocation is charged.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { def inner = Optional.empty(); "
                + "return inner.orElseGet(() -> { return new int[1000000]; }); });",
            "1kb"
        );
    }

    public void testMixedDefOuterLambdaTypedInnerConstructorReferenceTrips() {
        // Mixed typing, other direction: a def outer lambda whose body builds a typed constructor reference. The typed ref
        // captures #scriptThis via the FunctionRef path against the enclosing def lambda's synthetic method.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { "
                + "for (int i = 0; i < 1000000; ++i) { Optional.empty().orElseGet(ArrayList::new); } return 1; });",
            "1mb"
        );
    }

    public void testMixedDefOuterLambdaTypedInnerLambdaTrips() {
        // Mixed typing, other direction: a def outer lambda whose body contains a typed inner lambda; the typed inner
        // lambda's #scriptThis resolves against the enclosing def lambda's synthetic method and its body allocation is charged.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { "
                + "return Optional.empty().orElseGet(() -> { return new int[1000000]; }); });",
            "1kb"
        );
    }

    public void testDepth3MixedTypedTypedDefInnerLambdaTrips() {
        // Mixed typing crossed with depth 3: typed outer -> typed middle -> def inner lambda. The def inner's script capture
        // must resolve to #scriptThis threaded through TWO nested typed static lambdas (fix combined with recursion), so the
        // innermost body allocation is charged. Stresses the capture fix at a depth a single enclosing lambda can't reach.
        assertTripsLimit(
            "return Optional.empty().orElseGet(() -> { return Optional.empty().orElseGet(() -> { "
                + "def inner = Optional.empty(); return inner.orElseGet(() -> { return new int[1000000]; }); }); });",
            "1kb"
        );
    }

    public void testMixedTypedOuterLambdaDefInnerLambdaCompletes() {
        // Mixed typing under the limit: a def inner lambda inside a typed outer static lambda runs to completion and returns
        // the correct value, proving the #scriptThis-cast instance capture yields a functionally correct reference (not just
        // one that trips) — the fixed capture does not corrupt the delegate call.
        Object result = compile(
            "int[] a = (int[]) Optional.empty().orElseGet(() -> { def inner = Optional.empty(); "
                + "return inner.orElseGet(() -> { return new int[4]; }); }); return a.length;",
            "1mb"
        ).execute();
        assertEquals(4, result);
    }

    public void testDefReferenceNotChargedWhenTrackingOff() {
        // With tracking off, an annotated def constructor reference is not charge-captured and resolves normally.
        Object result = compile("def opt = Optional.empty(); return ((List) opt.orElseGet(ArrayList::new)).size();", "-1b").execute();
        assertEquals(0, result);
    }
}
