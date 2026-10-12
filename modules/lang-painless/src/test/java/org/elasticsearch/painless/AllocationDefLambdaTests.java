/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptedMetricAggContexts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
 *   <tr><td>bound instance-method ref (def receiver, def-call target, PR 8.6)</td>
 *       <td>{@link #testDefReceiverBoundInstanceMethodReferenceTrips},
 *           nested {@link #testNestedDefReceiverBoundReferenceInLambdaBodyTrips}</td>
 *       <td>fixed {@link #testDefReceiverBoundInstanceMethodReferenceCounted},
 *           dynamic {@link #testDefReceiverBoundReferenceDynamicEstimatorCounted}</td>
 *       <td>unannotated {@link #testDefReceiverBoundReferenceToUnannotatedTargetCompletes},
 *           off {@link #testDefReceiverBoundReferenceNotChargedWhenTrackingOff}</td></tr>
 *   <tr><td>bound instance-method ref (def receiver, known target type, PR 8.6)</td>
 *       <td>{@link #testTypedTargetDefReceiverBoundReferenceTrips}</td>
 *       <td>{@link #testTypedTargetDefReceiverBoundReferenceCounted}</td>
 *       <td>off {@link #testTypedTargetDefReceiverBoundReferenceNotChargedWhenTrackingOff}</td></tr>
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
 * The bound reference whose receiver is itself {@code def} ({@code def s = obj; s::method}) is charged as of PR 8.6, in both
 * forms: when the enclosing call is also {@code def} (target type unknown, routes through the def-call path) and when the
 * target functional-interface type is known (e.g. {@code Optional.empty().orElseGet(s::method)}, which emits a real
 * {@code REFERENCE} invokedynamic). Both dispatch on the receiver capture, so the script is over-captured as a trailing
 * capture (the receiver type is unknown at compile time, so there is no pre-filter) and the runtime-resolved target is charged
 * only when annotated. See the two def-receiver rows in the table above.
 * <p>
 * The lambda matrix at the end ({@link #testLambdaMatrixRunsAndCharges}, {@link #testLambdaMatrixCaptureShape}) crosses every
 * lambda shape (typed, def, nested and mixed, inside a user function) with every body kind and compile mode, including a
 * cancellation-aware context, since lambda changes have let bugs through more than once.
 */
public class AllocationDefLambdaTests extends AllocationTestCase {

    public void testDefReceiverBoundInstanceMethodReferenceCounted() {
        // PR 8.6: a bound instance-method reference whose receiver is itself def (`def o = ...; o::constantAllocating`).
        // constantAllocating charges a fixed 48 per call; two invocations are both counted. This routes through the dynamic
        // REFERENCE bootstrap (dispatch on the receiver capture) with the script over-captured as a trailing capture, proving
        // the runtime-resolved target is charged even when the receiver type is unknown until runtime.
        long bytes = allocatedBytes(
            "def o = new AllocationEstimatorTestObject(); def a = Optional.empty(); def b = Optional.empty(); "
                + "a.orElseGet(o::constantAllocating); b.orElseGet(o::constantAllocating); return null;"
        );
        assertTrue("expected per-invocation def-receiver bound instance-method-reference charges, got [" + bytes + "]", bytes >= 96);
    }

    public void testDefReceiverBoundInstanceMethodReferenceTrips() {
        // PR 8.6: s is def, so s::concat is a def-receiver bound reference; the per-invocation concat charge accumulates
        // across the loop and trips (this is the form that previously escaped the budget entirely).
        assertTripsLimit(
            "def s = 'abcdefghij'; def opt = Optional.of(s); for (int i = 0; i < 1000000; ++i) { opt.map(s::concat); } return 1;",
            "1mb"
        );
    }

    public void testDefReceiverBoundReferenceToUnannotatedTargetCompletes() {
        // A def-receiver bound reference to an UNANNOTATED target (String.length has no @allocates) is still over-captured at
        // compile time (the receiver type is unknown, so there is no pre-filter), but the runtime resolves no estimator, so
        // the trailing script capture is dropped, nothing is charged, and the call resolves normally. Exercises the
        // estimator-null branch of the dynamic charging path — the one genuinely new drop-without-charge case.
        Object result = compile("def s = 'hello'; def a = Optional.empty(); return a.orElseGet(s::length);", "1mb").execute();
        assertEquals(5, result);
    }

    public void testDefReceiverBoundReferenceDynamicEstimatorCounted() {
        // A def-receiver bound reference to a dynamic-estimator target (dynamicBoxed's estimator returns n * 100, reading the
        // SAM argument). opt carries 5, so the mapped call charges 500 — proving the estimator receives both the receiver and
        // the SAM argument correctly through the receiver-at-0 / script-trailing capture layout, not just a fixed cost.
        long bytes = allocatedBytes(
            "def o = new AllocationEstimatorTestObject(); def opt = Optional.of(5); opt.map(o::dynamicBoxed); return null;"
        );
        assertTrue(
            "expected the dynamic estimator to charge n*100=500 through the def-receiver bound ref, got [" + bytes + "]",
            bytes >= 500
        );
    }

    public void testDefReceiverBoundReferenceNotChargedWhenTrackingOff() {
        // With tracking off, a def-receiver bound reference is not over-captured or charge-routed and resolves normally.
        Object result = compile("def s = 'hello'; def opt = Optional.of(s); return opt.map(s::concat).get();", "-1b").execute();
        assertEquals("hellohello", result);
    }

    public void testTypedTargetDefReceiverBoundReferenceCounted() {
        // Gap A: a def-receiver bound ref used where the target type IS known — Optional.empty() is typed, so orElseGet gives
        // the ref a known Supplier target while the receiver o stays def. This emits a real REFERENCE invokedynamic (not the
        // def-call path). constantAllocating charges 48; two calls are both counted, proving the real-indy path charges too.
        long bytes = allocatedBytes(
            "def o = new AllocationEstimatorTestObject(); Optional.empty().orElseGet(o::constantAllocating); "
                + "Optional.empty().orElseGet(o::constantAllocating); return null;"
        );
        assertTrue("expected the typed-target def-receiver bound ref to charge, got [" + bytes + "]", bytes >= 96);
    }

    public void testTypedTargetDefReceiverBoundReferenceTrips() {
        // Gap A trip form: hugeAllocatingInstance's estimator returns Long.MAX_VALUE, so one call through the typed-target
        // def-receiver bound ref trips.
        assertTripsLimit(
            "def o = new AllocationEstimatorTestObject(); Optional.empty().orElseGet(o::hugeAllocatingInstance); return 1;",
            "1mb"
        );
    }

    public void testTypedTargetDefReceiverBoundReferenceNotChargedWhenTrackingOff() {
        // Gap A, tracking off: the typed-target def-receiver bound ref is not over-captured or charge-routed and resolves.
        Object result = compile(
            "def o = new AllocationEstimatorTestObject(); return Optional.empty().orElseGet(o::constantAllocating);",
            "-1b"
        ).execute();
        assertEquals(0, result);
    }

    public void testDefReceiverReferenceToScriptAwareMethodCompletesUnderTracking() {
        assertEquals(true, compile("def s = 'abc'; return Optional.of('b').map(s::contains).get();", "1mb").execute());
        assertEquals(true, compile("def s = 'abc'; def o = Optional.of('b'); return o.map(s::contains).get();", "1mb").execute());
        assertEquals(1, compile("def m = ['abc':'b']; m.replaceAll(String::indexOf); return m['abc'];", "1mb").execute());
    }

    public void testScriptAwareAndChargedReferenceCharged() {
        // replace is @script_aware and @allocates: the one pushed script serves the charge and the delegate.
        String setup = "def m = ['b':'X']; ";
        long base = allocatedBytes(setup + "return 'x';");
        long typed = allocatedBytes("String s = 'abc'; " + setup + "m.replaceAll(s::replace); return m['b'];");
        long dynamic = allocatedBytes("def s = 'abc'; " + setup + "m.replaceAll(s::replace); return m['b'];");
        long expected = AllocSizes.captureSize(2) + AllocationEstimators.replaceBytes(null, "abc", "b", "X");

        assertEquals(expected, typed - base);
        assertEquals(expected, dynamic - base);
        assertEquals("aXc", compile("def s = 'abc'; " + setup + "m.replaceAll(s::replace); return m['b'];", "1mb").execute());
    }

    public void testDefReceiverReferenceToTargetWithInjectedConstantCharged() {
        String functions = "String[] split(Function f) { f.apply('a,b,c') } ";
        long base = allocatedBytes(functions + "def p = /,/; return 'x';");
        long withSplit = allocatedBytes(functions + "def p = /,/; split(p::split); return 'x';");
        long expected = AllocSizes.captureSize(2) + AllocationEstimators.patternSplitBytes(Pattern.compile(","), 0, "a,b,c");

        assertEquals(expected, withSplit - base);
    }

    public void testNestedDefReceiverBoundReferenceInLambdaBodyTrips() {
        // A def-receiver bound reference (s is def, s::concat) built and invoked inside an outer def static lambda body: its
        // trailing #scriptThis capture must resolve against the enclosing lambda's synthetic method (not the top-level
        // script), composing the PR 8.6 receiver-at-0/script-trailing layout with nested script propagation.
        assertTripsLimit(
            "def opt = Optional.empty(); return opt.orElseGet(() -> { def s = 'abcdefghij'; def inner = Optional.of(s); "
                + "for (int i = 0; i < 1000000; ++i) { inner.map(s::concat); } return 1; });",
            "1mb"
        );
    }

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

    public void testDefCallToAllocatingMethodInsideTypedLambdaCharged() {
        // A def call gated only by @allocates inside a typed lambda: the lambda captures the script and the call is charged.
        // Without the capture this failed to compile ("no 'this' pointer within static method") whenever tracking was on.
        long withCall = allocatedBytes("def s = 'abcdef'; Optional.of(1).map(x -> s.substring(0, 3)); return 'x';");
        long withoutCall = allocatedBytes("def s = 'abcdef'; Optional.of(1).map(x -> s.length()); return 'x';");
        assertEquals(AllocationEstimators.substringBytes("abcdef", 0, 3), withCall - withoutCall);
    }

    public void testTypedStreamLambdaWithDefCallRunsUnderTracking() {
        // Common ingest shape: a typed list streamed through a lambda whose def parameter calls an annotated method.
        String source = "List l = ['a', 'b']; return l.stream().map(t -> t.toUpperCase()).collect(Collectors.toList());";
        assertEquals(List.of("A", "B"), compile(source, MAX_ALLOCATION_BYTES_DISABLED).execute());
        assertEquals(List.of("A", "B"), compile(source, "1mb").execute());
    }

    public void testUserFunctionWithDefCallRunsUnderTracking() {
        // User functions get the script pointer their own way; this pins that a def call inside one still compiles and runs.
        String source = "def f(def s) { return s.toUpperCase(); } return f('a');";
        assertEquals("A", compile(source, MAX_ALLOCATION_BYTES_DISABLED).execute());
        assertEquals("A", compile(source, "1mb").execute());
    }

    public void testCompoundBracketStoreInsideLambdaRunsUnderTracking() {
        // l[0] += x is a write at semantic time but still loads, so the lambda must capture the script for that read's dispatch.
        String source = "def l = [1, 2]; Optional.of(1).map(x -> { l[0] += x; return x; }); return l[0];";
        assertEquals(2, compile(source, MAX_ALLOCATION_BYTES_DISABLED).execute());
        assertEquals(2, compile(source, "1mb").execute());
    }

    public void testCompoundDotStoreInsideLambdaRunsUnderTracking() {
        // m.value += x on a def map: the load may resolve to a getValue() that needs the script, so the lambda must capture it.
        String source = "def m = ['value': 1]; Optional.of(1).map(x -> { m.value += x; return x; }); return m.value;";
        assertEquals(2, compile(source, MAX_ALLOCATION_BYTES_DISABLED).execute());
        assertEquals(2, compile(source, "1mb").execute());
    }

    public void testCompoundAssignmentShapesInsideTypedLambdasRun() {
        // A typed lambda is static unless something in it needs the script. A compound store on a def target loads first, and
        // that load may push the script, so each shape here must either capture it or not need it, with tracking off and on.
        String[][] cases = {
            { "def l = [1, 2]; Optional.of(1).map(x -> { l[0]++; return x; }); return l[0];", "2" },
            { "def l = [1, 2]; Optional.of(1).map(x -> { ++l[0]; return x; }); return l[0];", "2" },
            { "def l = [1, 2]; return Optional.of(1).map(x -> l[0]++).get() + l[0];", "3" },
            { "def l = [1, 2]; return Optional.of(1).map(x -> (l[0] += x)).get();", "2" },
            { "def m = ['value': 'x']; Optional.of(1).map(x -> { m.value += 'a'; return x; }); return m.value;", "xa" },
            { "def m = ['value': 1]; Optional.of(1).map(x -> { m.value++; return x; }); return m.value;", "2" },
            { "def m = ['k': 1]; Optional.of(1).map(x -> { m['k'] += x; return x; }); return m.k;", "2" },
            { "def m = ['other': 1]; Optional.of(1).map(x -> { m.other += x; return x; }); return m.other;", "2" },
            { "def l = [1]; def i = 0; Optional.of(1).map(x -> { l[i] += x; return x; }); return l[0];", "2" },
            { "def l = [['value': 1]]; Optional.of(1).map(x -> { l[0].value += x; return x; }); return l[0].value;", "2" },
            { "List l = [1]; Optional.of(1).map(x -> { l[0] += x; return x; }); return l[0];", "2" },
            { "int[] a = new int[1]; Optional.of(1).map(x -> { a[0] += x; return x; }); return a[0];", "1" },
            { "def l = [1]; Optional.of(1).map(x -> Optional.of(x).map(y -> { l[0] += y; return y; }).get()); return l[0];", "2" },
            { "def l = [1]; def o = Optional.of(1); o.map(x -> { l[0] += x; return x; }); return l[0];", "2" },
            { "def f(def l) { l[0] += 1; return l[0]; } return f([1]);", "2" },
            { "def f(def m) { m.value += 1; return m.value; } return f(['value': 1]);", "2" }, };
        for (String[] c : cases) {
            assertEquals(c[0], c[1], String.valueOf(compile(c[0], MAX_ALLOCATION_BYTES_DISABLED).execute()));
            assertEquals(c[0], c[1], String.valueOf(compile(c[0], "1mb").execute()));
        }
    }

    // ---- Lambda matrix. Every lambda shape, body kind and compile mode in one table, since the lambda paths have let bugs
    // ---- through more than once. A cell must compile, return the right value and charge exactly the body's cost, and the
    // ---- bytecode must show exactly the expected static and instance lambdas. A second table does the same for every method
    // ---- reference form, at every depth, with the references also passed through user functions as values.

    /** How a cell is compiled: tracking off, a limit, metrics, both, and the same four in a cancellation-aware context. */
    private enum Mode {
        OFF(false, false, false),
        LIMIT(true, false, false),
        METRICS(false, true, false),
        BOTH(true, true, false),
        CANCEL_OFF(false, false, true),
        CANCEL_LIMIT(true, false, true),
        CANCEL_METRICS(false, true, true),
        CANCEL_BOTH(true, true, true);

        final boolean limit;
        final boolean metrics;
        final boolean cancellation;

        Mode(boolean limit, boolean metrics, boolean cancellation) {
            this.limit = limit;
            this.metrics = metrics;
            this.cancellation = cancellation;
        }

        boolean tracking() {
            return limit || metrics;
        }
    }

    /** When a body makes a typed lambda need the script: never, only under tracking, or always. */
    private enum Need {
        NEVER,
        TRACKING,
        ALWAYS
    }

    /**
     * A lambda shape. {@code SETUP} takes the body's setup, {@code BODY} its code and {@code STORE} its code with the final
     * return turned into {@code out.add(...)} for lambdas that cannot return the value. {@code chain} lists the lambdas from
     * the outside in: T typed, D def, F a typed lambda that calls a user function, which always captures the script.
     */
    private record Wrapper(String name, String functions, String setup, String expression, String chain) {}

    /**
     * What a lambda body does. {@code body} and {@code neutral} share {@code setup}, capture the same locals and return the
     * same kind of value, so the charge difference between them is exactly {@code delta}.
     */
    private record Body(
        String name,
        String functions,
        String setup,
        String body,
        String expected,
        String neutral,
        String neutralExpected,
        long delta,
        Need need
    ) {}

    private static final List<Wrapper> WRAPPERS = List.of(
        new Wrapper("typed", "", "SETUP", "Optional.of(1).map(x -> { BODY }).get()", "T"),
        new Wrapper("def", "", "SETUP def o1 = Optional.of(1);", "o1.map(x -> { BODY }).get()", "D"),
        new Wrapper("supplier", "", "SETUP def x = 1;", "Optional.empty().orElseGet(() -> { BODY })", "T"),
        new Wrapper("def supplier", "", "SETUP def x = 1; def o1 = Optional.empty();", "o1.orElseGet(() -> { BODY })", "D"),
        new Wrapper("binary operator", "", "SETUP", "[1].stream().reduce(0, (acc, x) -> { BODY })", "T"),
        new Wrapper("consumer", "", "SETUP List out = new ArrayList(); [1].forEach(x -> { STORE });", "out[0]", "T"),
        new Wrapper("predicate", "", "SETUP List out = new ArrayList(); [1].removeIf(x -> { STORE return false; });", "out[0]", "T"),
        new Wrapper("typed, invoked in a user function", "def fn(Function f) { return f.apply(1); } ", "SETUP", "fn(x -> { BODY })", "T"),
        new Wrapper(
            "typed, returned from a user function",
            "def fr(Function f) { return f; } ",
            "SETUP",
            "fr(x -> { BODY }).apply(1)",
            "T"
        ),
        new Wrapper("typed int parameter", "", "SETUP", "Optional.of(1).map((int x) -> { BODY }).get()", "T"),
        new Wrapper("typed in typed", "", "SETUP", "Optional.of(1).map(x -> Optional.of(x).map(y -> { BODY }).get()).get()", "TT"),
        new Wrapper(
            "def in typed",
            "",
            "SETUP",
            "Optional.of(1).map(x -> { def o2 = Optional.of(x); return o2.map(y -> { BODY }).get(); }).get()",
            "TD"
        ),
        new Wrapper(
            "typed in def",
            "",
            "SETUP def o1 = Optional.of(1);",
            "o1.map(x -> Optional.of(x).map(y -> { BODY }).get()).get()",
            "DT"
        ),
        new Wrapper(
            "def in def",
            "",
            "SETUP def o1 = Optional.of(1);",
            "o1.map(x -> { def o2 = Optional.of(x); return o2.map(y -> { BODY }).get(); }).get()",
            "DD"
        ),
        new Wrapper(
            "typed in def in typed",
            "",
            "SETUP",
            "Optional.of(1).map(x -> { def o2 = Optional.of(x); return o2.map(y -> Optional.of(y).map(z -> { BODY }).get()).get(); }).get()",
            "TDT"
        ),
        new Wrapper(
            "def in typed in def",
            "",
            "SETUP def o1 = Optional.of(1);",
            "o1.map(x -> Optional.of(x).map(y -> { def o3 = Optional.of(y); return o3.map(z -> { BODY }).get(); }).get()).get()",
            "DTD"
        ),
        new Wrapper(
            "typed in user function",
            "def fn(def seed, def params) { SETUP return Optional.of(seed).map(x -> { BODY }).get(); } ",
            "",
            "fn(1, params)",
            "T"
        ),
        new Wrapper(
            "typed in user function in typed",
            "def fn(def seed, def params) { SETUP return Optional.of(seed).map(x -> { BODY }).get(); } ",
            "",
            "Optional.of(1).map(q -> fn(q, params)).get()",
            "FT"
        )
    );

    private static final long SUBSTRING_BYTES = AllocationEstimators.substringBytes("abcdef", 0, 3);
    private static final long KEYWORD_BYTES = AllocationEstimators.termStringBytes(5);
    private static final String HELLO_NEUTRAL = "return params.isEmpty() ? 'x' : 'hello';";
    private static final String TEST_OBJECT = "AllocationEstimatorTestObject";

    /** A charged reference carries the script as one more slot in its capture object; an unannotated target does not. */
    private static final long SCRIPT_CAPTURE_SLOT = AllocSizes.captureSize(1) - AllocSizes.captureSize(0);

    private static final List<Body> BODIES = List.of(
        new Body("plain", "", "", "return x + 1;", "2", "return x;", "1", 0, Need.NEVER),
        new Body(
            "reference to a user function",
            "def g(def s) { return s.substring(0, 3); } def h(def s) { return s; } ",
            "def s = 'abcdef';",
            "return Optional.of(s).map(this::g).get();",
            "abc",
            "return Optional.of(s).map(this::h).get();",
            "abcdef",
            SUBSTRING_BYTES,
            Need.ALWAYS
        ),
        new Body(
            "reference to a user function with no parameters",
            "def g0() { return 'abcdef'.substring(0, 3); } def h0() { return 'abcdef'; } ",
            "",
            "return Optional.empty().orElseGet(this::g0);",
            "abc",
            "return Optional.empty().orElseGet(this::h0);",
            "abcdef",
            SUBSTRING_BYTES,
            Need.ALWAYS
        ),
        new Body(
            "user function call with no parameters",
            "def g0() { return 'abcdef'.substring(0, 3); } def h0() { return 'abcdef'; } ",
            "",
            "return g0();",
            "abc",
            "return h0();",
            "abcdef",
            SUBSTRING_BYTES,
            Need.ALWAYS
        ),
        new Body(
            "static reference",
            "",
            "",
            "return Optional.of(3).map(" + TEST_OBJECT + "::staticAllocating).get();",
            "3",
            "return Optional.of(3).map(Math::abs).get();",
            "3.0",
            AllocationEstimatorTestObject.staticAllocatingEstimate(3) + SCRIPT_CAPTURE_SLOT,
            Need.NEVER
        ),
        new Body(
            "constructor reference",
            "",
            "",
            "return Optional.empty().orElseGet(ArrayList::new).size();",
            "0",
            "return Optional.empty().orElseGet(" + TEST_OBJECT + "::new).zeroAllocating();",
            "0",
            AllocationEstimators.arrayListShellBytes() + SCRIPT_CAPTURE_SLOT,
            Need.NEVER
        ),
        new Body(
            "constructor reference with a parameter",
            "",
            "List src = ['a'];",
            "return Optional.of(src).map(ArrayList::new).get().size();",
            "1",
            "return Optional.of(src).map(List::size).get();",
            "1",
            AllocationEstimators.arrayListCollectionBytes(List.of("a")) + SCRIPT_CAPTURE_SLOT,
            Need.NEVER
        ),
        new Body(
            "array constructor reference",
            "",
            "",
            "return Optional.of(2).map(int[]::new).get().length;",
            "2",
            "return Optional.of(2).map(Math::abs).get();",
            "2.0",
            AllocSizes.arrayBytes(2, 4) + SCRIPT_CAPTURE_SLOT,
            Need.NEVER
        ),
        new Body(
            "bound reference on a def receiver",
            "",
            "def o = new " + TEST_OBJECT + "();",
            "return Optional.empty().orElseGet(o::constantAllocating);",
            "0",
            "return Optional.empty().orElseGet(o::zeroAllocating);",
            "0",
            AllocationEstimatorTestObject.constantEstimate(null),
            Need.NEVER
        ),
        new Body(
            "bound reference on a primitive receiver",
            "",
            "int i = 3;",
            "return Optional.empty().orElseGet(i::toString);",
            "3",
            "return Optional.empty().orElseGet(i::intValue);",
            "3",
            AllocationEstimators.toStringBytes((Object) 3) + SCRIPT_CAPTURE_SLOT,
            Need.NEVER
        ),
        new Body(
            "script-aware reference on a def receiver",
            "",
            "def kw = params.kw;",
            "return Optional.empty().orElseGet(kw::getValue);",
            "hello",
            "return Optional.empty().orElseGet(kw::size);",
            "1",
            KEYWORD_BYTES,
            Need.ALWAYS
        ),
        new Body(
            "script-aware reference on a typed receiver",
            "",
            "String s = 'abc';",
            "return Optional.of('b').map(s::contains).get();",
            "true",
            "return Optional.of('b').map(s::endsWith).get();",
            "false",
            SCRIPT_CAPTURE_SLOT,
            Need.ALWAYS
        ),
        new Body(
            "reference on a loop variable",
            "",
            "def l = [1, 2];",
            "int n = 0; for (def v : l) { n = n + Optional.empty().orElseGet(v::intValue); } return n;",
            "3",
            "int n = 0; for (def v : l) { n = n + Optional.empty().orElseGet(v::hashCode); } return n;",
            "3",
            0,
            Need.NEVER
        ),
        new Body("typed capture", "", "int c = 5;", "return x + c;", "6", "return c + x - 5;", "1", 0, Need.NEVER),
        new Body("def capture", "", "def c = 5;", "return x + c;", "6", "return c + x - 5;", "1", 0, Need.NEVER),
        new Body(
            "typed call, estimator",
            "",
            "String s = 'abcdef';",
            "return s.substring(0, 3);",
            "abc",
            "return s;",
            "abcdef",
            SUBSTRING_BYTES,
            Need.NEVER
        ),
        new Body(
            "def call, estimator",
            "",
            "def s = 'abcdef';",
            "return s.substring(0, 3);",
            "abc",
            "return s;",
            "abcdef",
            SUBSTRING_BYTES,
            Need.TRACKING
        ),
        new Body(
            "def call on a lambda local",
            "",
            "",
            "def s = x + 'abcde'; return s.substring(0, 3);",
            "1ab",
            "def s = x + 'abcde'; return s;",
            "1abcde",
            AllocationEstimators.substringBytes("1abcde", 0, 3),
            Need.TRACKING
        ),
        new Body("def call, script-aware", "", "", "return params.kw.get(0);", "hello", HELLO_NEUTRAL, "hello", KEYWORD_BYTES, Need.ALWAYS),
        new Body(
            "def getter, script-aware",
            "",
            "",
            "return params.kw.value;",
            "hello",
            HELLO_NEUTRAL,
            "hello",
            KEYWORD_BYTES,
            Need.ALWAYS
        ),
        new Body(
            "def getter, estimator",
            "",
            "",
            "return params.geo.lat > 12;",
            "true",
            "return params.isEmpty() == false;",
            "true",
            AllocationEstimators.geoPointReadBytes(null),
            Need.TRACKING
        ),
        new Body("def bracket, script-aware", "", "", "return params.kw[0];", "hello", HELLO_NEUTRAL, "hello", KEYWORD_BYTES, Need.ALWAYS),
        new Body("def bracket on a list", "", "def l = [7];", "return l[0];", "7", "return l.size() * 7;", "7", 0, Need.ALWAYS),
        new Body(
            "post-increment as a value",
            "",
            "def l = [1];",
            "return l[0]++;",
            "1",
            "def t = l[0]; t++; return t - 1;",
            "1",
            0,
            Need.ALWAYS
        ),
        new Body(
            "compound bracket store",
            "",
            "def l = [1, 2];",
            "l[0] += x; return l[0];",
            "2",
            "def t = l[0]; t += x; return t - x;",
            "1",
            0,
            Need.ALWAYS
        ),
        new Body(
            "compound dot store",
            "",
            "def m = ['value': 1];",
            "m.value += x; return m.value;",
            "2",
            "def t = m.value; t += x; return t - x;",
            "1",
            0,
            Need.ALWAYS
        ),
        new Body("new array", "", "", "return new int[4].length;", "4", "return 4;", "4", AllocSizes.arrayBytes(4, 4), Need.NEVER),
        new Body(
            "def concat",
            "",
            "def a = 'ab'; def b = 'cd';",
            "return a + b;",
            "abcd",
            "return a == b ? a : b;",
            "cd",
            AllocSizes.STRING_CONCAT_RESULT_OVERHEAD + AllocSizes.stringConcatOperandBytes("ab") + AllocSizes.stringConcatOperandBytes(
                "cd"
            ),
            Need.NEVER
        ),
        new Body(
            "list literal",
            "",
            "",
            "return ['a', 'b'].size();",
            "2",
            "return 2;",
            "2",
            AllocationEstimators.listLiteralBytes(2),
            Need.NEVER
        ),
        new Body("regex", "", "", "return 'foo' =~ /o/;", "true", "return true;", "true", AllocSizes.matcherBytes(0), Need.NEVER),
        new Body(
            "for-each over def",
            "",
            "def l = [1, 2];",
            "int n = 0; for (def v : l) { n = n + v; } return n;",
            "3",
            "int n = l.size() + 1; return n;",
            "3",
            AllocSizes.ITERATOR_BYTES,
            Need.NEVER
        ),
        new Body(
            "user function call",
            "def g(def s) { return s.substring(0, 3); } ",
            "def s = 'abcdef';",
            "return g(s);",
            "abc",
            "return s;",
            "abcdef",
            SUBSTRING_BYTES,
            Need.ALWAYS
        )
    );

    /**
     * A method reference form. {@code REF} in a tail is where the reference goes. The charged and neutral tails run the same
     * receiver pipeline against a charged and an uncharged target of the same kind, so the charge difference between them is
     * exactly {@code delta}. {@code escape} names the user function that can pass the reference through as a value.
     */
    private record Ref(
        String name,
        String functions,
        String setup,
        String receiver,
        String chargedTail,
        String charged,
        String expected,
        String neutralTail,
        String neutral,
        String neutralExpected,
        long delta,
        String escape
    ) {}

    private static final String USER_FUNCTIONS = "def g(def s) { return s.substring(0, 3); } def h(def s) { return s; } ";
    private static final String USER_FUNCTIONS_NO_PARAMETERS =
        "def g0() { return 'abcdef'.substring(0, 3); } def h0() { return 'abcdef'; } ";

    private static final List<Ref> REFS = List.of(
        new Ref(
            "static",
            "",
            "",
            "Optional.of(3)",
            ".map(REF).get()",
            TEST_OBJECT + "::staticAllocating",
            "3",
            ".map(REF).get()",
            "Math::abs",
            "3.0",
            AllocationEstimatorTestObject.staticAllocatingEstimate(3) + SCRIPT_CAPTURE_SLOT,
            "escapeFunction"
        ),
        new Ref(
            "constructor",
            "",
            "",
            "Optional.empty()",
            ".orElseGet(REF).size()",
            "ArrayList::new",
            "0",
            ".orElseGet(REF).zeroAllocating()",
            TEST_OBJECT + "::new",
            "0",
            AllocationEstimators.arrayListShellBytes() + SCRIPT_CAPTURE_SLOT,
            "escapeSupplier"
        ),
        new Ref(
            "constructor with a parameter",
            "",
            "",
            "Optional.of(['a'])",
            ".map(REF).get().size()",
            "ArrayList::new",
            "1",
            ".map(REF).get()",
            "List::size",
            "1",
            AllocationEstimators.arrayListCollectionBytes(List.of("a")) + SCRIPT_CAPTURE_SLOT,
            "escapeFunction"
        ),
        new Ref(
            "array constructor",
            "",
            "",
            "Optional.of(2)",
            ".map(REF).get().length",
            "int[]::new",
            "2",
            ".map(REF).get()",
            "Math::abs",
            "2.0",
            AllocSizes.arrayBytes(2, 4) + SCRIPT_CAPTURE_SLOT,
            "escapeFunction"
        ),
        new Ref(
            "unbound instance",
            "",
            "",
            "Optional.of(new " + TEST_OBJECT + "())",
            ".map(REF).get()",
            TEST_OBJECT + "::constantAllocating",
            "0",
            ".map(REF).get()",
            TEST_OBJECT + "::zeroAllocating",
            "0",
            AllocationEstimatorTestObject.constantEstimate(null),
            "escapeFunction"
        ),
        new Ref(
            "unbound, script-aware",
            "",
            "",
            "['abc': 'b']",
            ".merge('abc', 'c', REF)",
            "String::indexOf",
            "-1",
            ".merge('abc', 'c', REF)",
            "String::compareTo",
            "-1",
            SCRIPT_CAPTURE_SLOT,
            "escapeBiFunction"
        ),
        new Ref(
            "bound, typed receiver",
            "",
            TEST_OBJECT + " o = new " + TEST_OBJECT + "();",
            "Optional.empty()",
            ".orElseGet(REF)",
            "o::constantAllocating",
            "0",
            ".orElseGet(REF)",
            "o::zeroAllocating",
            "0",
            AllocationEstimatorTestObject.constantEstimate(null),
            "escapeSupplier"
        ),
        new Ref(
            "bound, typed receiver, script-aware",
            "",
            "String s = 'abc';",
            "Optional.of('b')",
            ".map(REF).get()",
            "s::contains",
            "true",
            ".map(REF).get()",
            "s::endsWith",
            "false",
            SCRIPT_CAPTURE_SLOT,
            "escapeFunction"
        ),
        new Ref(
            "bound, primitive receiver",
            "",
            "int i = 3;",
            "Optional.empty()",
            ".orElseGet(REF)",
            "i::toString",
            "3",
            ".orElseGet(REF)",
            "i::intValue",
            "3",
            AllocationEstimators.toStringBytes((Object) 3) + SCRIPT_CAPTURE_SLOT,
            "escapeSupplier"
        ),
        new Ref(
            "bound, def receiver",
            "",
            "def o = new " + TEST_OBJECT + "();",
            "Optional.empty()",
            ".orElseGet(REF)",
            "o::constantAllocating",
            "0",
            ".orElseGet(REF)",
            "o::zeroAllocating",
            "0",
            AllocationEstimatorTestObject.constantEstimate(null),
            "escapeSupplier"
        ),
        new Ref(
            "bound, def receiver, script-aware",
            "",
            "def kw = params.kw;",
            "Optional.empty()",
            ".orElseGet(REF)",
            "kw::getValue",
            "hello",
            ".orElseGet(REF)",
            "kw::size",
            "1",
            KEYWORD_BYTES,
            "escapeSupplier"
        ),
        new Ref(
            "user function",
            USER_FUNCTIONS,
            "",
            "Optional.of('abcdef')",
            ".map(REF).get()",
            "this::g",
            "abc",
            ".map(REF).get()",
            "this::h",
            "abcdef",
            SUBSTRING_BYTES,
            "escapeFunction"
        ),
        new Ref(
            "user function with no parameters",
            USER_FUNCTIONS_NO_PARAMETERS,
            "",
            "Optional.empty()",
            ".orElseGet(REF)",
            "this::g0",
            "abc",
            ".orElseGet(REF)",
            "this::h0",
            "abcdef",
            SUBSTRING_BYTES,
            "escapeSupplier"
        ),
        new Ref(
            "user function holding a lambda",
            "def gl(def s) { return Optional.of(s).map(v -> v.substring(0, 3)).get(); } "
                + "def hl(def s) { return Optional.of(s).map(v -> v).get(); } ",
            "",
            "Optional.of('abcdef')",
            ".map(REF).get()",
            "this::gl",
            "abc",
            ".map(REF).get()",
            "this::hl",
            "abcdef",
            SUBSTRING_BYTES,
            "escapeFunction"
        ),
        new Ref(
            "user function with no parameters holding a supplier lambda",
            "def gz() { return Optional.empty().orElseGet(() -> 'abcdef'.substring(0, 3)); } "
                + "def hz() { return Optional.empty().orElseGet(() -> 'abcdef'); } ",
            "",
            "Optional.empty()",
            ".orElseGet(REF)",
            "this::gz",
            "abc",
            ".orElseGet(REF)",
            "this::hz",
            "abcdef",
            SUBSTRING_BYTES,
            "escapeSupplier"
        ),
        new Ref(
            "user function holding a lambda holding a reference",
            USER_FUNCTIONS
                + "def gll(def s) { return Optional.of(1).map(q -> Optional.of(s).map(this::g).get()).get(); } "
                + "def hll(def s) { return Optional.of(1).map(q -> Optional.of(s).map(this::h).get()).get(); } ",
            "",
            "Optional.of('abcdef')",
            ".map(REF).get()",
            "this::gll",
            "abc",
            ".map(REF).get()",
            "this::hll",
            "abcdef",
            SUBSTRING_BYTES,
            "escapeFunction"
        )
    );

    /**
     * Where a reference is used: on a typed or def receiver, at the top level, inside typed and def lambdas nested either way
     * round, inside supplier lambdas, inside a user function called from a lambda ({@code REFSETUP} takes the form's setup), or
     * passed through a user function as a value ({@code refWrap} wraps the reference, {@code ESCAPE} being the form's function).
     */
    private record RefContext(String name, String functions, String setup, String expression, String refWrap) {}

    private static final String ESCAPE_FUNCTIONS = "def escapeFunction(Function f) { return f; } "
        + "def escapeSupplier(Supplier s) { return s; } "
        + "def escapeBiFunction(BiFunction b) { return b; } ";

    private static final List<RefContext> REF_CONTEXTS = List.of(
        new RefContext("typed receiver", "", "", "RECEIVER TAIL", "REF"),
        new RefContext("def receiver", "", "def r = RECEIVER;", "r TAIL", "REF"),
        new RefContext("typed receiver in typed lambda", "", "", "Optional.of(1).map(q -> RECEIVER TAIL).get()", "REF"),
        new RefContext("def receiver in typed lambda", "", "def r = RECEIVER;", "Optional.of(1).map(q -> r TAIL).get()", "REF"),
        new RefContext("typed receiver in def lambda", "", "def o9 = Optional.of(1);", "o9.map(q -> RECEIVER TAIL).get()", "REF"),
        new RefContext("def receiver in def lambda", "", "def r = RECEIVER; def o9 = Optional.of(1);", "o9.map(q -> r TAIL).get()", "REF"),
        new RefContext("typed receiver in a supplier lambda", "", "", "Optional.empty().orElseGet(() -> RECEIVER TAIL)", "REF"),
        new RefContext(
            "def receiver in a def supplier lambda",
            "",
            "def r = RECEIVER; def o9 = Optional.empty();",
            "o9.orElseGet(() -> r TAIL)",
            "REF"
        ),
        new RefContext(
            "typed receiver in def lambda in typed lambda",
            "",
            "",
            "Optional.of(1).map(p -> { def o8 = Optional.of(p); return o8.map(q -> RECEIVER TAIL).get(); }).get()",
            "REF"
        ),
        new RefContext(
            "def receiver in typed lambda in def lambda",
            "",
            "def r = RECEIVER; def o9 = Optional.of(1);",
            "o9.map(p -> Optional.of(p).map(q -> r TAIL).get()).get()",
            "REF"
        ),
        new RefContext(
            "in a user function called from a typed lambda",
            "def fx(def params) { REFSETUP return RECEIVER TAIL; } ",
            "",
            "Optional.of(1).map(q -> fx(params)).get()",
            "REF"
        ),
        new RefContext("passed through a user function", ESCAPE_FUNCTIONS, "", "RECEIVER TAIL", "ESCAPE(REF)"),
        new RefContext(
            "passed through a user function inside a typed lambda",
            ESCAPE_FUNCTIONS,
            "",
            "Optional.of(1).map(q -> RECEIVER TAIL).get()",
            "ESCAPE(REF)"
        )
    );

    private record Outcome(Object value, long bytes) {}

    private static final String CANCEL_LIMIT_KEY = "script.painless.max_allocation_bytes.context."
        + ScriptedMetricAggContexts.InitScript.CONTEXT.name
        + ".limit";

    /** One engine per mode, serving the test context and the cancellation-aware init script context with the same allowlist. */
    private PainlessScriptEngine matrixEngine(Mode mode) {
        String limit = mode.limit ? "1mb" : MAX_ALLOCATION_BYTES_DISABLED;
        Settings settings = Settings.builder().put(LIMIT_KEY, limit).put(CANCEL_LIMIT_KEY, limit).build();
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>(scriptContexts());
        contexts.put(ScriptedMetricAggContexts.InitScript.CONTEXT, contexts.get(PainlessTestScript.CONTEXT));
        return new PainlessScriptEngine(settings, contexts, () -> AllocationMetrics.NOOP, mode.metrics);
    }

    private static Map<String, Object> matrixParams() throws IOException {
        return Map.of(
            "kw",
            AllocationEsFieldTests.keywordField("hello").toScriptDocValues(),
            "geo",
            AllocationEsFieldTests.geoPointField(12.5, 45.25).toScriptDocValues()
        );
    }

    /** The init script context's execute is void, so a cell there leaves its value in {@code state.r}. */
    private static String matrixResult(Mode mode, String expression) {
        return (mode.cancellation ? "state.r = " : "return ") + expression + ";";
    }

    private static String matrixSource(Mode mode, Wrapper wrapper, Body body, String code) {
        // Every body ends in "return v;", which a lambda that cannot return a value turns into "out.add(v);".
        int last = code.lastIndexOf("return ");
        String stored = code.substring(0, last) + "out.add(" + code.substring(last + "return ".length(), code.length() - 1) + ");";
        String functions = body.functions + wrapper.functions.replace("SETUP", body.setup).replace("BODY", code);
        String setup = wrapper.setup.replace("SETUP", body.setup).replace("BODY", code).replace("STORE", stored);
        return functions + setup + " " + matrixResult(mode, wrapper.expression.replace("BODY", code));
    }

    private static String refSource(Mode mode, RefContext context, Ref ref, boolean charged) {
        String reference = context.refWrap.replace("ESCAPE", ref.escape).replace("REF", charged ? ref.charged : ref.neutral);
        String tail = (charged ? ref.chargedTail : ref.neutralTail).replace("REF", reference);
        boolean setupInFunction = context.functions.contains("REFSETUP");
        String functions = ref.functions + context.functions.replace("REFSETUP", ref.setup)
            .replace("RECEIVER", ref.receiver)
            .replace("TAIL", tail);
        String setup = (setupInFunction ? "" : ref.setup) + " " + context.setup.replace("RECEIVER", ref.receiver);
        String expression = context.expression.replace("RECEIVER", ref.receiver).replace("TAIL", tail);
        return functions + setup + " " + matrixResult(mode, expression);
    }

    private static Outcome runMatrixCell(PainlessScriptEngine engine, Mode mode, String source, Map<String, Object> params, String label) {
        try {
            return runMatrixCell(engine, mode, source, params);
        } catch (RuntimeException e) {
            throw new AssertionError(label, e);
        }
    }

    private static Outcome runMatrixCell(PainlessScriptEngine engine, Mode mode, String source, Map<String, Object> params) {
        if (mode.cancellation) {
            ScriptedMetricAggContexts.InitScript.Factory factory = engine.compile(
                "test",
                source,
                ScriptedMetricAggContexts.InitScript.CONTEXT,
                Map.of()
            );
            ScriptedMetricAggContexts.InitScript script = factory.newInstance(params, new HashMap<>());
            script.execute();
            return new Outcome(((Map<?, ?>) script.getState()).get("r"), ((PainlessScript) script).getAllocBytes());
        }
        PainlessTestScript script = engine.compile("test", source, PainlessTestScript.CONTEXT, Map.of()).newInstance(params);
        return new Outcome(script.execute(), ((PainlessScript) script).getAllocBytes());
    }

    /** Runs a charged source and its neutral twin and records any wrong value or wrong charge difference. */
    private static void checkMatrixCell(
        PainlessScriptEngine engine,
        Mode mode,
        Map<String, Object> params,
        List<String> problems,
        String label,
        String charged,
        String expected,
        String neutral,
        String neutralExpected,
        long delta
    ) {
        Outcome got = runMatrixCell(engine, mode, charged, params, label);
        Outcome base = runMatrixCell(engine, mode, neutral, params, "neutral " + label);
        if (expected.equals(String.valueOf(got.value)) == false) {
            problems.add("value " + label + " expected " + expected + " got " + got.value);
        }
        if (neutralExpected.equals(String.valueOf(base.value)) == false) {
            problems.add("neutral value " + label + " expected " + neutralExpected + " got " + base.value);
        }
        long expectedDelta = mode.tracking() ? delta : 0L;
        if (got.bytes - base.bytes != expectedDelta) {
            problems.add("charge " + label + " expected " + expectedDelta + " got " + (got.bytes - base.bytes));
        }
    }

    private String matrixBytecode(Mode mode, String source) {
        CompilerSettings settings = new CompilerSettings();
        settings.setMaxAllocationBytes(mode.limit ? 1024 * 1024L : -1L);
        settings.setAllocationMetricsEnabled(mode.metrics);
        Class<?> scriptClass = mode.cancellation ? ScriptedMetricAggContexts.InitScript.class : PainlessTestScript.class;
        return Debugger.toString(scriptClass, source, settings, scriptContexts().get(PainlessTestScript.CONTEXT));
    }

    /**
     * How many of a shape's lambdas must be instance methods. A lambda captures the script when the context supports
     * cancellation, when it calls a user function, when it is a def lambda under tracking, or when a need inside it asks for
     * the script, which also makes every lambda around it capture.
     */
    private static int expectedInstanceLambdas(Mode mode, Need need, String chain) {
        boolean asked = need == Need.ALWAYS || (need == Need.TRACKING && mode.tracking());
        int instance = 0;
        for (char kind : chain.toCharArray()) {
            if (mode.cancellation || asked || kind == 'F' || (kind == 'D' && mode.tracking())) {
                instance++;
            }
        }
        return instance;
    }

    private static int count(String text, String part) {
        int n = 0;
        for (int i = text.indexOf(part); i >= 0; i = text.indexOf(part, i + part.length())) {
            n++;
        }
        return n;
    }

    public void testLambdaMatrixRunsAndCharges() throws IOException {
        Map<String, Object> params = matrixParams();
        List<String> problems = new ArrayList<>();
        for (Mode mode : Mode.values()) {
            PainlessScriptEngine engine = matrixEngine(mode);
            for (Wrapper wrapper : WRAPPERS) {
                for (Body body : BODIES) {
                    String charged = matrixSource(mode, wrapper, body, body.body);
                    String neutral = matrixSource(mode, wrapper, body, body.neutral);
                    String label = mode + " / " + wrapper.name + " / " + body.name + ": " + charged;
                    checkMatrixCell(
                        engine,
                        mode,
                        params,
                        problems,
                        label,
                        charged,
                        body.expected,
                        neutral,
                        body.neutralExpected,
                        body.delta
                    );
                }
            }
        }
        assertTrue(problems.size() + " problems:\n" + String.join("\n", problems), problems.isEmpty());
    }

    public void testReferenceMatrixRunsAndCharges() throws IOException {
        Map<String, Object> params = matrixParams();
        List<String> problems = new ArrayList<>();
        for (Mode mode : Mode.values()) {
            PainlessScriptEngine engine = matrixEngine(mode);
            for (RefContext context : REF_CONTEXTS) {
                for (Ref ref : REFS) {
                    String charged = refSource(mode, context, ref, true);
                    String neutral = refSource(mode, context, ref, false);
                    String label = mode + " / " + context.name + " / " + ref.name + ": " + charged;
                    checkMatrixCell(engine, mode, params, problems, label, charged, ref.expected, neutral, ref.neutralExpected, ref.delta);
                }
            }
        }
        assertTrue(problems.size() + " problems:\n" + String.join("\n", problems), problems.isEmpty());
    }

    public void testLambdaMatrixCaptureShape() {
        // Every lambda in a shape must be static or an instance method exactly as expectedInstanceLambdas says. Anything else
        // is an over-capture or an under-capture.
        List<String> problems = new ArrayList<>();
        for (Mode mode : Mode.values()) {
            for (Wrapper wrapper : WRAPPERS) {
                for (Body body : BODIES) {
                    String source = matrixSource(mode, wrapper, body, body.body);
                    String asm = matrixBytecode(mode, source);
                    int instance = count(asm, "public synthetic lambda$");
                    int statics = count(asm, "public static synthetic lambda$");
                    int expected = expectedInstanceLambdas(mode, body.need, wrapper.chain);
                    if (instance != expected || statics != wrapper.chain.length() - expected) {
                        problems.add(
                            mode
                                + " / "
                                + wrapper.name
                                + " / "
                                + body.name
                                + ": expected "
                                + expected
                                + " instance of "
                                + wrapper.chain.length()
                                + " lambdas, got "
                                + instance
                                + " instance and "
                                + statics
                                + " static: "
                                + source
                        );
                    }
                }
            }
        }
        assertTrue(problems.size() + " problems:\n" + String.join("\n", problems), problems.isEmpty());
    }
}
