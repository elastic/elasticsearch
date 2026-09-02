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
 * End-to-end tests for allocation charging on {@code def}-dispatched method calls. Unlike statically-typed calls (charged with
 * emitted bytecode), a {@code def} call resolves its target at runtime, so the charge is applied inside
 * {@code Def.lookupMethod} when the resolved allowlist method carries an {@code @allocates} annotation.
 * The script receiver is threaded to the call site via the same {@code 'S'} recipe the cancellation machinery uses.
 */
public class AllocationDefDispatchTests extends AllocationTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    public void testDefDynamicMethodCharged() {
        // def s.substring(0, 5) resolves to the annotated String.substring(int, int); its estimator is replayed with the
        // def-dispatched operands.
        long expected = AllocationEstimators.substringBytes("hello world", 0, 5);
        assertEquals(expected, allocatedBytes("def s = \"hello world\"; s.substring(0, 5); return \"x\";"));
    }

    public void testDefDynamicChargeVariesWithArguments() {
        // Same call site, different arguments, different charge — proves the estimator sees the actual def-dispatched operands.
        long expected = AllocationEstimators.substringBytes("hello world", 0, 11);
        assertEquals(expected, allocatedBytes("def s = \"hello world\"; s.substring(0, 11); return \"x\";"));
    }

    public void testDefDynamicMethodTripsLimit() {
        assertTripsLimit("def s = \"hello world\"; s.substring(0, 5); return \"x\";");
    }

    public void testDefAugmentedMethodEstimatorSeesReceiverFirst() {
        // An augmented method dispatched via def: the estimator matches the underlying static signature (receiver first).
        // charge = receiver.length() * 100 + n proves both the receiver and the argument reached the estimator.
        assertEquals(3 * 100L + 7, allocatedBytes("def s = \"abc\"; s.augmentedEstimated(7); return \"x\";"));
    }

    public void testDefConstantMethodCharged() {
        // A def call to an @allocates instance method charges the constant; the surrounding new/typed local does not.
        assertEquals(48L, allocatedBytes("def x = new AllocationEstimatorTestObject(); x.constantAllocating(); return \"y\";"));
    }

    public void testDefConstantMethodTripsLimit() {
        assertTripsLimit("def x = new AllocationEstimatorTestObject(); x.constantAllocating(); return \"y\";");
    }

    public void testDefZeroConstantChargesNothing() {
        // @allocates[0] is an audited no-op: the receiver is still pushed but nothing is charged.
        assertEquals(0L, allocatedBytes("def x = new AllocationEstimatorTestObject(); x.zeroAllocating(); return \"y\";"));
    }

    public void testDefUnannotatedMethodChargesNothing() {
        // length() carries no allocation annotation, so the receiver is not pushed and nothing is charged.
        assertEquals(0L, allocatedBytes("def s = \"hello\"; s.length(); return \"x\";"));
    }

    public void testDefSameNameUnannotatedTargetChargesNothing() {
        // "substring"/2 is annotated on String, so the receiver is pushed, but StringBuilder.substring(int, int) is not
        // annotated — resolving to it must charge nothing.
        assertEquals(0L, allocatedBytes("def sb = new StringBuilder(\"hello\"); sb.substring(0, 5); return \"x\";"));
    }

    public void testDefDynamicEstimatorSanitizedAndTrips() {
        // A def call whose estimator returns Long.MAX_VALUE must trip even a roomy limit without overflowing the running total.
        PainlessTestScript script = compile(
            "def x = new AllocationEstimatorTestObject(); x.hugeAllocatingInstance(); return \"y\";",
            "1mb"
        );
        Exception e = expectThrows(Exception.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error, but got: " + e, e);
    }

    public void testDefCallChargesEachInvocation() {
        // The bootstrap caches the charge-wrapped handle as the call-site target, so every dispatch through it charges again.
        long perCall = AllocationEstimators.substringBytes("hello world", 0, 5);
        long total = allocatedBytes("""
            def s = "hello world";
            for (int i = 0; i < 3; ++i) {
                s.substring(0, 5);
            }
            return "x";
            """);
        assertEquals(3 * perCall, total);
    }

    public void testDefConstantBoxedParamMethodCharged() {
        // A boxed (Integer) parameter makes Painless dispatch this via a runtime bridge method; the constant annotation must
        // survive onto that derived bridge and still charge.
        assertEquals(48L, allocatedBytes("def x = new AllocationEstimatorTestObject(); x.constantBoxed(5); return \"y\";"));
    }

    public void testDefDynamicBoxedParamMethodCharged() {
        // Same for the dynamic path: the estimator survives onto the bridge and reads the (Object-widened) boxed argument.
        assertEquals(5 * 100L, allocatedBytes("def x = new AllocationEstimatorTestObject(); x.dynamicBoxed(5); return \"y\";"));
    }

    public void testDefBoxedParamMethodTripsLimit() {
        assertTripsLimit("def x = new AllocationEstimatorTestObject(); x.constantBoxed(5); return \"y\";");
    }

    public void testDefInheritedConstantCharged() {
        // The implementation method is allowlisted unannotated; the annotation is on the interface. Def resolves to the
        // implementation method, so charging must walk to the annotated interface method.
        assertEquals(56L, allocatedBytes("def x = new AllocationInheritanceObject(); x.inheritedConstant(); return \"y\";"));
    }

    public void testDefInheritedDynamicCharged() {
        // Same for the estimator path — the estimator inherited from the interface reads the argument (receiver widened).
        assertEquals(5 * 10L, allocatedBytes("def x = new AllocationInheritanceObject(); x.inheritedDynamic(5); return \"y\";"));
    }

    public void testDefInheritedConstantTripsLimit() {
        assertTripsLimit("def x = new AllocationInheritanceObject(); x.inheritedConstant(); return \"y\";");
    }
}
