/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * End-to-end tests for {@code @allocates} pre-checks: the annotated call's operands are replayed through the estimator
 * and its sanitized result charged before the call executes. Covers the built-in estimators, misbehaving-estimator
 * sanitization, and load-time failures for badly declared estimators.
 */
public class AllocationEstimatorTests extends AllocationTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }

    public void testSubstringChargedFromArguments() {
        long expected = AllocationEstimators.substringBytes("hello world", 0, 5);
        assertEquals(expected, allocatedBytes("String s = \"hello world\"; s.substring(0, 5); return \"x\";"));
    }

    public void testSubstringChargeVariesWithArguments() {
        // Same method, different arguments, different charge.
        long expected = AllocationEstimators.substringBytes("hello world", 0, 11);
        assertEquals(expected, allocatedBytes("String s = \"hello world\"; s.substring(0, 11); return \"x\";"));
    }

    public void testSubstringTripsLimit() {
        assertTripsLimit("String s = \"hello world\"; s.substring(0, 5); return \"x\";");
    }

    public void testConstructorChargedFromArguments() {
        // Inner new ArrayList() charges its constant 40; the outer copy constructor charges via the estimator.
        long expected = 40L + AllocationEstimators.arrayListCollectionBytes(new ArrayList<>());
        assertEquals(expected, allocatedBytes("new ArrayList(new ArrayList()); return \"x\";"));
    }

    public void testConstructorArgumentsEvaluatedExactlyOnce() {
        // The dynamic-constructor emission reorders evaluation (args before NEW); side effects must happen exactly once.
        PainlessTestScript script = compile(
            "List once(List l) { l.add(1); return l; } List src = new ArrayList(); new ArrayList(once(src)); return src.size();",
            "1mb"
        );
        assertEquals(1, script.execute());
    }

    public void testNegativeEstimateClampsToZero() {
        // A negative estimate (an estimator bug) charges nothing rather than crediting the running total.
        assertEquals(0L, allocatedBytes("AllocationEstimatorTestObject.negativeEstimated(); return \"x\";"));
    }

    public void testHugeEstimateTripsAnyLimit() {
        // Long.MAX_VALUE from an estimator must trip a roomy limit without overflowing the running total.
        PainlessTestScript script = compile("AllocationEstimatorTestObject.hugeEstimated(); return \"x\";", "1mb");
        Exception e = expectThrows(Exception.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error, but got: " + e, e);
    }

    public void testAugmentedMethodEstimatorSeesReceiverFirst() {
        // The charge (receiver.length() * 100 + n) proves the estimator saw the receiver and the argument.
        assertEquals(3 * 100L + 7, allocatedBytes("String s = \"abc\"; s.augmentedEstimated(7); return \"x\";"));
    }

    public void testInheritedConstantChargedForStaticTypedCall() {
        // Statically-typed call to an unannotated implementation method; the constant annotation is on the interface and must be
        // charged via the inheritance walk (the direct-call counterpart of the def-dispatch inheritance test).
        assertEquals(
            56L,
            allocatedBytes("AllocationInheritanceObject x = new AllocationInheritanceObject(); x.inheritedConstant(); return \"y\";")
        );
    }

    public void testInheritedDynamicChargedForStaticTypedCall() {
        assertEquals(
            5 * 10L,
            allocatedBytes("AllocationInheritanceObject x = new AllocationInheritanceObject(); x.inheritedDynamic(5); return \"y\";")
        );
    }

    public void testConflictingAnnotationsAcrossWhitelistsRejected() {
        // Two allowlists annotating the same method differently fail via the existing duplicate-entry equivalence rule.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
            whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator"));
            whitelists.add(
                WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.allocation-estimator-conflict")
            );
            PainlessLookupBuilder.buildFromWhitelists(whitelists, new HashMap<>(), new HashMap<>());
        });
        assertThat(e.getCause().getMessage(), containsString("cannot add methods with the same name and arity"));
    }

    public void testMissingEstimatorClassFailsAtLoadTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> loadTestWhitelist("org.elasticsearch.painless.allocation-estimator-missing-class")
        );
        assertThat(e.getCause().getMessage(), containsString("estimator class [org.elasticsearch.painless.DoesNotExist] not found"));
    }

    public void testMissingEstimatorMethodFailsAtLoadTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> loadTestWhitelist("org.elasticsearch.painless.allocation-estimator-missing-method")
        );
        assertThat(e.getCause().getMessage(), containsString("#nope"));
        assertThat(e.getCause().getMessage(), containsString("not found"));
    }

    public void testNonLongEstimatorFailsAtLoadTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> loadTestWhitelist("org.elasticsearch.painless.allocation-estimator-not-long")
        );
        assertThat(e.getCause().getMessage(), containsString("must be public static and return long"));
    }

    private static void loadTestWhitelist(String resource) {
        // Load alongside the base allowlist, as a plugin's would be, so common type names resolve.
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, resource));
        PainlessLookupBuilder.buildFromWhitelists(whitelists, new HashMap<>(), new HashMap<>());
    }
}
