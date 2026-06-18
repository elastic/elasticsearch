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
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for PR 3 compile-time-known allocation pre-checks: {@code new T()}, initialized arrays {@code new T[]{...}},
 * autoboxing, and lambda captures. Each site charges the running counter before allocating and trips the per-context limit
 * (raising an uncatchable {@link PainlessError}, surfaced as a {@link ScriptException}) when the charge exceeds it.
 */
public class AllocationPreCheckTests extends ESTestCase {

    private static final String LIMIT_KEY = "script.painless.max_allocation_bytes.context." + PainlessTestScript.CONTEXT.name + ".limit";

    private static PainlessTestScript compile(String source, String limit) {
        Settings settings = Settings.builder().put(LIMIT_KEY, limit).build();
        PainlessScriptEngine engine = new PainlessScriptEngine(settings, scriptContexts());
        PainlessTestScript.Factory factory = engine.compile("test", source, PainlessTestScript.CONTEXT, Map.of());
        return factory.newInstance(Map.of());
    }

    /** Runs {@code source} under a 1mb limit and returns the running allocation total afterwards. */
    private static long allocatedBytes(String source) {
        PainlessTestScript script = compile(source, "1mb");
        script.execute();
        return ((PainlessScript) script).getAllocBytes();
    }

    /** Asserts that running {@code source} under a 1b limit trips the allocation limit. */
    private static void assertTripsLimit(String source) {
        PainlessTestScript script = compile(source, "1b");
        ScriptException e = expectThrows(ScriptException.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error for [" + source + "], but got: " + e, e);
    }

    public void testInitializedArrayCharged() {
        // new int[]{1,2,3,4} => pad8(16 + 4*4) = 32 bytes.
        assertEquals(AllocSizes.arraySize(int.class, 4), allocatedBytes("int[] a = new int[] {1, 2, 3, 4}; return \"x\";"));
    }

    public void testInitializedArrayTripsLimit() {
        assertTripsLimit("int[] a = new int[] {1, 2, 3}; return \"x\";");
    }

    public void testAutoboxIntCharged() {
        // Boxing an int to Integer (via the def cast) charges 16 bytes.
        assertEquals(AllocSizes.boxSize(Integer.class), allocatedBytes("def o = 5; return \"x\";"));
    }

    public void testAutoboxLongCharged() {
        // Boxing a long to Long charges 24 bytes.
        assertEquals(AllocSizes.boxSize(Long.class), allocatedBytes("def o = 5L; return \"x\";"));
    }

    public void testAutoboxTripsLimit() {
        assertTripsLimit("def o = 5; return \"x\";");
    }

    public void testStringLiteralNotCharged() {
        // A constant-pool string load is not a runtime allocation and must not be charged.
        assertEquals(0L, allocatedBytes("String s = \"literal\"; return \"x\";"));
    }

    public void testLambdaCaptureCharged() {
        // Creating the lambda allocates a capture object, which is charged in the enclosing method.
        assertThat(allocatedBytes("Optional.empty().orElseGet(() -> 1); return \"x\";"), org.hamcrest.Matchers.greaterThan(0L));
    }

    public void testLambdaCaptureTripsLimit() {
        assertTripsLimit("Optional.empty().orElseGet(() -> 1); return \"x\";");
    }

    private static Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.baseWhiteList());
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.test"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }
}
