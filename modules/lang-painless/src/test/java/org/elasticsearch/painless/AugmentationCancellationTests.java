/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptedMetricAggContexts;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@code @cancellation_aware} whitelist augmentations.  These verify that the
 * driver loop inside the augmentation method itself (not just the user's lambda body)
 * polls the script's cancel runnable so iteration over a large collection with a trivial
 * consumer body can still honour search timeouts.
 *
 * Run against {@link ScriptedMetricAggContexts.InitScript} because that context supports
 * cancellation (overrides {@code _getCancellationCheck()}); the same script source against
 * a non-cancellation-aware context resolves to the non-cancellation overload of the
 * augmentation and runs at native speed.
 */
public class AugmentationCancellationTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(ScriptedMetricAggContexts.InitScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        return contexts;
    }

    /**
     * Builds a Painless source that declares a helper {@code populate} of {@code n} sequential
     * {@code l.add(...)} statements (no loops, so the script body's $cancelPoll counter ticks
     * only once at function entry), calls it once, then runs the given augmentation expression.
     * Designed so the augmentation's own per-iteration poll is the only thing that can fire the
     * runnable for {@code n >= CANCELLATION_POLL_INTERVAL}.
     */
    private static String buildPopulateThenEach(int n, String augmentationCallExpression) {
        StringBuilder source = new StringBuilder("void populate(List l) {");
        for (int i = 0; i < n; i++) {
            source.append(" l.add(").append(i).append(");");
        }
        source.append("} List l = new ArrayList(); populate(l); ").append(augmentationCallExpression).append(";");
        return source.toString();
    }

    private ScriptedMetricAggContexts.InitScript compileInit(String source) {
        ScriptedMetricAggContexts.InitScript.Factory factory = scriptEngine.compile(
            "test",
            source,
            ScriptedMetricAggContexts.InitScript.CONTEXT,
            Collections.emptyMap()
        );
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> state = new HashMap<>();
        return factory.newInstance(params, state);
    }

    /**
     * Calling {@code each} on a collection with a trivial consumer body — too cheap for the
     * existing loop-back-edge poll to ever fire from inside the lambda — still triggers the
     * cancel runnable because the augmentation's own iteration loop polls every 1000 elements.
     */
    public void testEachAugmentationFiresCancelRunnable() {
        // Populate via a helper of N sequential add statements so the script body's $cancelPoll
        // never ticks during construction — only the augmentation's own local poll does, and we
        // assert the runnable fires from there. A loop would tick the script counter and risk
        // tripping the runnable before each() runs.
        ScriptedMetricAggContexts.InitScript script = compileInit(buildPopulateThenEach(1500, "l.each(x -> x.toString())"));

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-each");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-each", ex.getCause().getMessage());
        assertTrue("cancel runnable should fire at least once from inside each(), was: " + callCount.get(), callCount.get() >= 1);
    }

    /**
     * When no cancellation runnable is set, the augmentation must take the fast path and
     * delegate straight to {@link Iterable#forEach} with no poll counter.  Verifies the
     * null-fast-path branch.
     */
    public void testEachAugmentationNoRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(buildPopulateThenEach(1500, "l.each(x -> x.toString())"));
        // No runnable set — _getCancellationCheck() returns null.
        script.execute();  // must not throw
    }

    /**
     * The augmentation pushes the script receiver as a synthetic leading capture.  Verify
     * user-supplied captures still work alongside it.
     */
    /**
     * Same as {@link #testEachAugmentationFiresCancelRunnable} but the receiver is {@code def}-typed
     * so dispatch goes through {@code DefBootstrap.bootstrap} and {@code Def.lookupMethod} rather
     * than a static invokedynamic.  Verifies the def call-site path: the compiler prefixes the
     * recipe with 'S' and pushes the script receiver; the runtime resolves the cancellation-aware
     * overload and passes the script through.
     */
    public void testDefEachAugmentationFiresCancelRunnable() {
        // Same shape as the static variant but `def l` forces dynamic dispatch.
        StringBuilder source = new StringBuilder("void populate(def l) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" l.add(").append(i).append(");");
        }
        source.append("} def l = new ArrayList(); populate(l); l.each(x -> x.toString());");

        ScriptedMetricAggContexts.InitScript script = compileInit(source.toString());

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-def-each");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-def-each", ex.getCause().getMessage());
        assertTrue("cancel runnable should fire from inside def-dispatched each(), was: " + callCount.get(), callCount.get() >= 1);
    }

    /**
     * A def call to a method whose name is NOT in the cancellation-aware set must not push
     * the script receiver.  Verifies the gate works (no spurious aload 0 for unrelated def
     * calls like {@code toString}).
     */
    public void testDefCallToUnrelatedMethodIsUnchanged() {
        ScriptedMetricAggContexts.InitScript script = compileInit("def x = 1; return x.toString();");
        // No runnable, no cancellation expected — just verify the call runs without errors.
        script.execute();
    }

    public void testEachAugmentationWithUserCapture() {
        // User-defined Painless functions must precede statements in the script. Inline the
        // capture declaration into the body the same helper builds.
        StringBuilder source = new StringBuilder("void populate(List l) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" l.add(").append(i).append(");");
        }
        source.append("} int base = 42; List l = new ArrayList(); populate(l); l.each(x -> base + x);");

        ScriptedMetricAggContexts.InitScript script = compileInit(source.toString());

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-with-capture");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-with-capture", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }
}
