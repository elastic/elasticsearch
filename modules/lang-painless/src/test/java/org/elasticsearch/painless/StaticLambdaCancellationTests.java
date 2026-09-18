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
 * Tests that static lambdas in cancellation-aware script contexts honour task
 * cancellation.  The compiler injects a synthetic {@code #scriptThis} capture
 * carrying the script receiver so that loops inside static lambdas share the
 * script's persistent {@code $cancelPoll} counter and can fetch the cancel
 * {@code Runnable} via {@code _getCancellationCheck()}.
 */
public class StaticLambdaCancellationTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(ScriptedMetricAggContexts.InitScript.CONTEXT, PAINLESS_BASE_WHITELIST);
        return contexts;
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
     * Verifies that the cancel runnable is invoked when a loop runs inside a typed static lambda.
     * Uses {@code removeIf(Predicate)} so the block-body lambda can return a value (satisfying
     * Painless's "all paths escape" requirement) while still running a long inner loop.
     */
    public void testStaticLambdaLoopInvokesCancelRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "List l = new ArrayList(); l.add(1);" + "l.removeIf(x -> { int i = 0; while (i < 2000000) { i++; } return false; });"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled");
        });

        // Painless wraps script exceptions in ScriptException("runtime error", cause).
        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled", ex.getCause().getMessage());
        // The runnable must have been called at least once from inside the lambda.
        assertTrue("cancel runnable should be called at least once, was: " + callCount.get(), callCount.get() >= 1);
    }

    /**
     * Verifies that a static lambda with a loop runs to completion when no cancellation
     * runnable is set (null).  The null guard in the injected code must not throw.
     */
    public void testStaticLambdaLoopNoCancelRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "List l = new ArrayList(); l.add(1); l.add(2);" + "l.removeIf(x -> { int i = 0; while (i < 100) { i++; } return false; });"
        );
        // No runnable set — _getCancellationCheck() returns null.
        script.execute();  // must not throw
    }

    /**
     * Verifies that a static lambda capturing an outer variable (in addition to the synthetic
     * cancel capture) works correctly — the captured user variable remains accessible.
     */
    public void testStaticLambdaWithUserCaptureAndLoop() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "int base = 10;"
                + "List l = new ArrayList(); l.add(1);"
                + "l.removeIf(x -> { int i = 0; while (i < 2000000) { i += base; } return false; });"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-with-capture");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-with-capture", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * Verifies that static lambdas with no inner loops complete without interference
     * from the injected cancel machinery.
     */
    public void testStaticLambdaNoLoopNotAffected() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "List l = new ArrayList(); l.add(2); l.add(3);" + "l.removeIf(x -> { return x > 10; });"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(callCount::incrementAndGet);
        script.execute();
        // Cancel runnable may have been called from the enclosing execute() entry point
        // but the lambda body itself has no loop, so we just verify the script ran cleanly.
    }

    /**
     * Cancellation is accounted at lambda <em>entry</em>, not only at loop back-edges: each
     * invocation of a cancellation-aware lambda decrements the script's persistent {@code $cancelPoll}
     * counter and runs the cancel runnable when it reaches zero. This is why every lambda in a
     * cancellation-aware script captures the script receiver even when it contains no loop. Here a
     * loopless predicate lambda with no cancellation-aware call of its own is driven enough times
     * (via {@code removeIf} over a 1500-element list) that the per-entry decrement trips the
     * runnable.
     */
    public void testLooplessStaticLambdaEntryPollFiresCancelRunnable() {
        StringBuilder source = new StringBuilder("void fill(List m) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" m.add(").append(i).append(");");
        }
        // removeIf is not cancellation-aware and does not poll itself, so the only thing that can
        // fire the runnable is the loopless predicate lambda's own per-entry counter decrement.
        source.append("} List l = new ArrayList(); fill(l); l.removeIf(x -> x > 100000);");

        ScriptedMetricAggContexts.InitScript script = compileInit(source.toString());

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-entry-poll");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-entry-poll", ex.getCause().getMessage());
        assertTrue("entry-time poll should fire from the loopless lambda after enough invocations", callCount.get() >= 1);
    }

    /**
     * A static lambda's loop must keep the legacy max-loop-counter as a fallback when no cancellation
     * runnable is bound — otherwise its loop would have no bound at all. With no runnable set, a loop
     * exceeding the counter (default 1,000,000) must trip the legacy guard rather than run unbounded.
     */
    public void testStaticLambdaLoopFallsBackToLegacyCounterWithoutRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "List l = new ArrayList(); l.add(1);" + "l.removeIf(x -> { int i = 0; while (i < 2000000) { i++; } return false; });"
        );
        // No cancellation runnable set — _getCancellationCheck() returns null, so the persistent poll
        // never fires and the legacy counter must catch the over-long loop.
        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertTrue(
            ex.getCause().getMessage().contains("The maximum number of statements that can be executed in a loop has been reached.")
        );
    }
}
