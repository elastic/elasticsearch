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
 * Tests that def-typed lambdas in cancellation-aware script contexts honour task
 * cancellation.  A def-typed lambda is one whose target functional interface is
 * not statically known — typically because the receiver of the call is {@code def},
 * so dispatch goes through {@code DefBootstrap} instead of {@code LambdaBootstrap}.
 * The compiler reuses the existing {@code needsInstance} synthetic-capture machinery
 * to thread the script receiver through the lambda's captured fields, so the lambda
 * body shares the script's persistent {@code $cancelPoll} counter and can fetch the
 * cancel {@code Runnable} via {@code _getCancellationCheck()}.
 */
public class DefLambdaCancellationTests extends ScriptTestCase {

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
     * Verifies that the cancel runnable is invoked when a loop runs inside a def-typed lambda.
     * The {@code def l} receiver forces {@code l.removeIf(...)} through the dynamic dispatch
     * path, so the lambda is compiled without a {@code TargetType} decoration.
     */
    public void testDefLambdaLoopInvokesCancelRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "def l = new ArrayList(); l.add(1);" + "l.removeIf(x -> { int i = 0; while (i < 2000000) { i++; } return false; });"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled", ex.getCause().getMessage());
        assertTrue("cancel runnable should be called at least once, was: " + callCount.get(), callCount.get() >= 1);
    }

    /**
     * Verifies that a def-typed lambda with a loop runs to completion when no cancellation
     * runnable is set (null).  The null guard in the injected code must not throw.
     */
    public void testDefLambdaLoopNoCancelRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "def l = new ArrayList(); l.add(1); l.add(2);" + "l.removeIf(x -> { int i = 0; while (i < 100) { i++; } return false; });"
        );
        // No runnable set — _getCancellationCheck() returns null.
        script.execute();  // must not throw
    }

    /**
     * Verifies that a def-typed lambda capturing an outer variable (in addition to the
     * synthetic script-receiver capture) works correctly — the user capture remains
     * accessible and the cancellation poll still fires.
     */
    public void testDefLambdaWithUserCaptureAndLoop() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "int base = 10;"
                + "def l = new ArrayList(); l.add(1);"
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
     * Verifies that def-typed lambdas with no inner loops complete without interference
     * from the injected cancel machinery.
     */
    public void testDefLambdaNoLoopNotAffected() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "def l = new ArrayList(); l.add(2); l.add(3);" + "l.removeIf(x -> { return x > 10; });"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(callCount::incrementAndGet);
        script.execute();
    }
}
