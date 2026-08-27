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
 *
 * Also covers nested lambdas (lambda inside a lambda) and method references inside
 * lambdas, to confirm the script receiver propagates correctly through multiple
 * levels of synthetic indirection.
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

    /**
     * Nested def lambdas: an outer def lambda iterates a def collection, and each invocation
     * dispatches an inner def lambda whose body contains the spinning loop.  Both levels are
     * non-static methods on the script class and share the same {@code $cancelPoll} counter,
     * so the runnable fires regardless of which level the loop lives in.
     */
    public void testNestedDefLambdas() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "def outer = new ArrayList(); outer.add(new ArrayList()); outer.get(0).add(1);"
                + "outer.removeIf(inner -> {"
                + "  inner.removeIf(x -> { int i = 0; while (i < 2000000) { i++; } return false; });"
                + "  return false;"
                + "});"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-nested-def");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-nested-def", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * Typed outer lambda containing a def inner lambda: the inner lambda's receiver is def
     * (since it came out of a def collection), so the inner is def-dispatched while the outer
     * uses {@code LambdaBootstrap}.  Both must propagate the script receiver.
     */
    public void testTypedLambdaContainingDefLambda() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "List outer = new ArrayList(); def inner = new ArrayList(); inner.add(1); outer.add(inner);"
                + "outer.removeIf(item -> {"
                + "  item.removeIf(x -> { int i = 0; while (i < 2000000) { i++; } return false; });"
                + "  return false;"
                + "});"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-typed-outer-def-inner");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-typed-outer-def-inner", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * Def outer lambda containing a typed inner lambda: the inner lambda's body declares its
     * own typed receiver, so the inner is statically dispatched.  Both must still poll.
     */
    public void testDefLambdaContainingTypedLambda() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "def outer = new ArrayList(); outer.add(1);"
                + "outer.removeIf(x -> {"
                + "  List inner = new ArrayList(); inner.add(1);"
                + "  inner.removeIf(y -> { int i = 0; while (i < 2000000) { i++; } return false; });"
                + "  return false;"
                + "});"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-def-outer-typed-inner");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-def-outer-typed-inner", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * Method reference to a user-defined Painless function passed to a def-typed forEach.
     * The user function is itself an instance method on the script class with
     * {@code IRCInstanceCancellationCheck} attached, so the cancellation poll fires on every
     * invocation — not because of any lambda machinery but via the regular function-entry
     * decrement.
     */
    public void testDefMethodReferenceToUserFunction() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "void process(def x) { int i = 0; while (i < 2000000) { i++; } }"
                + "def l = new ArrayList(); l.add(1);"
                + "l.forEach(this::process);"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-def-methodref");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-def-methodref", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * Method reference inside a def lambda body: the outer def lambda is the cancellation-
     * aware wrapper; the inner forEach over a def collection takes a method reference to a
     * user-defined Painless function.  Cancellation fires from the user function's own
     * cancellation poll.
     */
    public void testMethodReferenceInsideDefLambda() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "void process(def x) { int i = 0; while (i < 2000000) { i++; } }"
                + "def outer = new ArrayList(); def inner = new ArrayList(); inner.add(1); outer.add(inner);"
                + "outer.removeIf(item -> { item.forEach(this::process); return false; });"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-methodref-in-def-lambda");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-methodref-in-def-lambda", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * Def lambda whose body calls a user-defined Painless function directly: the lambda
     * itself dispatches via DefBootstrap; the function call inside the lambda body is a
     * normal local-function invocation which carries its own cancellation prologue.
     */
    public void testDefLambdaCallingUserFunction() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "void spin() { int i = 0; while (i < 2000000) { i++; } }"
                + "def l = new ArrayList(); l.add(1);"
                + "l.removeIf(x -> { spin(); return false; });"
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-def-call-user-fn");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-def-call-user-fn", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * Verifies that the cancellation poll fires even when neither lambda has a loop, purely
     * from the per-invocation function-entry decrement.  With many def lambda invocations,
     * the shared {@code $cancelPoll} counter eventually drops to zero and the runnable
     * fires.  This is the case that motivated the entry-time decrement in the first place.
     */
    public void testManyDefLambdaInvocationsTripCounter() {
        StringBuilder source = new StringBuilder("def l = new ArrayList();");
        for (int i = 0; i < 3000; i++) {
            source.append(" l.add(").append(i).append(");");
        }
        source.append(" l.removeIf(x -> { return false; });");

        ScriptedMetricAggContexts.InitScript script = compileInit(source.toString());

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-many-invocations");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-many-invocations", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }
}
