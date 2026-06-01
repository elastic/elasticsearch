/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptedMetricAggContexts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

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
        // InitScript supports cancellation; also register the shadow fixture used by the def
        // fallback test so a def receiver can resolve a non-cancellation-aware method named `each`.
        List<Whitelist> initWhitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        initWhitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.cancellation-shadow"));
        contexts.put(ScriptedMetricAggContexts.InitScript.CONTEXT, initWhitelists);
        // PainlessTestScript does NOT support cancellation; used by the inherited exec() helper to
        // verify @cancellation_aware augmentations resolve to the plain (script-less) overload.
        contexts.put(PainlessTestScript.CONTEXT, PAINLESS_BASE_WHITELIST);
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

    /**
     * Method reference to a {@code @cancellation_aware} augmentation.  With the script-first
     * signature, {@code FunctionRef.withSyntheticScriptCapture} prepends the script class at
     * factoryMethodType position 0 — matching the augmentation's first parameter directly —
     * and the construction site pushes the script receiver via the existing
     * {@code IRCInstanceCapture} bytecode path.  Pass {@code list::each} as a Painless local
     * function argument typed {@code Function<Consumer, Object>} so the FunctionRef takes
     * effect; the augmentation still polls the runnable from inside its driver loop.
     */
    public void testEachAugmentationMethodRefFiresCancelRunnable() {
        StringBuilder source = new StringBuilder("void populate(List l) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" l.add(").append(i).append(");");
        }
        source.append("} ");
        // Apply a Function<Consumer, Object> to a typed Consumer so the lambda has a known
        // functional interface target.
        source.append("def apply(Function f, Consumer arg) { return f.apply(arg); } ");
        source.append("List l = new ArrayList(); populate(l); ");
        source.append("apply(l::each, x -> x.toString());");

        ScriptedMetricAggContexts.InitScript script = compileInit(source.toString());

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-methodref");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-methodref", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
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

    /**
     * In a context whose base class does NOT support cancellation ({@link PainlessTestScript}),
     * the {@code @cancellation_aware} annotation is dropped during lookup and {@code each} resolves
     * to the plain {@code each(Iterable, Consumer)} overload.  Verifies the call still runs and
     * produces the expected result with no script receiver involved.
     */
    public void testEachInNonCancellationContextRunsViaPlainOverload() {
        Object result = exec(
            "List l = new ArrayList(); l.add(1); l.add(2); l.add(3);" + "List out = new ArrayList(); l.each(x -> out.add(x)); return out;"
        );
        assertEquals(List.of(1, 2, 3), result);
    }

    /**
     * The "don't add parameters where they aren't needed" guarantee for the static call path:
     * in a non-cancellation context the emitted bytecode must invoke the plain, receiver-first
     * {@code each(Iterable, Consumer)} overload and must not reference {@code PainlessScript}
     * (no synthetic {@code aload 0} / script-first overload).  {@link Debugger} compiles against
     * {@link PainlessTestScript}, which does not support cancellation.
     */
    public void testEachInNonCancellationContextEmitsNoScriptThis() {
        String bytecode = Debugger.toString("List l = new ArrayList(); l.add(1); l.each(x -> x.toString());");
        assertThat(bytecode, containsString("each (Ljava/lang/Iterable;Ljava/util/function/Consumer;)"));
        // The generated script class is itself named PainlessScript$Script, so assert specifically
        // that the script-first augmentation overload (leading PainlessScript parameter) is absent.
        assertThat(bytecode, not(containsString("each (Lorg/elasticsearch/painless/PainlessScript;")));
    }

    /**
     * Lookup-time guard: {@code @cancellation_aware} on a whitelist line that declares no
     * augmentation class is rejected, because the annotation can only redirect to a script-first
     * augmentation overload.
     */
    public void testCancellationAwareWithoutAugmentationClassFailsLookup() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PainlessLookupBuilder.buildFromWhitelists(
                List.of(
                    WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.cancellation-no-augmentation")
                ),
                ScriptedMetricAggContexts.InitScript.class,
                new HashMap<>(),
                new HashMap<>()
            )
        );
        assertThat(e.getCause().getMessage(), containsString("requires the whitelist line to declare an augmentation class"));
    }

    /**
     * Lookup-time guard: in a cancellation-supporting context, a {@code @cancellation_aware}
     * augmentation whose augmentation class lacks the script-first overload (leading
     * {@code PainlessScript} parameter) is rejected with an actionable message.
     */
    public void testCancellationAwareAugmentationMissingScriptOverloadFailsLookup() {
        // Include the base whitelist so primitive/base types (e.g. the int return type) resolve and
        // the augmentation line reaches the reflection lookup where the missing-overload guard fires.
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(
            WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.cancellation-missing-overload")
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PainlessLookupBuilder.buildFromWhitelists(
                whitelists,
                ScriptedMetricAggContexts.InitScript.class,
                new HashMap<>(),
                new HashMap<>()
            )
        );
        assertThat(e.getCause().getMessage(), containsString("with a leading [org.elasticsearch.painless.PainlessScript] parameter"));
    }

    /**
     * The def-dispatch fallback: {@code each} is in the cancellation-aware name set (the
     * {@code Iterable} augmentation), so a {@code def} call to {@code each} pushes the synthetic
     * script-this slot.  When the runtime receiver resolves {@code each} to an ordinary method
     * that is NOT cancellation-aware (see {@link CancellationShadowTestObject}), {@code
     * Def.lookupMethod} must drop the extra slot so the call still resolves and runs correctly.
     */
    public void testDefShadowingMethodFallsBackToPlainCall() {
        ScriptedMetricAggContexts.InitScript.Factory factory = scriptEngine.compile(
            "test",
            "def x = new CancellationShadowTestObject(); state.result = x.each(41);",
            ScriptedMetricAggContexts.InitScript.CONTEXT,
            Collections.emptyMap()
        );
        Map<String, Object> state = new HashMap<>();
        ScriptedMetricAggContexts.InitScript script = factory.newInstance(new HashMap<>(), state);

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(callCount::incrementAndGet);

        script.execute();  // must not throw despite the name-gated script-this push
        assertEquals(42, ((Number) state.get("result")).intValue());
        // The resolved method is not cancellation-aware, so it never polls the runnable itself.
        assertEquals(0, callCount.get());
    }

    /**
     * The def call-site gate keys on {@code name/arity}, not name alone, so a def call whose
     * argument count cannot match a cancellation-aware overload skips the script-this push at
     * compile time.  {@code each(Consumer)} is the only cancellation-aware augmentation, with
     * user-visible arity 1.
     */
    public void testCancellationAwareGateKeyedOnNameAndArity() {
        PainlessLookup lookup = PainlessLookupBuilder.buildFromWhitelists(
            PAINLESS_BASE_WHITELIST,
            ScriptedMetricAggContexts.InitScript.class,
            new HashMap<>(),
            new HashMap<>()
        );
        assertTrue(lookup.hasCancellationAwareMethod("each", 1));
        // Arity mismatches can never resolve to each(Consumer): the gate (and thus the push) is skipped.
        assertFalse(lookup.hasCancellationAwareMethod("each", 0));
        assertFalse(lookup.hasCancellationAwareMethod("each", 2));
        // Unrelated method names are never gated regardless of arity.
        assertFalse(lookup.hasCancellationAwareMethod("toString", 0));
    }

    /**
     * In a context whose base class does not support cancellation, the {@code @cancellation_aware}
     * annotation is dropped during lookup, so no method key (not even the real {@code each/1}) is
     * registered and every def call skips the push.
     */
    public void testCancellationAwareGateEmptyInNonCancellationContext() {
        PainlessLookup lookup = PainlessLookupBuilder.buildFromWhitelists(
            PAINLESS_BASE_WHITELIST,
            PainlessTestScript.class,
            new HashMap<>(),
            new HashMap<>()
        );
        assertFalse(lookup.hasCancellationAwareMethod("each", 1));
    }
}
