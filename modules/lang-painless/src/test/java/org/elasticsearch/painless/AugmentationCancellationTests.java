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
import org.elasticsearch.painless.spi.annotation.ScriptAwareAnnotation;
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

/**
 * Tests for {@code @script_aware} whitelist augmentations.  These verify that the
 * driver loop inside the augmentation method itself (not just the user's lambda body)
 * polls the script's cancel runnable so iteration over a large collection with a trivial
 * consumer body can still honour search timeouts.
 *
 * Run against {@link ScriptedMetricAggContexts.InitScript} because that context supports
 * cancellation (overrides {@code _getCancellationCheck()}); the same script source against
 * a non-cancellation-aware context still resolves to the script-aware augmentation but finds a
 * {@code null} runnable and delegates to {@link Iterable#forEach}.
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
        // verify @script_aware augmentations still run correctly there (null runnable -> forEach).
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
        return compileInit(source, new HashMap<>(), new HashMap<>());
    }

    private ScriptedMetricAggContexts.InitScript compileInit(String source, Map<String, Object> params, Map<String, Object> state) {
        ScriptedMetricAggContexts.InitScript.Factory factory = scriptEngine.compile(
            "test",
            source,
            ScriptedMetricAggContexts.InitScript.CONTEXT,
            Collections.emptyMap()
        );
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
     * {@code String.replace} is an opaque JDK call whose O(length) cost is invisible to the loop/cancellation
     * budget, so a single replace over a large string can run unbounded.  The script-aware augmentation reimplements
     * it as a scan that polls per match.  Here a single replace over a string with >
     * {@code CANCELLATION_POLL_INTERVAL} literal matches — with no painless loop in the body — fires the runnable
     * purely from replace()'s own internal poll, proving the augmentation (not a loop back-edge) does the polling.
     */
    public void testReplaceAugmentationFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "String s = params['big']; s.replace('A', 'AB');",
            params,
            new HashMap<>()
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-replace");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-replace", ex.getCause().getMessage());
        assertTrue("cancel runnable should fire from inside replace(), was: " + callCount.get(), callCount.get() >= 1);
    }

    /**
     * Same as {@link #testReplaceAugmentationFiresCancelRunnable} but the receiver is {@code def}-typed, so
     * dispatch goes through {@code DefBootstrap}/{@code Def.lookupMethod} (recipe prefixed with 'S') rather than a
     * static invokedynamic.  Verifies the def call-site threads the script into the script-aware replace overload.
     */
    public void testDefReplaceAugmentationFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit("def s = params['big']; s.replace('A', 'AB');", params, new HashMap<>());

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-def-replace");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-def-replace", ex.getCause().getMessage());
        assertTrue("cancel runnable should fire from inside def-dispatched replace(), was: " + callCount.get(), callCount.get() >= 1);
    }

    /**
     * When no cancellation runnable is set, replace takes the fast path and delegates straight to
     * {@link String#replace(CharSequence, CharSequence)}.  Verifies the null-fast-path branch and its result.
     */
    public void testReplaceAugmentationNoRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        Map<String, Object> state = new HashMap<>();
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "String s = params['big']; state['out'] = s.replace('A', 'AB');",
            params,
            state
        );
        // No runnable set — _getCancellationCheck() returns null.
        script.execute();
        assertEquals("A".repeat(1500).replace("A", "AB"), state.get("out"));
    }

    /**
     * With a non-throwing runnable set, replace takes the reimplemented poll-during path rather than delegating to
     * the JDK.  Verify it produces byte-for-byte the same result as {@link String#replace(CharSequence, CharSequence)}
     * across edge cases: empty target (insert around every char), empty receiver, empty replacement, no match,
     * multi-char and overlapping-candidate targets, and the growth case from the threat model.
     */
    public void testReplaceCancellationAwarePathMatchesJdk() {
        String[][] cases = {
            { "AAAAAAA", "A", "AB" },
            { "hello world", "o", "0" },
            { "abc", "", "X" },
            { "", "", "X" },
            { "abcabcabc", "abc", "" },
            { "xyz", "q", "Q" },
            { "banana", "ana", "X" },
            { "mississippi", "ss", "S" }, };
        for (String[] testCase : cases) {
            Map<String, Object> params = new HashMap<>();
            params.put("s", testCase[0]);
            params.put("t", testCase[1]);
            params.put("r", testCase[2]);
            Map<String, Object> state = new HashMap<>();
            ScriptedMetricAggContexts.InitScript script = compileInit(
                "String s = params['s']; String t = params['t']; String r = params['r']; state['out'] = s.replace(t, r);",
                params,
                state
            );
            script._setCancellationCheck(() -> {}); // non-throwing: force the reimplemented path without aborting
            script.execute();
            assertEquals(
                "replace([" + testCase[0] + "],[" + testCase[1] + "],[" + testCase[2] + "])",
                testCase[0].replace(testCase[1], testCase[2]),
                state.get("out")
            );
        }
    }

    /**
     * In a context whose base class does not support cancellation, replace still resolves to the script-aware
     * augmentation (the lookup always binds it), finds a {@code null} runnable, and delegates to the JDK method.
     */
    public void testReplaceInNonCancellationContextRunsCorrectly() {
        Object result = exec("String s = 'AAAAAAA'; return s.replace('A', 'AB');");
        assertEquals("ABABABABABABAB", result);
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
     * Method reference to a {@code @script_aware} augmentation.  {@code FunctionRef.create} detects
     * the annotation and prepends a synthetic {@code PainlessScript} factory capture (matching the
     * augmentation's actual leading parameter type), and the construction site pushes the script
     * receiver via the {@code IRCInstanceCapture} bytecode path.  Pass {@code l::each} as a typed
     * {@code Function<Consumer, Object>} argument so the FunctionRef takes effect; the augmentation
     * polls the runnable from inside its driver loop.
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

    /**
     * Isolates the method-ref augmentation's own poll from the consumer.  The consumer is a method
     * reference to a non-{@code @script_aware} method ({@code out::add}), which is compiled as a
     * plain delegate with no per-entry cancellation poll of its own.  So the only thing that can
     * fire the runnable is {@code each}'s internal per-element {@code _pollCancellation()} — proving
     * the {@code l::each} method reference actually threaded the script into the augmentation, rather
     * than cancellation merely coming from a Painless-lambda consumer's entry poll.
     */
    public void testEachMethodRefAugmentationPollsIndependentlyOfConsumer() {
        StringBuilder source = new StringBuilder("void populate(List l) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" l.add(").append(i).append(");");
        }
        source.append("} ");
        source.append("def apply(Function f, Consumer arg) { return f.apply(arg); } ");
        source.append("List l = new ArrayList(); populate(l); ");
        // out::add is a non-script-aware method ref → no entry poll, so each() is the sole poller.
        source.append("List out = new ArrayList(); apply(l::each, out::add);");

        ScriptedMetricAggContexts.InitScript script = compileInit(source.toString());

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-methodref-each");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-methodref-each", ex.getCause().getMessage());
        assertTrue("each() augmentation poll should fire independently of the consumer", callCount.get() >= 1);
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
     * In a context whose base class does not support cancellation, {@code each} still resolves to the
     * script-aware augmentation (the lookup always binds it), but the augmentation finds a {@code null}
     * cancellation runnable and delegates straight to {@link Iterable#forEach}.  Verifies the call
     * runs and produces the expected result.
     */
    public void testEachInNonCancellationContextRunsCorrectly() {
        Object result = exec(
            "List l = new ArrayList(); l.add(1); l.add(2); l.add(3);" + "List out = new ArrayList(); l.each(x -> out.add(x)); return out;"
        );
        assertEquals(List.of(1, 2, 3), result);
    }

    /**
     * A script-aware {@code each} nested inside a (non-capturing) lambda in a non-cancellation
     * context: because the augmentation always takes the script receiver, the enclosing lambda must
     * capture the script as {@code this} even without {@code supportsCancellation}.  Verifies it
     * compiles (no {@code VerifyError} from {@code loadThis} in a would-be static lambda) and runs.
     */
    public void testScriptAwareCallInsideLambdaInNonCancellationContext() {
        Object result = exec(
            "List l = new ArrayList(); l.add(1);"
                + "List out = new ArrayList();"
                + "l.removeIf(x -> { List inner = new ArrayList(); inner.add(7); inner.each(q -> out.add(q)); return false; });"
                + "return out;"
        );
        assertEquals(List.of(7), result);
    }

    /**
     * Lookup-time guard: {@code @script_aware} on a whitelist line that declares no
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
                new HashMap<>(),
                new HashMap<>()
            )
        );
        assertThat(e.getCause().getMessage(), containsString("requires the whitelist line to declare an augmentation class"));
    }

    /**
     * Lookup-time guard: in a cancellation-supporting context, a {@code @script_aware}
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
            () -> PainlessLookupBuilder.buildFromWhitelists(whitelists, new HashMap<>(), new HashMap<>())
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
        PainlessLookup lookup = PainlessLookupBuilder.buildFromWhitelists(PAINLESS_BASE_WHITELIST, new HashMap<>(), new HashMap<>());
        assertTrue(lookup.hasAnnotationAwareMethod(ScriptAwareAnnotation.class, "each", 1));
        // Arity mismatches can never resolve to each(Consumer): the gate (and thus the push) is skipped.
        assertFalse(lookup.hasAnnotationAwareMethod(ScriptAwareAnnotation.class, "each", 0));
        assertFalse(lookup.hasAnnotationAwareMethod(ScriptAwareAnnotation.class, "each", 2));
        // Unrelated method names are never gated regardless of arity.
        assertFalse(lookup.hasAnnotationAwareMethod(ScriptAwareAnnotation.class, "toString", 0));
    }

    /**
     * Builds a source whose script body fills a 1500-element list via a no-loop helper, then runs
     * {@code body} (an expression that iterates that list inside a lambda). The script's own
     * {@code $cancelPoll} counter never ticks during construction, so the only thing that can fire
     * the runnable is the nested augmentation's per-iteration poll — proving the script receiver
     * was correctly threaded into the augmentation call emitted inside the lambda body.
     */
    private String buildFillThen(String body) {
        StringBuilder source = new StringBuilder("void fill(List m) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" m.add(").append(i).append(");");
        }
        source.append("} List big = new ArrayList(); fill(big); ").append(body).append(";");
        return source.toString();
    }

    /**
     * A cancellation-aware {@code each} nested inside a lambda body still polls the script's cancel
     * runnable.  A lambda that needs the script is compiled as an instance method capturing the
     * script as {@code this}, so the call-site {@code loadThis()} push resolves to the script.
     * Static-typed receiver -> static invoke path inside the lambda.
     */
    public void testStaticEachNestedInLambdaFiresCancelRunnable() {
        // Outer each iterates a single element; its consumer iterates the 1500-element `big` list,
        // whose augmentation poll fires the runnable from inside the lambda.
        ScriptedMetricAggContexts.InitScript script = compileInit(
            buildFillThen("List l = new ArrayList(); l.add(1); l.each(x -> big.each(q -> q.toString()))")
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-nested-static");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-nested-static", ex.getCause().getMessage());
        assertTrue("cancel runnable should fire from the each() nested in the lambda", callCount.get() >= 1);
    }

    /**
     * Same as {@link #testStaticEachNestedInLambdaFiresCancelRunnable} but the nested receiver is
     * {@code def}-typed, so the inner call dispatches through {@code Def.lookupMethod} from inside
     * the lambda.  Verifies the def call-site script-this push works in a lambda body.
     */
    public void testDefEachNestedInLambdaFiresCancelRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            buildFillThen("List l = new ArrayList(); l.add(1); l.each(x -> { def d = big; d.each(q -> q.toString()); })")
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-nested-def");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-nested-def", ex.getCause().getMessage());
        assertTrue("cancel runnable should fire from the def each() nested in the lambda", callCount.get() >= 1);
    }

    /**
     * The adversarial intersection: a typed static lambda (which gets {@link
     * org.elasticsearch.painless.symbol.IRDecorations.IRCStaticCancellationCheck} for its own
     * loop) that <em>also</em> contains a cancellation-aware {@code each} call (which pushes the
     * script receiver). Both rely on the script being reachable; the lambda is realized as an
     * instance method capturing the script as {@code this}, so the loop's {@code _getCancellationCheck}
     * and the each call's {@code loadThis()} both resolve to it.  Verifies the runnable fires from
     * the nested each.
     */
    public void testEachInsideStaticLoopLambdaFiresCancelRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            buildFillThen(
                "List l = new ArrayList(); l.add(1);"
                    + " l.removeIf(x -> { int i = 0; while (i < 5) { i++; } big.each(q -> q.toString()); return false; })"
            )
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-loop-and-each");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-loop-and-each", ex.getCause().getMessage());
        assertTrue(callCount.get() >= 1);
    }

    /**
     * The outer lambda here is <em>def-encoded</em> (it is passed into a {@code def}-dispatched
     * {@code each}, so it has no static functional-interface target), and it contains a
     * cancellation-aware {@code each} call.  A def-encoded lambda takes the {@code targetType == null}
     * path in semantic analysis, but {@code needsScriptCapture} is still forced true in a
     * cancellation-aware context, so it too is realized as an instance method capturing the script
     * as {@code this} — meaning the nested call-site {@code loadThis()} push resolves to the script.
     * Verifies the runnable fires from inside the def-encoded lambda.
     */
    public void testEachNestedInDefEncodedLambdaFiresCancelRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileInit(
            buildFillThen("def l = new ArrayList(); l.add(1); l.each(x -> big.each(q -> q.toString()))")
        );

        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-def-encoded-lambda");
        });

        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-def-encoded-lambda", ex.getCause().getMessage());
        assertTrue("cancel runnable should fire from the each() nested in the def-encoded lambda", callCount.get() >= 1);
    }

    /**
     * Builds a source that declares {@code fill} plus the given user functions (functions must
     * precede statements in Painless), fills a 1500-element {@code big} list, then runs {@code stmts}.
     */
    private ScriptedMetricAggContexts.InitScript compileFillThen(String functions, String stmts) {
        StringBuilder source = new StringBuilder("void fill(List m) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" m.add(").append(i).append(");");
        }
        source.append("} ").append(functions).append(" List big = new ArrayList(); fill(big); ").append(stmts);
        return compileInit(source.toString());
    }

    private static void assertFires(ScriptedMetricAggContexts.InitScript script, String message) {
        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException(message);
        });
        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals(message, ex.getCause().getMessage());
        assertTrue("each() poll should fire", callCount.get() >= 1);
    }

    /** Runs {@code body} with a non-throwing check installed (forcing the polling branch) and returns its value. */
    private Object execWithCheckInstalled(String body, Map<String, Object> params) {
        return execSourceWithCheckInstalled("state['r'] = " + body + ";", params);
    }

    /** Source-level variant of {@link #execWithCheckInstalled} for scripts that declare a function (e.g. method refs). */
    private Object execSourceWithCheckInstalled(String source, Map<String, Object> params) {
        Map<String, Object> state = new HashMap<>();
        ScriptedMetricAggContexts.InitScript script = compileInit(source, params, state);
        script._setCancellationCheck(() -> {});
        script.execute();
        return state.get("r");
    }

    private void assertIndexOfParity(String haystack, String needle) {
        Map<String, Object> params = new HashMap<>();
        params.put("h", haystack);
        params.put("n", needle);
        Object actual = execWithCheckInstalled("((String)params['h']).indexOf((String)params['n'])", params);
        assertEquals(haystack.indexOf(needle), ((Number) actual).intValue());
    }

    private void assertIndexOfParity(String haystack, String needle, int fromIndex) {
        Map<String, Object> params = new HashMap<>();
        params.put("h", haystack);
        params.put("n", needle);
        params.put("f", fromIndex);
        Object actual = execWithCheckInstalled("((String)params['h']).indexOf((String)params['n'], (int)params['f'])", params);
        assertEquals(haystack.indexOf(needle, fromIndex), ((Number) actual).intValue());
    }

    private void assertLastIndexOfParity(String haystack, String needle) {
        Map<String, Object> params = new HashMap<>();
        params.put("h", haystack);
        params.put("n", needle);
        Object actual = execWithCheckInstalled("((String)params['h']).lastIndexOf((String)params['n'])", params);
        assertEquals(haystack.lastIndexOf(needle), ((Number) actual).intValue());
    }

    private void assertLastIndexOfParity(String haystack, String needle, int fromIndex) {
        Map<String, Object> params = new HashMap<>();
        params.put("h", haystack);
        params.put("n", needle);
        params.put("f", fromIndex);
        Object actual = execWithCheckInstalled("((String)params['h']).lastIndexOf((String)params['n'], (int)params['f'])", params);
        assertEquals(haystack.lastIndexOf(needle, fromIndex), ((Number) actual).intValue());
    }

    private void assertContainsParity(String haystack, String needle) {
        Map<String, Object> params = new HashMap<>();
        params.put("h", haystack);
        params.put("n", needle);
        Object actual = execWithCheckInstalled("((String)params['h']).contains((String)params['n'])", params);
        assertEquals(haystack.contains(needle), actual);
    }

    /** A statically-typed {@code each} called inside a user function (an instance method) polls. */
    public void testStaticEachInsideUserFunctionFiresCancelRunnable() {
        assertFires(compileFillThen("void helper(List m) { m.each(q -> q.toString()); }", "helper(big);"), "cancelled-static-userfn");
    }

    /** A {@code def}-dispatched {@code each} called inside a user function polls. */
    public void testDefEachInsideUserFunctionFiresCancelRunnable() {
        assertFires(compileFillThen("void helper(def m) { m.each(q -> q.toString()); }", "helper(big);"), "cancelled-def-userfn");
    }

    /** A bound {@code big::each} method reference constructed inside a lambda body polls. */
    public void testMethodRefEachNestedInLambdaFiresCancelRunnable() {
        assertFires(
            compileFillThen(
                "def apply(Function f, Consumer a) { return f.apply(a); }",
                "List l = new ArrayList(); l.add(1); l.each(x -> apply(big::each, q -> q.toString()));"
            ),
            "cancelled-methodref-in-lambda"
        );
    }

    /** An unbound {@code List::each} method reference (receiver supplied as a SAM argument) polls. */
    public void testUnboundMethodRefEachFiresCancelRunnable() {
        assertFires(
            compileFillThen(
                "def applyBi(BiFunction f, def a, Consumer c) { return f.apply(a, c); }",
                "applyBi(List::each, big, q -> q.toString());"
            ),
            "cancelled-unbound-methodref"
        );
    }

    // --- Iterable script-aware augmentations: per-method fires tests ---

    /** {@code any} with an always-false predicate scans the whole iterable and must poll. */
    public void testAnyAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.any(x -> false);"), "cancelled-any");
    }

    /** {@code every} with an always-true predicate scans the whole iterable and must poll. */
    public void testEveryAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.every(x -> true);"), "cancelled-every");
    }

    /** {@code eachWithIndex} visits every element and must poll. */
    public void testEachWithIndexAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.eachWithIndex((x, i) -> x.toString());"), "cancelled-eachwithindex");
    }

    /** {@code findResults} applies the function to every element and must poll. */
    public void testFindResultsAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.findResults(x -> x.toString());"), "cancelled-findresults");
    }

    /** {@code groupBy} visits every element building a map keyed by the function result and must poll. */
    public void testGroupByAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.groupBy(x -> x % 3);"), "cancelled-groupby");
    }

    /** {@code sum(ToDoubleFunction)} applies the function to every element and must poll. */
    public void testSumWithToDoubleFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.sum(x -> 1.0d);"), "cancelled-sum-fn");
    }

    /** {@code count(Predicate)} scans the whole iterable counting matches and must poll. */
    public void testCountAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.count(x -> true);"), "cancelled-count");
    }

    /**
     * Each new Iterable script-aware augmentation must take the no-poll fast path when the script has
     * no cancellation check installed.  Exercises all seven new methods in one script execution.
     */
    public void testIterableAugmentationsNoRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileFillThen(
            "",
            "big.any(x -> false); "
                + "big.every(x -> true); "
                + "big.eachWithIndex((x, i) -> x.toString()); "
                + "big.findResults(x -> x.toString()); "
                + "big.groupBy(x -> x % 3); "
                + "big.sum(x -> 1.0d); "
                + "big.count(x -> true);"
        );
        // No runnable set — _getCancellationCheck() returns null; fast paths must not throw.
        script.execute();
    }

    // --- Collection script-aware augmentations: per-method fires tests ---

    /** {@code collect(Function)} maps every element and must poll. */
    public void testCollectFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.collect(x -> x.toString());"), "cancelled-collect-fn");
    }

    /** {@code collect(Collection, Function)} maps every element into the given collection and must poll. */
    public void testCollectCollectionFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.collect(new ArrayList(), x -> x.toString());"), "cancelled-collect-coll-fn");
    }

    /** {@code find} with an always-false predicate scans the whole collection and must poll. */
    public void testFindAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.find(x -> false);"), "cancelled-find");
    }

    /** {@code findAll} visits every element and must poll. */
    public void testFindAllAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.findAll(x -> false);"), "cancelled-findall");
    }

    /** {@code findResult(Function)} with an always-null function scans the whole collection and must poll. */
    public void testFindResultFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.findResult(x -> null);"), "cancelled-findresult-fn");
    }

    /** {@code findResult(default, Function)} with an always-null function scans the whole collection and must poll. */
    public void testFindResultDefaultFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.findResult('none', x -> null);"), "cancelled-findresult-default-fn");
    }

    /** {@code split} partitions every element and must poll. */
    public void testSplitAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.split(x -> x % 2 == 0);"), "cancelled-split");
    }

    /**
     * Each new Collection script-aware augmentation must take the no-poll fast path when the script
     * has no cancellation check installed.  Exercises all seven new methods in one script execution.
     */
    public void testCollectionAugmentationsNoRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileFillThen(
            "",
            "big.collect(x -> x.toString()); "
                + "big.collect(new ArrayList(), x -> x.toString()); "
                + "big.find(x -> false); "
                + "big.findAll(x -> false); "
                + "big.findResult(x -> null); "
                + "big.findResult('none', x -> null); "
                + "big.split(x -> x % 2 == 0);"
        );
        // No runnable set — _getCancellationCheck() returns null; fast paths must not throw.
        script.execute();
    }

    // --- Map script-aware augmentations: per-method fires tests ---

    /**
     * Builds a source that declares {@code fill(Map m)} plus the given user functions, populates a
     * 1500-entry {@code big} HashMap via sequential {@code put} statements (so the script body's
     * $cancelPoll never ticks during construction), then runs {@code stmts}.
     */
    private ScriptedMetricAggContexts.InitScript compileFillMapThen(String functions, String stmts) {
        StringBuilder source = new StringBuilder("void fill(Map m) {");
        for (int i = 0; i < 1500; i++) {
            source.append(" m.put(").append(i).append(", ").append(i).append(");");
        }
        source.append("} ").append(functions).append(" Map big = new HashMap(); fill(big); ").append(stmts);
        return compileInit(source.toString());
    }

    /** Map {@code each} visits every entry and must poll. */
    public void testMapEachAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.each((k, v) -> v.toString());"), "cancelled-map-each");
    }

    /** Map {@code collect(BiFunction)} maps every entry and must poll. */
    public void testMapCollectBiFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.collect((k, v) -> v.toString());"), "cancelled-map-collect-fn");
    }

    /** Map {@code collect(Collection, BiFunction)} maps every entry into the given collection and must poll. */
    public void testMapCollectCollectionBiFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.collect(new ArrayList(), (k, v) -> v.toString());"), "cancelled-map-collect-coll-fn");
    }

    /** Map {@code count} visits every entry and must poll. */
    public void testMapCountAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.count((k, v) -> false);"), "cancelled-map-count");
    }

    /** Map {@code every} with an always-true predicate scans the whole map and must poll. */
    public void testMapEveryAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.every((k, v) -> true);"), "cancelled-map-every");
    }

    /** Map {@code find} with an always-false predicate scans the whole map and must poll. */
    public void testMapFindAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.find((k, v) -> false);"), "cancelled-map-find");
    }

    /** Map {@code findAll} visits every entry and must poll. */
    public void testMapFindAllAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.findAll((k, v) -> false);"), "cancelled-map-findall");
    }

    /** Map {@code findResult(BiFunction)} with an always-null function scans the whole map and must poll. */
    public void testMapFindResultBiFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.findResult((k, v) -> null);"), "cancelled-map-findresult-fn");
    }

    /** Map {@code findResult(default, BiFunction)} with an always-null function scans the whole map and must poll. */
    public void testMapFindResultDefaultBiFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.findResult('none', (k, v) -> null);"), "cancelled-map-findresult-default-fn");
    }

    /** Map {@code findResults} applies the function to every entry and must poll. */
    public void testMapFindResultsAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.findResults((k, v) -> v.toString());"), "cancelled-map-findresults");
    }

    /** Map {@code groupBy} visits every entry building a map keyed by the function result and must poll. */
    public void testMapGroupByAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.groupBy((k, v) -> k % 3);"), "cancelled-map-groupby");
    }

    /**
     * Each new Map script-aware augmentation must take the no-poll fast path when the script has no
     * cancellation check installed.  Exercises all eleven new methods in one script execution.
     */
    public void testMapAugmentationsNoRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileFillMapThen(
            "",
            "big.each((k, v) -> v.toString()); "
                + "big.collect((k, v) -> v.toString()); "
                + "big.collect(new ArrayList(), (k, v) -> v.toString()); "
                + "big.count((k, v) -> false); "
                + "big.every((k, v) -> true); "
                + "big.find((k, v) -> false); "
                + "big.findAll((k, v) -> false); "
                + "big.findResult((k, v) -> null); "
                + "big.findResult('none', (k, v) -> null); "
                + "big.findResults((k, v) -> v.toString()); "
                + "big.groupBy((k, v) -> k % 3);"
        );
        // No runnable set — _getCancellationCheck() returns null; fast paths must not throw.
        script.execute();
    }

    // --- Native methods routed through @script_aware Augmentation wrappers ---

    /** {@code Iterable.forEach} visits every element and must poll. */
    public void testIterableForEachAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.forEach(x -> x.toString());"), "cancelled-iterable-foreach");
    }

    /** {@code Collection.removeIf} with an always-false predicate scans the whole collection and must poll. */
    public void testCollectionRemoveIfAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.removeIf(x -> false);"), "cancelled-removeif");
    }

    /** {@code Iterator.forEachRemaining} on a fresh iterator visits every element and must poll. */
    public void testIteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.iterator().forEachRemaining(x -> x.toString());"), "cancelled-iterator-foreachremaining");
    }

    /** {@code List.replaceAll} with an identity operator scans the whole list and must poll. */
    public void testListReplaceAllAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.replaceAll(x -> x);"), "cancelled-list-replaceall");
    }

    /** {@code Map.forEach} visits every entry and must poll. */
    public void testMapForEachAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.forEach((k, v) -> v.toString());"), "cancelled-map-foreach");
    }

    /** {@code Map.replaceAll} with an identity function scans the whole map and must poll. */
    public void testMapReplaceAllAugmentationFiresCancelRunnable() {
        assertFires(compileFillMapThen("", "big.replaceAll((k, v) -> v);"), "cancelled-map-replaceall");
    }

    /** {@code Spliterator.forEachRemaining} on a fresh spliterator visits every element and must poll. */
    public void testSpliteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.spliterator().forEachRemaining(x -> x.toString());"),
            "cancelled-spliterator-foreachremaining"
        );
    }

    /** {@code PrimitiveIterator.OfInt.forEachRemaining} visits every element and must poll. */
    public void testIntPrimitiveIteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToInt(x -> x).iterator().forEachRemaining(x -> x);"),
            "cancelled-int-primitiveiterator-foreachremaining"
        );
    }

    /** {@code PrimitiveIterator.OfLong.forEachRemaining} visits every element and must poll. */
    public void testLongPrimitiveIteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToLong(x -> x).iterator().forEachRemaining(x -> x);"),
            "cancelled-long-primitiveiterator-foreachremaining"
        );
    }

    /** {@code PrimitiveIterator.OfDouble.forEachRemaining} visits every element and must poll. */
    public void testDoublePrimitiveIteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToDouble(x -> x).iterator().forEachRemaining(x -> x);"),
            "cancelled-double-primitiveiterator-foreachremaining"
        );
    }

    /** {@code Spliterator.OfInt.forEachRemaining} visits every element and must poll. */
    public void testIntSpliteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToInt(x -> x).spliterator().forEachRemaining(x -> x);"),
            "cancelled-int-spliterator-foreachremaining"
        );
    }

    /** {@code Spliterator.OfLong.forEachRemaining} visits every element and must poll. */
    public void testLongSpliteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToLong(x -> x).spliterator().forEachRemaining(x -> x);"),
            "cancelled-long-spliterator-foreachremaining"
        );
    }

    /** {@code Spliterator.OfDouble.forEachRemaining} visits every element and must poll. */
    public void testDoubleSpliteratorForEachRemainingAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToDouble(x -> x).spliterator().forEachRemaining(x -> x);"),
            "cancelled-double-spliterator-foreachremaining"
        );
    }

    /**
     * Each new native-wrapper script-aware augmentation must take the no-poll fast path when the
     * script has no cancellation check installed.  Exercises all native wrappers (Iterable,
     * Collection, Iterator, List, Map x2, Spliterator, and the six primitive iterator/spliterator
     * variants) in one script execution.
     */
    public void testNativeWrapperAugmentationsNoRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileFillThen(
            "Map newMap() { Map m = new HashMap(); m.put(1, 1); return m; }",
            "big.forEach(x -> x.toString()); "
                + "big.removeIf(x -> false); "
                + "big.iterator().forEachRemaining(x -> x.toString()); "
                + "big.replaceAll(x -> x); "
                + "Map m = newMap(); m.forEach((k, v) -> v.toString()); "
                + "Map m2 = newMap(); m2.replaceAll((k, v) -> v); "
                + "big.spliterator().forEachRemaining(x -> x.toString()); "
                + "big.stream().mapToInt(x -> x).iterator().forEachRemaining(x -> x); "
                + "big.stream().mapToLong(x -> x).iterator().forEachRemaining(x -> x); "
                + "big.stream().mapToDouble(x -> x).iterator().forEachRemaining(x -> x); "
                + "big.stream().mapToInt(x -> x).spliterator().forEachRemaining(x -> x); "
                + "big.stream().mapToLong(x -> x).spliterator().forEachRemaining(x -> x); "
                + "big.stream().mapToDouble(x -> x).spliterator().forEachRemaining(x -> x);"
        );
        // No runnable set — _getCancellationCheck() returns null; fast paths must not throw.
        script.execute();
    }

    // --- Stream<T> terminal-op script-aware augmentations: per-method fires tests ---

    /** {@code Stream.forEach} drives the pipeline and must poll. */
    public void testStreamForEachAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().forEach(x -> x.toString());"), "cancelled-stream-foreach");
    }

    /** {@code Stream.forEachOrdered} drives the pipeline and must poll. */
    public void testStreamForEachOrderedAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().forEachOrdered(x -> x.toString());"), "cancelled-stream-foreachordered");
    }

    /** {@code Stream.allMatch} with an always-true predicate scans the whole stream and must poll. */
    public void testStreamAllMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().allMatch(x -> true);"), "cancelled-stream-allmatch");
    }

    /** {@code Stream.anyMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testStreamAnyMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().anyMatch(x -> false);"), "cancelled-stream-anymatch");
    }

    /** {@code Stream.noneMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testStreamNoneMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().noneMatch(x -> false);"), "cancelled-stream-nonematch");
    }

    /** {@code Stream.reduce(BinaryOperator)} scans the whole stream and must poll. */
    public void testStreamReduceAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().reduce((a, b) -> a);"), "cancelled-stream-reduce");
    }

    /** {@code Stream.reduce(identity, BinaryOperator)} scans the whole stream and must poll. */
    public void testStreamReduceWithIdentityAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().reduce(0, (a, b) -> a);"), "cancelled-stream-reduce-identity");
    }

    /** {@code Stream.reduce(identity, BiFunction, BinaryOperator)} scans the whole stream and must poll. */
    public void testStreamReduceWithBiFunctionAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().reduce(0, (a, b) -> a, (a, b) -> a);"), "cancelled-stream-reduce-bifunction");
    }

    /** {@code Stream.collect(Supplier, BiConsumer, BiConsumer)} scans the whole stream and must poll. */
    public void testStreamCollectAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().collect(() -> new ArrayList(), (l, x) -> l.add(x), (a, b) -> a.addAll(b));"),
            "cancelled-stream-collect"
        );
    }

    /** {@code Stream.collect(Collector)} drives the whole stream through the collector's accumulator and must poll. */
    public void testStreamCollectCollectorAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().collect(Collectors.toList());"), "cancelled-stream-collect-collector");
    }

    /**
     * {@code Stream.collect(Collector)} with a finishing collector ({@code counting()} is not
     * {@code IDENTITY_FINISH}) still polls — the rebuilt collector applies the original finisher.
     */
    public void testStreamCollectFinishingCollectorAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().collect(Collectors.counting());"), "cancelled-stream-collect-counting");
    }

    // --- IntStream terminal-op script-aware augmentations: per-method fires tests ---

    /** {@code IntStream.forEach} drives the pipeline and must poll. */
    public void testIntStreamForEachAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToInt(x -> x).forEach(x -> x);"), "cancelled-intstream-foreach");
    }

    /** {@code IntStream.forEachOrdered} drives the pipeline and must poll. */
    public void testIntStreamForEachOrderedAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToInt(x -> x).forEachOrdered(x -> x);"), "cancelled-intstream-foreachordered");
    }

    /** {@code IntStream.allMatch} with an always-true predicate scans the whole stream and must poll. */
    public void testIntStreamAllMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToInt(x -> x).allMatch(x -> true);"), "cancelled-intstream-allmatch");
    }

    /** {@code IntStream.anyMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testIntStreamAnyMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToInt(x -> x).anyMatch(x -> false);"), "cancelled-intstream-anymatch");
    }

    /** {@code IntStream.noneMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testIntStreamNoneMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToInt(x -> x).noneMatch(x -> false);"), "cancelled-intstream-nonematch");
    }

    /** {@code IntStream.reduce(IntBinaryOperator)} scans the whole stream and must poll. */
    public void testIntStreamReduceAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToInt(x -> x).reduce((a, b) -> a);"), "cancelled-intstream-reduce");
    }

    /** {@code IntStream.reduce(int, IntBinaryOperator)} scans the whole stream and must poll. */
    public void testIntStreamReduceWithIdentityAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToInt(x -> x).reduce(0, (a, b) -> a);"), "cancelled-intstream-reduce-identity");
    }

    /** {@code IntStream.collect(Supplier, ObjIntConsumer, BiConsumer)} scans the whole stream and must poll. */
    public void testIntStreamCollectAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToInt(x -> x).collect(() -> new ArrayList(), (l, i) -> l.add(i), (a, b) -> a.addAll(b));"),
            "cancelled-intstream-collect"
        );
    }

    // --- LongStream terminal-op script-aware augmentations: per-method fires tests ---

    /** {@code LongStream.forEach} drives the pipeline and must poll. */
    public void testLongStreamForEachAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToLong(x -> x).forEach(x -> x);"), "cancelled-longstream-foreach");
    }

    /** {@code LongStream.forEachOrdered} drives the pipeline and must poll. */
    public void testLongStreamForEachOrderedAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToLong(x -> x).forEachOrdered(x -> x);"), "cancelled-longstream-foreachordered");
    }

    /** {@code LongStream.allMatch} with an always-true predicate scans the whole stream and must poll. */
    public void testLongStreamAllMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToLong(x -> x).allMatch(x -> true);"), "cancelled-longstream-allmatch");
    }

    /** {@code LongStream.anyMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testLongStreamAnyMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToLong(x -> x).anyMatch(x -> false);"), "cancelled-longstream-anymatch");
    }

    /** {@code LongStream.noneMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testLongStreamNoneMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToLong(x -> x).noneMatch(x -> false);"), "cancelled-longstream-nonematch");
    }

    /** {@code LongStream.reduce(LongBinaryOperator)} scans the whole stream and must poll. */
    public void testLongStreamReduceAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToLong(x -> x).reduce((a, b) -> a);"), "cancelled-longstream-reduce");
    }

    /** {@code LongStream.reduce(long, LongBinaryOperator)} scans the whole stream and must poll. */
    public void testLongStreamReduceWithIdentityAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToLong(x -> x).reduce(0L, (a, b) -> a);"), "cancelled-longstream-reduce-identity");
    }

    /** {@code LongStream.collect(Supplier, ObjLongConsumer, BiConsumer)} scans the whole stream and must poll. */
    public void testLongStreamCollectAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen(
                "",
                "big.stream().mapToLong(x -> x).collect(() -> new ArrayList(), (l, v) -> l.add(v), (a, b) -> a.addAll(b));"
            ),
            "cancelled-longstream-collect"
        );
    }

    // --- DoubleStream terminal-op script-aware augmentations: per-method fires tests ---

    /** {@code DoubleStream.forEach} drives the pipeline and must poll. */
    public void testDoubleStreamForEachAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToDouble(x -> x).forEach(x -> x);"), "cancelled-doublestream-foreach");
    }

    /** {@code DoubleStream.forEachOrdered} drives the pipeline and must poll. */
    public void testDoubleStreamForEachOrderedAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToDouble(x -> x).forEachOrdered(x -> x);"),
            "cancelled-doublestream-foreachordered"
        );
    }

    /** {@code DoubleStream.allMatch} with an always-true predicate scans the whole stream and must poll. */
    public void testDoubleStreamAllMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToDouble(x -> x).allMatch(x -> true);"), "cancelled-doublestream-allmatch");
    }

    /** {@code DoubleStream.anyMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testDoubleStreamAnyMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToDouble(x -> x).anyMatch(x -> false);"), "cancelled-doublestream-anymatch");
    }

    /** {@code DoubleStream.noneMatch} with an always-false predicate scans the whole stream and must poll. */
    public void testDoubleStreamNoneMatchAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToDouble(x -> x).noneMatch(x -> false);"), "cancelled-doublestream-nonematch");
    }

    /** {@code DoubleStream.reduce(DoubleBinaryOperator)} scans the whole stream and must poll. */
    public void testDoubleStreamReduceAugmentationFiresCancelRunnable() {
        assertFires(compileFillThen("", "big.stream().mapToDouble(x -> x).reduce((a, b) -> a);"), "cancelled-doublestream-reduce");
    }

    /** {@code DoubleStream.reduce(double, DoubleBinaryOperator)} scans the whole stream and must poll. */
    public void testDoubleStreamReduceWithIdentityAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen("", "big.stream().mapToDouble(x -> x).reduce(0.0d, (a, b) -> a);"),
            "cancelled-doublestream-reduce-identity"
        );
    }

    /** {@code DoubleStream.collect(Supplier, ObjDoubleConsumer, BiConsumer)} scans the whole stream and must poll. */
    public void testDoubleStreamCollectAugmentationFiresCancelRunnable() {
        assertFires(
            compileFillThen(
                "",
                "big.stream().mapToDouble(x -> x).collect(() -> new ArrayList(), (l, v) -> l.add(v), (a, b) -> a.addAll(b));"
            ),
            "cancelled-doublestream-collect"
        );
    }

    /**
     * Each new stream-terminal script-aware augmentation must take the no-poll fast path when the
     * script has no cancellation check installed.  Exercises representative methods across all four
     * stream types in one script execution.
     */
    public void testStreamTerminalAugmentationsNoRunnable() {
        ScriptedMetricAggContexts.InitScript script = compileFillThen(
            "",
            "big.stream().forEach(x -> x.toString()); "
                + "big.stream().allMatch(x -> true); "
                + "big.stream().reduce((a, b) -> a); "
                + "big.stream().reduce(0, (a, b) -> a); "
                + "big.stream().reduce(0, (a, b) -> a, (a, b) -> a); "
                + "big.stream().collect(() -> new ArrayList(), (l, x) -> l.add(x), (a, b) -> a.addAll(b)); "
                + "big.stream().collect(Collectors.toList()); "
                + "big.stream().collect(Collectors.counting()); "
                + "big.stream().mapToInt(x -> x).forEach(x -> x); "
                + "big.stream().mapToInt(x -> x).allMatch(x -> true); "
                + "big.stream().mapToInt(x -> x).reduce((a, b) -> a); "
                + "big.stream().mapToInt(x -> x).reduce(0, (a, b) -> a); "
                + "big.stream().mapToInt(x -> x).collect(() -> new ArrayList(), (l, i) -> l.add(i), (a, b) -> a.addAll(b)); "
                + "big.stream().mapToLong(x -> x).forEach(x -> x); "
                + "big.stream().mapToLong(x -> x).reduce((a, b) -> a); "
                + "big.stream().mapToLong(x -> x).reduce(0L, (a, b) -> a); "
                + "big.stream().mapToLong(x -> x).collect(() -> new ArrayList(), (l, v) -> l.add(v), (a, b) -> a.addAll(b)); "
                + "big.stream().mapToDouble(x -> x).forEach(x -> x); "
                + "big.stream().mapToDouble(x -> x).reduce((a, b) -> a); "
                + "big.stream().mapToDouble(x -> x).reduce(0.0d, (a, b) -> a); "
                + "big.stream().mapToDouble(x -> x).collect(() -> new ArrayList(), (l, v) -> l.add(v), (a, b) -> a.addAll(b));"
        );
        // No runnable set — _getCancellationCheck() returns null; fast paths must not throw.
        script.execute();
    }

    // --- String search augmentations: indexOf / lastIndexOf / contains ---

    /**
     * {@code String.indexOf} with a never-matching needle must scan every candidate position; the augmentation polls between
     * positions so the cancel runnable fires from the scan itself rather than from any script-body back-edge.
     */
    public void testIndexOfAugmentationFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit("String s = params['big']; s.indexOf('Z');", params, new HashMap<>());
        AtomicInteger callCount = new AtomicInteger();
        script._setCancellationCheck(() -> {
            callCount.incrementAndGet();
            throw new RuntimeException("cancelled-indexof");
        });
        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertEquals("cancelled-indexof", ex.getCause().getMessage());
        assertTrue("indexOf scan should fire the cancel runnable", callCount.get() >= 1);
    }

    /** Same as the no-arg case but starting from a non-zero index — the augmentation must still poll. */
    public void testIndexOfWithFromIndexAugmentationFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit("String s = params['big']; s.indexOf('Z', 0);", params, new HashMap<>());
        assertFires(script, "cancelled-indexof-fromindex");
    }

    /** {@code String.lastIndexOf} with a never-matching needle scans backwards through every candidate position and must poll. */
    public void testLastIndexOfAugmentationFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit("String s = params['big']; s.lastIndexOf('Z');", params, new HashMap<>());
        assertFires(script, "cancelled-lastindexof");
    }

    /** Same as the no-arg case but starting from a non-default index. */
    public void testLastIndexOfWithFromIndexAugmentationFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "String s = params['big']; s.lastIndexOf('Z', 1499);",
            params,
            new HashMap<>()
        );
        assertFires(script, "cancelled-lastindexof-fromindex");
    }

    /** {@code String.contains} delegates to indexOf and inherits its polling. */
    public void testContainsAugmentationFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit("String s = params['big']; s.contains('Z');", params, new HashMap<>());
        assertFires(script, "cancelled-contains");
    }

    /** Each new String search augmentation must take the no-poll fast path when no cancellation check is installed. */
    public void testStringSearchAugmentationsNoRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "String s = params['big']; "
                + "s.indexOf('Z'); "
                + "s.indexOf('Z', 0); "
                + "s.lastIndexOf('Z'); "
                + "s.lastIndexOf('Z', 1499); "
                + "s.contains('Z');",
            params,
            new HashMap<>()
        );
        // No runnable set — _getCancellationCheck() returns null; fast paths must not throw.
        script.execute();
    }

    /**
     * The polling scan (taken with a check installed) must return exactly what the JDK returns across the edge cases
     * that diverge most easily: empty needle, negative/out-of-bounds {@code fromIndex}, no match, and surrogate pairs.
     * {@code charAt} matching is code-unit based like {@link String#indexOf}, so a surrogate pair matches only when both
     * halves line up.
     */
    public void testStringSearchAugmentationsMatchJdkWithCheckInstalled() {
        String emoji = "a😀b😀c"; // "a😀b😀c" — two surrogate pairs
        String e = "😀";                    // "😀"

        // indexOf(String)
        assertIndexOfParity(emoji, e);
        assertIndexOfParity("abcabc", "bc");
        assertIndexOfParity("abc", "x");
        assertIndexOfParity("abc", "");

        // indexOf(String, int)
        assertIndexOfParity(emoji, e, 2);
        assertIndexOfParity("abcabc", "bc", -5);  // negative fromIndex clamps to 0
        assertIndexOfParity("abc", "", 1);
        assertIndexOfParity("abc", "", 100);      // empty needle, fromIndex past the end
        assertIndexOfParity("abc", "a", 100);     // non-empty needle, fromIndex past the end
        assertIndexOfParity("ab", "abcd");        // needle longer than haystack -> -1
        assertIndexOfParity("", "a");             // empty haystack -> -1

        // lastIndexOf(String)
        assertLastIndexOfParity(emoji, e);
        assertLastIndexOfParity("abcabc", "bc");
        assertLastIndexOfParity("ab", "abcd");    // needle longer than haystack -> -1

        // lastIndexOf(String, int)
        assertLastIndexOfParity(emoji, e, 3);
        assertLastIndexOfParity("abc", "a", -1);  // negative fromIndex -> -1
        assertLastIndexOfParity("abc", "", -1);   // empty needle + negative fromIndex -> -1
        assertLastIndexOfParity("abc", "", -5);   // empty needle + negative fromIndex -> -1
        assertLastIndexOfParity("abc", "", 1);    // empty needle + in-bounds fromIndex
        assertLastIndexOfParity("abc", "", 100);  // empty needle + fromIndex past the end

        // contains(CharSequence)
        assertContainsParity(emoji, e);
        assertContainsParity("abc", "x");
        assertContainsParity("ab", "abcd");       // needle longer than haystack -> false
    }

    /** A StringBuilder arg (CharSequence, not String) exercises the {@code search.toString()} conversion in the scan. */
    public void testContainsNonStringCharSequenceMatchesJdkWithCheckInstalled() {
        Map<String, Object> params = new HashMap<>();
        params.put("h", "abcabc");
        assertEquals("abcabc".contains("bc"), execWithCheckInstalled("((String)params['h']).contains(new StringBuilder('bc'))", params));
        assertEquals("abcabc".contains("xy"), execWithCheckInstalled("((String)params['h']).contains(new StringBuilder('xy'))", params));
    }

    /** indexOf on a {@code def} receiver takes the runtime dispatch path; the script slot must still be threaded so it polls. */
    public void testIndexOfDefReceiverFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit("def s = params['big']; s.indexOf('Z');", params, new HashMap<>());
        assertFires(script, "cancelled-indexof-def");
    }

    /** Parity on the {@code def} runtime path, guarding the script-slot swap that adapts the handle for a def call site. */
    public void testStringSearchDefReceiverMatchesJdkWithCheckInstalled() {
        Map<String, Object> params = new HashMap<>();
        params.put("h", "abcabc");
        params.put("n", "bc");
        assertEquals("abcabc".indexOf("bc"), ((Number) execWithCheckInstalled("params['h'].indexOf(params['n'])", params)).intValue());
        assertEquals(
            "abcabc".lastIndexOf("bc"),
            ((Number) execWithCheckInstalled("params['h'].lastIndexOf(params['n'])", params)).intValue()
        );
        assertEquals("abcabc".contains("bc"), execWithCheckInstalled("params['h'].contains(params['n'])", params));
    }

    /** The {@code def} string-search dispatch must also take the no-poll fast path when no cancellation check is installed. */
    public void testStringSearchDefReceiverNoRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("big", "A".repeat(50));
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "def s = params['big']; s.indexOf('Z'); s.indexOf('Z', 0); s.lastIndexOf('Z'); s.lastIndexOf('Z', 49); s.contains('Z');",
            params,
            new HashMap<>()
        );
        // No runnable set — fast paths must not throw.
        script.execute();
    }

    // --- CharSequence regex augmentations: regex limit factor (Fix A) + per-match polling (Fix B) ---

    /**
     * Fix A: replaceAll with a regex that exhibits catastrophic backtracking on a moderate-sized input must hit the regex
     * char-read limit factor and throw a CircuitBreakingException instead of running unbounded.  Closes a preexisting
     * protection gap where the JDK Matcher was constructed on the raw receiver.
     */
    public void testReplaceAllRegexLimitFactorTripsCircuitBreaker() {
        Map<String, Object> params = new HashMap<>();
        // Classic catastrophic-backtracking input: "aaaa...X" against /(a+)+b/.
        params.put("input", "a".repeat(40) + "X");
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "params['input'].replaceAll(/(a+)+b/, m -> 'replaced');",
            params,
            new HashMap<>()
        );
        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertThat(ex.getCause().getMessage(), containsString("Regular expression considered too many characters"));
    }

    /** Same protection on replaceFirst. */
    public void testReplaceFirstRegexLimitFactorTripsCircuitBreaker() {
        Map<String, Object> params = new HashMap<>();
        params.put("input", "a".repeat(40) + "X");
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "params['input'].replaceFirst(/(a+)+b/, m -> 'replaced');",
            params,
            new HashMap<>()
        );
        ScriptException ex = expectThrows(ScriptException.class, script::execute);
        assertThat(ex.getCause().getMessage(), containsString("Regular expression considered too many characters"));
    }

    /**
     * Fix B: replaceAll with a regex that matches many times runs the wrapped consumer per match; the per-match cancellation
     * poll inside the augmentation fires the runnable so a long match-and-replace sweep honours the search timeout.
     */
    public void testReplaceAllRegexFiresCancelRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("input", "a".repeat(1500));
        ScriptedMetricAggContexts.InitScript script = compileInit("params['input'].replaceAll(/a/, m -> 'b');", params, new HashMap<>());
        assertFires(script, "cancelled-replaceall-regex");
    }

    /** Fast path for the regex replace augmentations: no runnable installed → must not throw. */
    public void testReplaceAllRegexAndReplaceFirstNoRunnable() {
        Map<String, Object> params = new HashMap<>();
        params.put("input", "abc def abc");
        ScriptedMetricAggContexts.InitScript script = compileInit(
            "String s = params['input']; s.replaceAll(/abc/, m -> 'X'); s.replaceFirst(/abc/, m -> 'X');",
            params,
            new HashMap<>()
        );
        script.execute();  // must not throw
    }

    /** replaceAll's polling branch is a separate loop from the fast path; pin that its output (with groups) matches. */
    public void testReplaceAllWithCheckInstalledMatchesExpectedOutput() {
        Map<String, Object> params = new HashMap<>();
        params.put("input", "the quick brown fox");
        Object out = execWithCheckInstalled(
            "((String)params['input']).replaceAll(/[aeiou]/, m -> m.group().toUpperCase(Locale.ROOT))",
            params
        );
        assertEquals("thE qUIck brOwn fOx", out);
    }

    /**
     * A capturing lambda on a {@code def} receiver stresses argument positioning: the captured value must land in the
     * user-args region after the synthesised script and limit-factor slots.
     */
    public void testReplaceAllDefReceiverCapturingLambdaMatchesExpectedOutput() {
        Map<String, Object> params = new HashMap<>();
        params.put("input", "a1a2a3");
        params.put("prefix", "X");
        Object out = execSourceWithCheckInstalled(
            "String prefix = params['prefix']; state['r'] = params['input'].replaceAll(/a/, m -> prefix + m.group());",
            params
        );
        assertEquals("Xa1Xa2Xa3", out);
    }

    /** A method reference on a {@code def} receiver exercises the method-ref arity math with the script/inject offsets. */
    public void testReplaceAllDefReceiverMethodRefMatchesExpectedOutput() {
        Map<String, Object> params = new HashMap<>();
        params.put("input", "the quick brown fox");
        Object out = execSourceWithCheckInstalled(
            "String up(Matcher m) { return m.group().toUpperCase(Locale.ROOT); } "
                + "state['r'] = params['input'].replaceAll(/[aeiou]/, this::up);",
            params
        );
        assertEquals("thE qUIck brOwn fOx", out);
    }

    /**
     * The polling branch rebuilds the collector, so pin that its output matches the JDK across an
     * {@code IDENTITY_FINISH} collector ({@code toList}), a finishing collector ({@code counting}), and a
     * finishing collector with a non-trivial finisher ({@code joining}).  Receiver is a statically typed
     * {@code Stream}, so this exercises the static augmentation dispatch path.
     */
    public void testStreamCollectCollectorWithCheckInstalledMatchesExpectedOutput() {
        Map<String, Object> params = new HashMap<>();
        params.put("items", List.of("a", "b", "c"));
        assertEquals(
            List.of("a", "b", "c"),
            execSourceWithCheckInstalled("List l = params['items']; state['r'] = l.stream().collect(Collectors.toList());", params)
        );
        assertEquals(
            3L,
            execSourceWithCheckInstalled("List l = params['items']; state['r'] = l.stream().collect(Collectors.counting());", params)
        );
        assertEquals(
            "a,b,c",
            execSourceWithCheckInstalled("List l = params['items']; state['r'] = l.stream().collect(Collectors.joining(','));", params)
        );
    }

    /** A {@code def}-typed stream receiver dispatches the collector wrapper through {@code Def.lookupMethod}; pin its output. */
    public void testStreamCollectCollectorDefReceiverMatchesExpectedOutput() {
        Map<String, Object> params = new HashMap<>();
        params.put("items", List.of("a", "b", "c"));
        assertEquals(List.of("a", "b", "c"), execWithCheckInstalled("params['items'].stream().collect(Collectors.toList())", params));
    }
}
