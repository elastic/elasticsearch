/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * The census of expressions {@link QueryDslTranslator} synthesizes, and the transport version each one owes.
 * <p>
 * The request-filter rewrite is version-gated as a whole, on ONE constant chosen by hand. The set of functions this
 * translator emits is not fixed — it grows whenever a translation is added — so the constant and the output set drift
 * apart silently, and a node that clears the feature gate but predates the function is sent a plan naming a
 * {@code NamedWriteable} it has no reader for. It answers {@code Unknown NamedWriteable [...]}, which fails the query
 * on a deployed node and kills a node running with assertions enabled. That is not hypothetical: {@code MV_GREATER}
 * and {@code MV_LESS} were taught to {@code range} in one change that also rewrote the rewriter's javadoc to name
 * them, and left the transport version that javadoc cites untouched, with no build going red.
 * <p>
 * The filter is synthesized from a request the user wrote without naming any function, which is why these need a
 * check at all: {@code Versioned} states a version check is NOT required for new language features, because failing
 * on the transport layer is acceptable when a user asks for a function some node lacks. Nobody asked for this one.
 * <p>
 * So every expression class constructed in the translator is declared below as either available on any node that
 * clears the rewrite's own gate, or as carrying its own pin consulted before it is built. A new construction site
 * fails this test until its class is declared.
 * <p>
 * Known limits, deliberate. The census reads the CONSTRUCTION SITES out of the source rather than the translator's
 * output: a test over the output only covers the shapes someone wrote a case for, and the emit site nobody thought
 * of is exactly the one that ships unpinned. It counts classes, not sites, so adding a second unpinned emit site for
 * an already-declared gated class passes here — {@code QueryDslTranslatorTests} covers both paths per function for
 * that. It sees only {@code new X(}, and the shapes it cannot see — a static factory, an {@code X::new} reference —
 * are held shut by {@code testNoExpressionIsBuiltInAShapeTheCensusCannotSee} rather than by a sentence asking nicely.
 * It reads one file, so an emit moved into a helper class elsewhere in this package would still be invisible.
 * And it forces the DECLARATION, not its correctness: whether the pin named is the version that function actually
 * arrived in is settled by the behavioural cases in that suite, not here.
 */
public class TranslatorEmittedFunctionPinsTests extends ESTestCase {

    private static final String TRANSLATOR = "src/main/java/org/elasticsearch/xpack/esql/dsltranslate/QueryDslTranslator.java";

    /**
     * Available on any node that clears the rewrite's own gate ({@code esql_request_filter_on_dataset}), so emitting
     * one needs no check. Adding a class here is a claim that it predates that gate.
     */
    private static final Set<String> PREDATES_REWRITE_GATE = Set.of(
        "And",
        "Or",
        "Not",
        "IsNotNull",
        "Literal",
        "MapExpression",
        "ToLower",
        "MvContains",
        "MvInRange",
        "MvIntersects"
    );

    /**
     * Postdates the rewrite's gate, so it carries its own pin and the translator consults it before building one.
     * The value is the constant the source must be seen to check.
     */
    private static final Map<String, String> GATED = Map.of(
        "MvGreater",
        "MvGreater.MV_COMPARE_TRANSPORT_VERSION",
        "MvLess",
        "MvLess.MV_COMPARE_TRANSPORT_VERSION"
    );

    private static final Pattern CONSTRUCTION = Pattern.compile("\\bnew\\s+([A-Z]\\w+)\\s*\\(");
    // The shapes this census cannot see. The sibling census guards its own blind spot executably rather than in
    // prose; this does the same, so a new expression built through a factory or a constructor reference cannot
    // slip past by being spelled differently.
    private static final Pattern PIN_ARGUMENT = Pattern.compile("([A-Z]\\w+\\.[A-Z_][A-Z0-9_]*)");
    private static final Pattern STATIC_FACTORY = Pattern.compile("\\b([A-Z]\\w+)\\s*\\.\\s*([a-z]\\w*)\\s*\\(");
    private static final Pattern CTOR_REFERENCE = Pattern.compile("\\b([A-Z]\\w+)\\s*::\\s*new");

    /**
     * Factory calls on an expression class that are known not to synthesize a gated function. {@code Literal.keyword}
     * builds the options map's key; {@code Literal} predates the rewrite's gate. Anything else is a new way to build
     * an expression that {@link #CONSTRUCTION} cannot see, and has to be looked at before it is added here.
     */
    private static final Set<String> DECLARED_FACTORIES = Set.of("Literal.keyword");
    private static final Pattern EXPRESSION_IMPORT = Pattern.compile(
        "^import\\s+org\\.elasticsearch\\.xpack\\.esql\\.(?:core\\.)?expression\\.[\\w.]*?([A-Z]\\w+);",
        Pattern.MULTILINE
    );

    public void testEveryEmittedExpressionIsDeclared() throws IOException {
        String source = Files.readString(esqlModuleRoot().resolve(TRANSLATOR));
        Set<String> emitted = emittedExpressionClasses(source);
        Set<String> undeclared = undeclaredIn(source);
        assertThat(
            "QueryDslTranslator constructs "
                + undeclared
                + ", which this census does not declare. An expression synthesized from a request filter must be one "
                + "every targeted node can deserialize — the rewrite's own gate names one constant and cannot promise "
                + "that. Declare each new class: in PREDATES_REWRITE_GATE if it predates esql_request_filter_on_dataset, "
                + "otherwise give it a TransportVersion on its own class and add it to GATED, and route EVERY site that "
                + "builds it through gated() with that constant.",
            undeclared,
            empty()
        );

        Set<String> declaredButGone = new TreeSet<>(declaredClasses());
        declaredButGone.removeAll(emitted);
        assertThat("declared but no longer constructed — delete the declaration: " + declaredButGone, declaredButGone, empty());
    }

    /**
     * {@link #CONSTRUCTION} sees only {@code new X(}. A static factory or an {@code X::new} reference builds an
     * expression the census would never count, so a function synthesized that way would ship unpinned with the build
     * green. Naming that in prose is a rule with no executor; this is the executor.
     */
    public void testNoExpressionIsBuiltInAShapeTheCensusCannotSee() throws IOException {
        String source = Files.readString(esqlModuleRoot().resolve(TRANSLATOR));
        Set<String> imported = importedExpressionClasses(source);

        Set<String> factories = new TreeSet<>();
        Matcher m = STATIC_FACTORY.matcher(source);
        while (m.find()) {
            if (imported.contains(m.group(1))) {
                factories.add(m.group(1) + "." + m.group(2));
            }
        }
        factories.removeAll(DECLARED_FACTORIES);
        assertThat(
            "an expression built through a static factory is invisible to the construction census, so it could ship "
                + "unpinned with this suite green. Build it with a constructor, or declare it in DECLARED_FACTORIES "
                + "once you have checked it synthesizes nothing that postdates the rewrite's gate: "
                + factories,
            factories,
            empty()
        );

        Set<String> refs = new TreeSet<>();
        Matcher r = CTOR_REFERENCE.matcher(source);
        while (r.find()) {
            if (imported.contains(r.group(1))) {
                refs.add(r.group(1) + "::new");
            }
        }
        assertThat("a constructor reference is invisible to the construction census too: " + refs, refs, empty());
    }

    /**
     * Each gated call must consult the pin of the function IT builds. Asserting over the union of constants seen
     * anywhere in the file does not do that: swapping two sites' constants leaves the union identical, and it stays
     * inert only while the constants resolve to the same id — which {@code MvCompare}'s javadoc says is exactly what
     * must not be relied on, since the constants sit on the leaves so a future subclass cannot inherit a stale pin.
     */
    public void testEveryGatedExpressionConsultsItsOwnPin() throws IOException {
        String source = Files.readString(esqlModuleRoot().resolve(TRANSLATOR));
        Map<String, String> pinPerConstructedClass = new TreeMap<>();
        for (String call : gatedCalls(source)) {
            Matcher constant = PIN_ARGUMENT.matcher(call);
            Matcher built = CONSTRUCTION.matcher(call);
            assertTrue("a gated() call names no pin: " + call, constant.find());
            assertTrue("a gated() call builds nothing: " + call, built.find());
            pinPerConstructedClass.put(built.group(1), constant.group(1));
        }

        Map<String, String> expected = new TreeMap<>(GATED);
        assertThat(
            "each gated function must consult its own pin, not merely some pin: " + pinPerConstructedClass,
            pinPerConstructedClass,
            equalTo(expected)
        );
    }

    /** The text of every {@code gated(...)} call in the source, each from the name to its matching close paren. */
    private static List<String> gatedCalls(String source) {
        List<String> calls = new ArrayList<>();
        // The lookbehind skips the helper's own declaration, "private Expression gated(", whose parameter list
        // carries no qualified constant and would otherwise read as a call that names no pin.
        Matcher m = Pattern.compile("(?<!Expression )\\bgated\\s*\\(").matcher(source);
        while (m.find()) {
            int depth = 1;
            int i = m.end();
            while (depth > 0 && i < source.length()) {
                char c = source.charAt(i++);
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                }
            }
            calls.add(source.substring(m.end(), i));
        }
        return calls;
    }

    /** The census has to fail on a new undeclared construction, or it is decoration. */
    public void testCensusFailsOnAnUndeclaredConstruction() {
        String fake = """
            import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
            import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSomethingNew;
            class T { Expression f() { return new And(new MvSomethingNew(source, field)); } }
            """;
        assertThat(emittedExpressionClasses(fake), equalTo(Set.of("And", "MvSomethingNew")));
        // The point is not that the regex works; it is that the check testEveryEmittedExpressionIsDeclared runs would
        // have gone red. So run that computation, not a restatement of it.
        assertThat(undeclaredIn(fake), equalTo(Set.of("MvSomethingNew")));
        assertThat("a declared class stays declared", undeclaredIn(fake), not(hasItem("And")));
    }

    /**
     * The pairing has to discriminate: a source that builds a gated function without routing it through the gate must
     * come back with nothing, and one that routes it through the WRONG pin must not read as correct.
     */
    public void testPinPairingSeesUngatedAndMispairedConstruction() {
        String ungated = """
            class T { Expression f() { return checkedLeaf(field, new MvGreater(source, field, bound, opts)); } }
            """;
        assertThat(gatedCalls(ungated), empty());

        String mispaired = """
            class T { Expression f() {
                return gated(field, MvLess.MV_COMPARE_TRANSPORT_VERSION, "range[...]",
                    () -> checkedLeaf(field, new MvGreater(source, field, bound, opts)));
            } }
            """;
        List<String> calls = gatedCalls(mispaired);
        assertThat(calls, hasSize(1));
        Matcher constant = PIN_ARGUMENT.matcher(calls.get(0));
        Matcher built = CONSTRUCTION.matcher(calls.get(0));
        assertTrue(constant.find());
        assertTrue(built.find());
        assertThat("the pairing must expose the mismatch, not hide it in a union", built.group(1), equalTo("MvGreater"));
        assertThat(constant.group(1), equalTo("MvLess.MV_COMPARE_TRANSPORT_VERSION"));
    }

    /** A constructed class that is not an imported expression is not the census's business. */
    public void testCensusIgnoresNonExpressionConstructions() {
        String fake = """
            import java.math.BigDecimal;
            import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
            class T { Object f() { return new BigDecimal(new Or(a, b).toString()); } }
            """;
        assertThat(emittedExpressionClasses(fake), equalTo(Set.of("Or")));
    }

    /** Every class the source constructs that this census does not declare — the computation the build fails on. */
    private static Set<String> undeclaredIn(String source) {
        Set<String> undeclared = new TreeSet<>(emittedExpressionClasses(source));
        undeclared.removeAll(declaredClasses());
        return undeclared;
    }

    private static Set<String> declaredClasses() {
        Set<String> declared = new TreeSet<>(PREDATES_REWRITE_GATE);
        declared.addAll(GATED.keySet());
        return declared;
    }

    /** The simple names the source imports as an ES|QL expression. */
    private static Set<String> importedExpressionClasses(String source) {
        Set<String> imported = new TreeSet<>();
        Matcher im = EXPRESSION_IMPORT.matcher(source);
        while (im.find()) {
            imported.add(im.group(1));
        }
        return imported;
    }

    /**
     * The classes the source both imports as an ES|QL expression and constructs. Intersecting the two is what keeps a
     * {@code BigDecimal} or a local record out of the census without maintaining a list of things to ignore.
     */
    private static Set<String> emittedExpressionClasses(String source) {
        Set<String> imported = importedExpressionClasses(source);
        Set<String> emitted = new TreeSet<>();
        Matcher cm = CONSTRUCTION.matcher(source);
        while (cm.find()) {
            if (imported.contains(cm.group(1))) {
                emitted.add(cm.group(1));
            }
        }
        return emitted;
    }

    /**
     * The esql module root. Gradle sets the test working directory to {@code <module>/build/testrun/<task>}, not the
     * module directory, and IDEs use the module or the repository root — so none of the three is assumed. Mirrors
     * {@code ReadConfigFingerprintDerivationSitesTests.findPluginRoot}, including its walk limit, which is generous
     * enough for the deepest of those starting points.
     */
    private static Path esqlModuleRoot() {
        Path cur = PathUtils.get("").toAbsolutePath();
        for (int i = 0; i < 12 && cur != null; i++, cur = cur.getParent()) {
            for (Path guess : List.of(cur, cur.resolve("x-pack/plugin/esql"))) {
                if (Files.isRegularFile(guess.resolve(TRANSLATOR))) {
                    return guess;
                }
            }
        }
        throw new AssertionError(
            "cannot locate the esql module from " + PathUtils.get("").toAbsolutePath() + " — the census needs the main sources"
        );
    }
}
