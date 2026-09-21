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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

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
 * that. And it forces the DECLARATION, not its correctness: whether the pin named is the version that function
 * actually arrived in is settled by the behavioural cases in that suite, not here.
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
    private static final Pattern EXPRESSION_IMPORT = Pattern.compile(
        "^import\\s+org\\.elasticsearch\\.xpack\\.esql\\.(?:core\\.)?expression\\.[\\w.]*?([A-Z]\\w+);",
        Pattern.MULTILINE
    );
    // A pin is a named constant on a class, so the call site reads Class.CONSTANT. Requiring that shape also
    // keeps the helper's own declaration — requireFunction(TransportVersion required, ...) — out of the census.
    private static final Pattern REQUIRE = Pattern.compile("requireFunction\\s*\\(\\s*([A-Z]\\w+\\.[A-Z_][A-Z0-9_]*)");

    public void testEveryEmittedExpressionIsDeclared() throws IOException {
        String source = Files.readString(esqlModuleRoot().resolve(TRANSLATOR));
        Set<String> emitted = emittedExpressionClasses(source);

        Set<String> declared = new TreeSet<>(PREDATES_REWRITE_GATE);
        declared.addAll(GATED.keySet());

        Set<String> undeclared = new TreeSet<>(emitted);
        undeclared.removeAll(declared);
        assertThat(
            "QueryDslTranslator constructs "
                + undeclared
                + ", which this census does not declare. An expression synthesized from a request filter must be one "
                + "every targeted node can deserialize — the rewrite's own gate names one constant and cannot promise "
                + "that. Declare each new class: in PREDATES_REWRITE_GATE if it predates esql_request_filter_on_dataset, "
                + "otherwise give it a TransportVersion on its own class and add it to GATED, and call requireFunction "
                + "with that constant at EVERY site that builds it.",
            undeclared,
            empty()
        );

        Set<String> declaredButGone = new TreeSet<>(declared);
        declaredButGone.removeAll(emitted);
        assertThat("declared but no longer constructed — delete the declaration: " + declaredButGone, declaredButGone, empty());
    }

    public void testEveryGatedExpressionHasItsPinConsulted() throws IOException {
        String source = Files.readString(esqlModuleRoot().resolve(TRANSLATOR));
        Set<String> consulted = pinsConsulted(source);
        Set<String> expected = new TreeSet<>(GATED.values());
        assertThat(
            "a gated expression whose pin is never consulted is an ungated expression with a constant beside it; "
                + "expected requireFunction calls on "
                + expected
                + " but found "
                + consulted,
            consulted,
            equalTo(expected)
        );
    }

    /** The census has to fail on a new undeclared construction, or it is decoration. */
    public void testCensusFailsOnAnUndeclaredConstruction() {
        String fake = """
            import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
            import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSomethingNew;
            class T { Expression f() { return new And(new MvSomethingNew(source, field)); } }
            """;
        Set<String> emitted = emittedExpressionClasses(fake);
        assertThat(emitted, equalTo(Set.of("And", "MvSomethingNew")));

        Set<String> declared = new TreeSet<>(PREDATES_REWRITE_GATE);
        declared.addAll(GATED.keySet());
        assertFalse("a class nobody declared must not read as declared", declared.contains("MvSomethingNew"));
    }

    /**
     * The pin half has to discriminate too: a source that builds a gated function without consulting its constant
     * must come back with nothing consulted, or the check passes on a translator that ships the function unpinned.
     */
    public void testPinCensusSeesNoConsultationWhenThereIsNone() {
        String unpinned = """
            class T { Expression f() { return checkedLeaf(field, new MvGreater(source, field, bound, opts)); } }
            """;
        assertThat(pinsConsulted(unpinned), empty());

        String pinned = """
            class T { Expression f() {
                requireFunction(MvGreater.MV_COMPARE_TRANSPORT_VERSION, "range[single lower bound on keyword]");
                return checkedLeaf(field, new MvGreater(source, field, bound, opts));
            } }
            """;
        assertThat(pinsConsulted(pinned), equalTo(Set.of("MvGreater.MV_COMPARE_TRANSPORT_VERSION")));
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

    /** The pin constants the source is seen to consult. */
    private static Set<String> pinsConsulted(String source) {
        Set<String> consulted = new TreeSet<>();
        Matcher m = REQUIRE.matcher(source);
        while (m.find()) {
            consulted.add(m.group(1));
        }
        return consulted;
    }

    /**
     * The classes the source both imports as an ES|QL expression and constructs. Intersecting the two is what keeps
     * a {@code BigDecimal} or a local record out of the census without maintaining a list of things to ignore.
     */
    private static Set<String> emittedExpressionClasses(String source) {
        Set<String> imported = new TreeSet<>();
        Matcher im = EXPRESSION_IMPORT.matcher(source);
        while (im.find()) {
            imported.add(im.group(1));
        }
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
     * The esql module root, found by walking up from the working directory until the translator is under it. Gradle
     * runs tests from the module directory, but the IDE and a {@code :x-pack:plugin:esql:test} invocation from the
     * repository root do not agree on that, so neither is assumed.
     */
    private static Path esqlModuleRoot() {
        Path candidate = PathUtils.get("").toAbsolutePath();
        for (int up = 0; up < 6 && candidate != null; up++, candidate = candidate.getParent()) {
            for (Path guess : List.of(candidate, candidate.resolve("x-pack/plugin/esql"))) {
                if (Files.isRegularFile(guess.resolve(TRANSLATOR))) {
                    return guess;
                }
            }
        }
        throw new AssertionError("could not locate the esql module root from " + PathUtils.get("").toAbsolutePath());
    }
}
