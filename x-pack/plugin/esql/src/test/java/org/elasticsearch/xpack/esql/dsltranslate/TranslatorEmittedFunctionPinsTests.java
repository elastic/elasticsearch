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
 * that. It sees only {@code new X(}, so a static factory or an {@code X::new} reference is invisible to it, and a
 * factory is the natural next thing someone writes — keep synthesizing through constructors, or widen the pattern.
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
    private static final Pattern EXPRESSION_IMPORT = Pattern.compile(
        "^import\\s+org\\.elasticsearch\\.xpack\\.esql\\.(?:core\\.)?expression\\.[\\w.]*?([A-Z]\\w+);",
        Pattern.MULTILINE
    );
    // The gate reads gated(field, Class.CONSTANT, construct, leaf), so the pin is the SECOND argument. Requiring the
    // Class.CONSTANT shape also keeps the helper's own declaration — gated(Expression, TransportVersion, ...) — out
    // of the census, since a parameter list carries no qualified constant.
    private static final Pattern REQUIRE = Pattern.compile("gated\\s*\\(\\s*[^,()]+,\\s*([A-Z]\\w+\\.[A-Z_][A-Z0-9_]*)");

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
                + "otherwise give it a TransportVersion on its own class and add it to GATED, and route EVERY site that "
                + "builds it through gated() with that constant.",
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
                + "expected gated() calls carrying "
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
                return gated(field, MvGreater.MV_COMPARE_TRANSPORT_VERSION, "range[...]", () -> leaf());
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
