/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.EnumSet;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Golden (characterization) tests for the analyzed plans produced by LET-prefix named-subquery scenarios.
 * LET named subqueries are resolved by {@link LetResolver} before view and IN-subquery expansion, mirroring
 * the ordering in {@code EsqlSession#execute}.
 */
public class AnalyzerLetGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerLetGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    // -- basic LET: single named subquery used as FROM source --

    public void testLetSingleBindingAsFromSource() {
        runGoldenTest("""
            LET top_langs = (FROM languages | WHERE language_code > 1 | KEEP language_name, language_code);
            FROM top_langs
            | WHERE language_code < 5
            | SORT language_code
            """, STAGES);
    }

    // -- multiple LET named subqueries, second references first --

    public void testLetMultipleBindingsSecondUsed() {
        runGoldenTest("""
            LET a = (FROM employees | WHERE emp_no > 10010 | KEEP emp_no, languages);
            LET b = (FROM languages | WHERE language_code > 2 | KEEP language_name, language_code);
            FROM b
            | SORT language_code
            """, STAGES);
    }

    // -- chained LET: later named subquery references earlier one --

    public void testLetChainedBindings() {
        runGoldenTest("""
            LET base = (FROM employees | WHERE emp_no > 10010 | KEEP emp_no, languages);
            LET top5 = (FROM base | LIMIT 5);
            FROM top5
            | SORT emp_no
            """, STAGES);
    }

    // -- LET named subquery beside an index in one FROM, without METADATA --

    /**
     * A named subquery is a branch of the FROM in its own right: it carries the SUBQUERY identity of a subquery, while the index
     * beside it is the bare INDEX branch.
     */
    public void testLetBindingBesideIndex() {
        runGoldenTest("""
            LET top_langs = (FROM languages | WHERE language_code > 1 | KEEP language_name, language_code);
            FROM top_langs, languages
            | SORT language_code
            """, STAGES);
    }

    // -- LET body used as the right-hand side of an IN subquery --

    public void testLetBindingInsideInSubquery() {
        runGoldenTest("""
            LET active_langs = (FROM languages | WHERE language_code > 1 | KEEP language_code);
            FROM employees
            | WHERE languages IN (FROM active_langs)
            | KEEP emp_no, languages
            | SORT emp_no
            """, STAGES);
    }

    // -- correlated subquery: free variable in LET body rejected by analyzer --
    // outer_code is unbound within the LET clause; it is not present in the languages index,
    // so the analyzer rejects it as an unknown column. Correlated subqueries are not yet supported.

    public void testLetCorrelatedSubquery() {
        var query = """
            LET same_lang = (FROM languages | WHERE language_code == outer_code | KEEP language_name);
            FROM languages
            | RENAME language_code AS outer_code
            | WHERE language_name IN (FROM same_lang)
            | KEEP outer_code, language_name
            | SORT outer_code
            """;
        var statement = EsqlTestUtils.TEST_PARSER.createStatement(query);
        var planAfterLet = LetResolver.resolve(statement.plan(), statement.letBindings(), ViewResolver.DEFAULT_MAX_PLAN_NODES_COUNT);
        var parsedPlan = InSubqueryResolver.resolve(planAfterLet);
        var esqlAnalyzer = EsqlTestUtils.analyzer().addLanguages().buildAnalyzer();
        var e = expectThrows(VerificationException.class, () -> esqlAnalyzer.analyze(parsedPlan));
        assertThat(e.getMessage(), containsString("Unknown column [outer_code]"));
    }

    // -- LET with a view body: view is resolved after LET substitution --

    public void testLetBindingReferencingView() {
        runGoldenTest("""
            LET top = (FROM view_langs | LIMIT 3);
            FROM top
            | SORT language_code
            """, STAGES, Map.of("view_langs", "FROM languages | WHERE language_code > 0"));
    }

    // -- LET subquery name referenced inside a view body is NOT visible to the view --
    // LetResolver runs on statement.plan() before ViewResolver expands view bodies.
    // A view body that references a LET subquery name treats it as an ES index, not
    // as the named subquery, and fails with an index-not-found error.
    public void testViewCannotReferenceLetBindingOutsideItsScope() {
        var query = """
            LET subquery = (FROM languages | WHERE language_code > 1 | KEEP language_code, language_name);
            FROM view
            | KEEP language_code, language_name
            """;
        var statement = EsqlTestUtils.TEST_PARSER.createStatement(query);
        var planAfterLet = LetResolver.resolve(statement.plan(), statement.letBindings(), ViewResolver.DEFAULT_MAX_PLAN_NODES_COUNT);
        var ta = EsqlTestUtils.analyzer()
            .addView("view", "FROM subquery | KEEP language_code, language_name")
            .addLanguages()
            .addIndex("subquery", IndexResolution.notFound("subquery"));
        var parsedPlan = ta.resolveViewsAndInSubqueries(planAfterLet);
        expectThrows(VerificationException.class, containsString("Unknown index [subquery]"), () -> ta.buildAnalyzer().analyze(parsedPlan));
    }

    // -- a LET named subquery cannot be used as a TS source, like an inline subquery --

    public void testLetBindingAsTsSourceRejected() {
        var statement = EsqlTestUtils.TEST_PARSER.createStatement("LET b = (FROM idx | LIMIT 3); TS b | STATS c = COUNT(*)");
        expectThrows(
            ParsingException.class,
            containsString("Subqueries are not supported in TS command"),
            () -> LetResolver.resolve(statement.plan(), statement.letBindings(), ViewResolver.DEFAULT_MAX_PLAN_NODES_COUNT)
        );
    }
}
