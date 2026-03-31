/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.EnumSet;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION;

/**
 * Golden tests for the lookup-node planning pipeline (logical + physical optimization).
 * These snapshot the output of {@link LookupLogicalOptimizer} and {@link LookupPhysicalPlanOptimizer}
 * for LOOKUP JOIN queries.
 */
public class LookupGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOOKUP_LOGICAL_OPTIMIZATION, Stage.LOOKUP_PHYSICAL_OPTIMIZATION);

    /**
     * Simple lookup with no filters.
     */
    public void testSimpleLookup() {
        runGoldenTest("FROM employees | LOOKUP JOIN test_lookup ON emp_no", STAGES);
    }

    /**
     * WHERE clause with a pushable right-only filter (equality on a keyword field).
     * The logical optimizer pushes it into the join's right side, and the lookup physical optimizer
     * pushes it down to ParameterizedQueryExec.query().
     */
    public void testLookupWithPushableFilter() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """, STAGES);
    }

    /**
     * WHERE clause with a non-pushable right-only filter (LENGTH function comparison).
     * The logical optimizer pushes it into the join's right side, but the lookup physical optimizer
     * cannot push it to Lucene, so it stays as a FilterExec.
     */
    public void testLookupWithNonPushableFilter() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE LENGTH(language_name) > 3
            """, STAGES);
    }

    /**
     * WHERE clause with both pushable and non-pushable right-only filters.
     * The pushable part goes to ParameterizedQueryExec.query(), the non-pushable stays as a FilterExec.
     */
    public void testLookupWithMixedFilters() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English" AND LENGTH(language_name) > 3
            """, STAGES);
    }

    /**
     * Two consecutive LOOKUP JOINs: first on test_lookup (by emp_no), then on languages_lookup (by language_code).
     * Each join's right side is independently planned on its respective lookup node.
     */
    public void testTwoLookupJoins() {
        runGoldenTest("""
            FROM employees
            | LOOKUP JOIN test_lookup ON emp_no
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """, STAGES);
    }

    /**
     * Filter that becomes always-true due to missing field stats should be pruned.
     * {@code language_name IS NULL} with language_name missing becomes {@code null IS NULL} which folds to true,
     * so the filter is removed. The null Eval from ReplaceFieldWithConstantOrNull is pruned by the
     * physical optimizer since the field is not needed for extraction.
     */
    public void testFilterOnMissingFieldFoldedToTrue() {
        SearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS,
            "language_name"
        );
        builder("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name IS NULL
            """).stages(STAGES).searchStats(stats).run();
    }

    /**
     * Filter on a missing field with equality (e.g. {@code language_name == "English"}) folds to
     * {@code null == "English"} which evaluates to null, marking the ParameterizedQueryExec as
     * {@code emptyResult=true} instead of collapsing the plan.
     */
    public void testFilterOnMissingFieldFoldedToEmpty() {
        SearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS,
            "language_name"
        );
        builder("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """).stages(STAGES).searchStats(stats).run();
    }

    /**
     * Constant field matching the filter value: {@code language_name} is a constant {@code "English"},
     * and the filter is {@code WHERE language_name == "English"}. The constant replaces the field reference,
     * the filter folds to {@code true} and is pruned.
     */
    public void testConstantFieldMatchingFilter() {
        SearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().withConstantValue("language_name", "English");
        builder("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """).stages(STAGES).searchStats(stats).run();
    }

    /**
     * ON expression with a pushable right-only filter combined with a non-pushable WHERE clause.
     * The ON equality filter is pushed to ParameterizedQueryExec.query(), while LENGTH stays as a FilterExec.
     */
    public void testOnExpressionFilterWithWhereClause() {
        assumeTrue("Requires LOOKUP JOIN on expression", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());
        builder("""
            FROM employees
            | LOOKUP JOIN languages_lookup ON languages == language_code AND language_name == "English"
            | WHERE LENGTH(language_name) > 3
            """).stages(STAGES).transportVersion(ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION).run();
    }

    /**
     * When a missing field is dropped from the output, it never appears in the lookup plan's addedFields,
     * so no null Eval is needed. Uses expression-based join so that language_code remains as an
     * extractable added field after dropping language_name.
     */
    public void testDropMissingFieldPrunesEval() {
        assumeTrue("Requires LOOKUP JOIN on expression", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());
        SearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS,
            "language_name"
        );
        builder("""
            FROM employees
            | LOOKUP JOIN languages_lookup ON languages == language_code
            | DROP language_name
            """).stages(STAGES).searchStats(stats).transportVersion(ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION).run();
    }
}
