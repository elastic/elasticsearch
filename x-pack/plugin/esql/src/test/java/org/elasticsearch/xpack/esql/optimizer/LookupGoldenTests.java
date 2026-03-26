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

    public void testSimpleLookup() {
        runGoldenTest("FROM employees | LOOKUP JOIN test_lookup ON emp_no", STAGES);
    }

    public void testLookupWithPushableFilter() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """, STAGES);
    }

    public void testLookupWithNonPushableFilter() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE LENGTH(language_name) > 3
            """, STAGES);
    }

    public void testLookupWithMixedFilters() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English" AND LENGTH(language_name) > 3
            """, STAGES);
    }

    public void testTwoLookupJoins() {
        runGoldenTest("""
            FROM employees
            | LOOKUP JOIN test_lookup ON emp_no
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """, STAGES);
    }

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

    public void testConstantFieldMatchingFilter() {
        SearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().withConstantValue("language_name", "English");
        builder("""
            FROM employees
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """).stages(STAGES).searchStats(stats).run();
    }

    public void testOnExpressionFilterWithWhereClause() {
        assumeTrue("Requires LOOKUP JOIN on expression", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());
        builder("""
            FROM employees
            | LOOKUP JOIN languages_lookup ON languages == language_code AND language_name == "English"
            | WHERE LENGTH(language_name) > 3
            """).stages(STAGES).transportVersion(ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION).run();
    }

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
