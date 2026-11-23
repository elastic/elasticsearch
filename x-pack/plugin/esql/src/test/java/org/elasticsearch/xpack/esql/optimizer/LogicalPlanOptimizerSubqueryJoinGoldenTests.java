/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.Before;

import java.util.EnumSet;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;

/**
 * Captures the analyzed and logically optimized plans of {@code LEFT OUTER JOIN} on a subquery. The session rejects the join before
 * optimization until it can run (see {@link SubqueryJoin#rejectUntilSupported}); these tests optimize the analyzed plan directly.
 */
public class LogicalPlanOptimizerSubqueryJoinGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    private static final String JOIN_LANGUAGES = """
        FROM employees
        | EVAL language_code = languages
        | LEFT OUTER JOIN (FROM languages) ON language_code
        """;

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LogicalPlanOptimizerSubqueryJoinGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    @Before
    public void checkGrammar() {
        assumeTrue("LEFT OUTER JOIN requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
    }

    // -- analysis --

    /**
     * The left key is the left side's attribute and the right key the subquery's.
     */
    public void testResolvesKeysOnEachSide() {
        runGoldenTest(JOIN_LANGUAGES, STAGES);
    }

    public void testResolvesMultipleKeysPairwise() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages, language_name = first_name
            | LEFT OUTER JOIN (FROM languages) ON language_code, language_name
            """, STAGES);
    }

    /**
     * Without a {@code Project} or an {@code Aggregate} of its own, a data node running the subquery could reduce its fields to
     * {@code _doc}, so the analyzer pins its output.
     */
    public void testPinsSubqueryOutput() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | WHERE language_code > 1) ON language_code
            """, STAGES);
    }

    /**
     * {@code INLINE STATS} holds an {@code Aggregate}, but the subquery's columns pass through it, so its output is pinned as well.
     */
    public void testPinsInlineStatsSubqueryOutput() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | INLINE STATS c = COUNT(*) BY language_code) ON language_code
            """, STAGES);
    }

    public void testDoesNotPinSubqueryWithItsOwnKeep() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | KEEP language_code, language_name) ON language_code
            """, STAGES);
    }

    public void testDoesNotPinSubqueryWithItsOwnStats() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | STATS c = COUNT(*) BY language_code) ON language_code
            """, STAGES);
    }

    /**
     * The subquery's columns come from its result, not from an index the main query reads, so they are references. The join key
     * keeps the left side's attribute, and a subquery column shadows a left column of the same name.
     */
    public void testSubqueryColumnsAreReferences() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages, language_name = first_name
            | KEEP emp_no, language_code, language_name
            | LEFT OUTER JOIN (FROM languages) ON language_code
            """, STAGES);
    }

    public void testNestedJoin() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (
                FROM languages
                | LEFT OUTER JOIN (FROM languages | EVAL x = 1 | KEEP language_code, x) ON language_code
              ) ON language_code
            """, STAGES);
    }

    // -- logical optimization --

    /**
     * A filter on a subquery column stays above the join: it must still reject the left rows that found no match. Pushing a copy
     * into the subquery as well, as for {@code LOOKUP JOIN}, would never settle: the subquery's own rules move the copy below its
     * projections, so the next pass would push it again.
     */
    public void testFilterOnSubqueryColumnStaysAboveJoin() {
        runGoldenTest(JOIN_LANGUAGES + "| WHERE language_name == \"English\"", STAGES);
    }

    public void testFilterOnLeftColumnIsPushedBelowJoin() {
        runGoldenTest(JOIN_LANGUAGES + "| WHERE emp_no > 10", STAGES);
    }

    /**
     * A row without a match gets null for the subquery's columns, so a constant the subquery defines must not replace its column
     * above the join. Folding {@code flag} to {@code true} would turn the filter into {@code false} and drop every row.
     */
    public void testSubqueryConstantIsNotFoldedAboveJoin() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | EVAL flag = true) ON language_code
            | WHERE flag IS NULL
            """, STAGES);
    }

    /**
     * For the same reason, a sort on such a column is kept: the rows without a match sort apart from the others.
     */
    public void testSortOnSubqueryConstantIsKept() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | EVAL flag = true) ON language_code
            | SORT flag
            | LIMIT 10
            """, STAGES);
    }

    /**
     * The subquery's constants still fold inside the subquery. A {@code ROW} becomes a {@link LocalRelation} only once
     * {@code b = a + 1} is folded.
     */
    public void testSubqueryConstantsFoldInsideSubquery() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (ROW language_code = 1, a = 1, b = a + 1) ON language_code
            """, STAGES);
    }

    /**
     * A {@code LIMIT} after the join is copied to the left side only: limiting the subquery would drop matches.
     */
    public void testLimitIsNotPushedIntoSubquery() {
        runGoldenTest(JOIN_LANGUAGES + "| LIMIT 10", STAGES);
    }

    /**
     * Moving a {@code RENAME} past the join needs an {@code Eval} when the renamed column is shadowed by a subquery column. The
     * rebuilt join must still be a {@link SubqueryJoin}, not a plain {@link Join}.
     */
    public void testProjectMovedPastJoinKeepsSubqueryJoin() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | RENAME first_name AS name
            | LEFT OUTER JOIN (FROM languages | EVAL first_name = language_name | KEEP language_code, first_name) ON language_code
            | KEEP emp_no, name, first_name
            """, STAGES);
    }

    /**
     * The join reads its subquery into a hash table, so a {@code SORT} in the subquery is dropped rather than rejected as an
     * unbounded sort.
     */
    public void testUnboundedSortInSubqueryIsDropped() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | SORT language_name) ON language_code
            """, STAGES);
    }

    /**
     * A {@code SORT} with a {@code LIMIT} decides which rows the subquery returns, so it stays.
     */
    public void testSortWithLimitInSubqueryIsKept() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | SORT language_name | LIMIT 2) ON language_code
            """, STAGES);
    }

    /**
     * The join copies the subquery's columns with {@code Block#lookup}, which some block types do not implement. The check runs after
     * optimization, so a column that nothing reads after the join is pruned instead of rejected.
     */
    public void testPayloadTypeWithoutLookupIsPruned() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LEFT OUTER JOIN (ROW language_code = 1, m = TO_AGGREGATE_METRIC_DOUBLE(1)) ON language_code
            | DROP m
            """, STAGES);
    }

    /**
     * A null left key matches nothing, so the join is replaced with nulls for the subquery's columns. Columns of the same type share
     * one null through an {@code Alias}, which the join's output must keep.
     */
    public void testNullKeyReplacesJoinWithNulls() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = null
            | LEFT OUTER JOIN (FROM languages | EVAL other_name = language_name) ON language_code
            """, STAGES);
    }
}
