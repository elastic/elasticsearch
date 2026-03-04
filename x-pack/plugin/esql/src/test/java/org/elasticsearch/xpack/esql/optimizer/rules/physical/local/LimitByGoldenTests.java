/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class LimitByGoldenTests extends GoldenTestCase {

    public void testLimitBy() {
        runGoldenTest(
            """
                FROM employees
                | SORT salary
                | LIMIT 2 BY languages
                """,
            // TODO Nacho There seems to be a bug here in the physical optimization
            EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION, Stage.PHYSICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION),
            STATS
        );
    }

    public void testLimitByWithoutSort() {
        runGoldenTest(
            """
                FROM employees
                | LIMIT 5 BY emp_no + 4, languages
                """,
            EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION, Stage.PHYSICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION),
            STATS
        );
    }

    /**
     * LIMIT BY with a plain attribute should not introduce any synthetic EVAL.
     */
    public void testTopNWithGroupingsPlan() {
        runGoldenTest(
            """
                FROM employees
                | SORT salary DESC
                | LIMIT 5 BY languages
                """,
            // TODO Do we need to test the physical optimization in this and the cases onwards?
            EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION),
            STATS
        );
    }

    /**
     * LIMIT BY with an expression should extract the expression into a synthetic EVAL.
     */
    public void testTopNWithGroupingsExpressionPlan() {
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages * 2
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    /**
     * LIMIT BY with a mix of attributes and expressions should only extract the expression.
     */
    public void testTopNWithGroupingsMixedPlan() {
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages, salary * 2
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    /**
     * All groupings are foldable -- the LIMIT BY degenerates to a plain LIMIT (TopN with no groupings).
     */
    public void testTopNWithGroupingsByConstant() {
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY 20 * 5, 10
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    /**
     * Mixed foldable and attribute groupings: the foldable one (42) is pruned, the attribute (languages) remains.
     */
    public void testTopNWithGroupingsMixedFoldableAndAttribute() {
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages, 42
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    /**
     * Mixed foldable and expression groupings: the foldable (42) is pruned, the expression (languages * 2) is extracted to eval.
     */
    public void testTopNWithGroupingsMixedFoldableAndExpression() {
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages * 2, 42
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    public void testTopNWithGroupingsAndSeveralSorts() {
        runGoldenTest("""
            FROM employees
            | SORT emp_no DESC
            | SORT salary DESC
            | LIMIT 5 BY languages
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    public void testTopNWithGroupingsAndNonContiguousSorts() {
        runGoldenTest("""
            FROM employees
            | SORT emp_no DESC
            | KEEP emp_no, salary, languages
            | SORT salary DESC
            | LIMIT 5 BY languages
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    public void testTopNWithEvalAndNonEvalGroupings() {
        runGoldenTest("""
            FROM employees
            | SORT emp_no DESC
            | KEEP emp_no, salary, languages, gender
            | SORT salary DESC
            | LIMIT 5 BY languages * 2, gender
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    public void testTopNWithGroupingsQualifiedNamePlan() {
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY [employees].[languages]
            """, EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION), STATS);
    }

    private static final EsqlTestUtils.TestSearchStatsWithMinMax STATS = new EsqlTestUtils.TestSearchStatsWithMinMax(
        Map.of("date", dateTimeToLong("2023-10-20T12:15:03.360Z")),
        Map.of("date", dateTimeToLong("2023-10-23T13:55:01.543Z"))
    );
}
