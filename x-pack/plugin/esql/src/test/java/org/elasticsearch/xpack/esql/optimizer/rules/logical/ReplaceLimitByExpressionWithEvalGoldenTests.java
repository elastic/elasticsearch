/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.junit.BeforeClass;

import java.util.EnumSet;

public class ReplaceLimitByExpressionWithEvalGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    @BeforeClass
    public static void checkLimitByCapability() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
    }

    public void testAttributeGroupingUnchanged() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY emp_no
            """, STAGES);
    }

    public void testMultipleAttributeGroupingsUnchanged() {
        runGoldenTest("""
            FROM employees
            | LIMIT 2 BY emp_no, salary
            """, STAGES);
    }

    public void testAllFoldableGroupingsDegenerateToPlainLimit() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY 1
            """, STAGES);
    }

    public void testMultipleFoldableGroupingsDegenerateToPlainLimit() {
        runGoldenTest("""
            FROM employees
            | LIMIT 3 BY 1, "constant", false
            """, STAGES);
    }

    public void testMixedFoldableAndAttributeGroupingsPruneFoldable() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY emp_no, 1
            """, STAGES);
    }

    public void testSingleExpressionMovedToEval() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no
            | LIMIT 1 BY emp_no + 5
            """, STAGES);
    }

    public void testMultipleExpressionsMovedToEval() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | LIMIT 1 BY emp_no + 5, salary * 2
            """, STAGES);
    }

    public void testMixedAttributeAndExpressionGroupings() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | LIMIT 1 BY emp_no, salary * 2
            """, STAGES);
    }

    public void testFoldableGroupingPrunedAndExpressionExtracted() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no
            | LIMIT 1 BY emp_no + 5, 1
            """, STAGES);
    }

    public void testFoldableGroupingPrunedAttributeSurvives() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY emp_no, 1, false
            """, STAGES);
    }

    public void testEvalAliasAndExpressionMixedInLimitBy() {
        runGoldenTest("""
            FROM employees
            | EVAL x = emp_no + 5
            | LIMIT 1 BY x, salary * 2
            """, STAGES);
    }

    public void testFoldableAttributeAndExpressionGroupingsMixed() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | LIMIT 1 BY emp_no, salary * 2, 1
            """, STAGES);
    }

    public void testTopNByAttributeDoesNotIntroduceEval() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages
            """, STAGES);
    }

    public void testTopNByExpressionIntroducesEval() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages * 2
            """, STAGES);
    }

    public void testTopNByMixedAttrAndExpression() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages, salary * 2
            """, STAGES);
    }

    public void testTopNByFoldableExprs() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY 20 * 5, 10
            """, STAGES);
    }

    public void testTopNByMixedAttrAndFoldable() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages, 42
            """, STAGES);
    }

    public void testTopNByMixedExprsAttr() {
        assumeTrue("SORT | LIMIT BY requires snapshot builds", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        runGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 5 BY languages * 2, 42
            """, STAGES);
    }
}
