/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class PruneLiteralsInChangePointByGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testAllFoldableGroupingsDegenerateToUngrouped() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY 1
            """, STAGES);
    }

    public void testMultipleFoldableGroupingsDegenerateToUngrouped() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY 1, "constant", false
            """, STAGES);
    }

    public void testFoldableArithmeticExpressionPruned() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY 1 + 1
            """, STAGES);
    }

    public void testFoldableFunctionCallPruned() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY LENGTH("hello")
            """, STAGES);
    }

    public void testMultipleFoldableExpressionsPruned() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY 1 + 1, LENGTH("foo"), false
            """, STAGES);
    }

    public void testMixedFoldableLiteralAndAttribute() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | CHANGE_POINT salary ON emp_no BY gender, 1
            """, STAGES);
    }

    public void testMixedFoldableExpressionAndAttribute() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | CHANGE_POINT salary ON emp_no BY gender, 1 + 1
            """, STAGES);
    }

    public void testFoldableGroupingPrunedAndExpressionExtracted() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY emp_no + 5, 1
            """, STAGES);
    }

    public void testEvalAliasFoldablePropagatedAndPruned() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | EVAL x = 5
            | CHANGE_POINT salary ON emp_no BY x
            """, STAGES);
    }

    public void testEvalAliasFoldableMixedWithAttribute() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | EVAL x = 5
            | CHANGE_POINT salary ON emp_no BY gender, x
            """, STAGES);
    }
}
