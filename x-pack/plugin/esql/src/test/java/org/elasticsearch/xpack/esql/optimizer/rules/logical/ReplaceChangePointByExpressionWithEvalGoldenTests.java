/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class ReplaceChangePointByExpressionWithEvalGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testAttributeGroupingUnchanged() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | CHANGE_POINT salary ON emp_no BY gender
            """, STAGES);
    }

    public void testMultipleAttributeGroupingsUnchanged() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender, languages
            | CHANGE_POINT salary ON emp_no BY gender, languages
            """, STAGES);
    }

    public void testSingleExpressionMovedToEval() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY emp_no + 5
            """, STAGES);
    }

    public void testMultipleExpressionsMovedToEval() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary
            | CHANGE_POINT salary ON emp_no BY emp_no + 5, salary * 2
            """, STAGES);
    }

    public void testFunctionCallGroupingMovedToEval() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, first_name
            | CHANGE_POINT salary ON emp_no BY LENGTH(first_name)
            """, STAGES);
    }

    public void testMixedAttributeAndExpressionGroupings() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | CHANGE_POINT salary ON emp_no BY gender, salary * 2
            """, STAGES);
    }

    public void testEvalAliasAndExpressionMixedInChangePointBy() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, languages
            | EVAL x = emp_no + 5
            | CHANGE_POINT salary ON emp_no BY x, languages * 2
            """, STAGES);
    }
}
