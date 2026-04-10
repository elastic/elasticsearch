/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class RewriteSumFieldPlusConstantGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testTwoSumsOfFieldPlusConstant() {
        runGoldenTest("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """, STAGES);
    }

    public void testTwoSumsOfFieldPlusConstantWithGroupBy() {
        runGoldenTest("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2) BY languages
            """, STAGES);
    }

    public void testTwoSumsOfFieldMinusConstant() {
        runGoldenTest("""
            FROM employees
            | STATS s1 = SUM(salary - 1), s2 = SUM(2 - salary)
            """, STAGES);
    }

    public void testSingleSumNotRewritten() {
        runGoldenTest("""
            FROM employees
            | STATS s1 = SUM(salary + 1)
            """, STAGES);
    }
}
