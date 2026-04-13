/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class RewriteSumOfExpressionPlusConstantGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testTwoSumsOfFieldPlusConstant() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """).stages(STAGES)
            .transportVersion(TransportVersion.current())
            .run();
    }

    public void testTwoSumsOfFieldPlusConstantWithGroupBy() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2) BY languages
            """).stages(STAGES)
            .transportVersion(TransportVersion.current())
            .run();
    }

    public void testTwoSumsOfFieldMinusConstant() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary - 1), s2 = SUM(2 - salary)
            """).stages(STAGES)
            .transportVersion(TransportVersion.current())
            .run();
    }

    public void testSingleSumNotRewritten() {
        runGoldenTest("""
            FROM employees
            | STATS s1 = SUM(salary + 1)
            """, STAGES);
    }
}
