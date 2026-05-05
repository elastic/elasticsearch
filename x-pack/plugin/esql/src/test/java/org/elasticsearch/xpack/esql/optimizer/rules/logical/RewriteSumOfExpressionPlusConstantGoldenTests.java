/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class RewriteSumOfExpressionPlusConstantGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testTwoSumsOfFieldPlusConstant() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testTwoSumsOfFieldPlusConstantWithGroupBy() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2) BY languages
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testTwoSumsOfFieldMinusConstant() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary - 1), s2 = SUM(2 - salary)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testSingleSumNotRewritten() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testTwoSumsNotRewrittenBelowMinVersion() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """).stages(STAGES).transportVersion(TransportVersionUtils.randomVersionNotSupporting(Sum.ESQL_SUM_LONG_OVERFLOW_FIX)).run();
    }

    public void testInlineStats() {
        builder("""
            FROM employees
            | INLINE STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testWithWhereBeforeStats() {
        builder("""
            FROM employees
            | WHERE gender == "M"
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testEvalOverridesFieldBeforeStats() {
        builder("""
            FROM employees
            | EVAL salary = salary * 2
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testSumAlongsideAvg() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2), a = AVG(salary)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testFilteredAggsNotRewritten() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1) WHERE gender == "M", s2 = SUM(salary + 2) WHERE gender == "M"
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testMixedFilteredAndUnfilteredAggs() {
        builder(
            """
                FROM employees
                | STATS s1 = SUM(salary + 1) WHERE gender == "M", s2 = SUM(salary + 2) WHERE gender == "M", s3 = SUM(salary + 3), s4 = SUM(salary + 4)
                """
        ).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testChainedStats() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2) BY languages
            | STATS t1 = SUM(s1 + 3), t2 = SUM(s1 + 4), t3 = SUM(s2 + 5), t4 = SUM(s2 + 6)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testMixedIntDoubleSums() {
        builder("""
            FROM employees
            | STATS s1 = SUM(salary + 1.5), s2 = SUM(salary + 2.5)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testDoubleNegation() {
        builder("""
            FROM employees
            | STATS s1 = SUM(-(-salary) + 1), s2 = SUM(-(-salary) + 2)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testExpressionInsteadOfField() {
        builder("""
            FROM employees
            | STATS s1 = SUM((2 * salary) + 1), s2 = SUM(-3 - (2 * salary))
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    // SUM(-x + 1) and SUM(1 - x) are mathematically equal but not grouped: the rule extracts
    // Neg(x) as the data expression for the first and x for the second, producing different keys.
    public void testNegationAndSubtractionNotGrouped() {
        builder("""
            FROM employees
            | STATS s1 = SUM(-salary + 1), s2 = SUM(1 - salary)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }

    public void testWhereFiltersAllRowsBeforeStats() {
        builder("""
            FROM employees
            | WHERE salary < 0
            | STATS s1 = SUM(salary + 1), s2 = SUM(salary + 2)
            """).stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION))
            .run();
    }
}
