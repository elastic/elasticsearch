/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Golden tests for {@link RewriteSumOfExpressionPlusConstant} using {@link TestSubstitutionOnlyOptimizer},
 * which runs only the substitution batch. This lets us inspect the raw output of the rule in isolation,
 * before later optimization passes (constant folding, simplification, etc.) transform the tree further.
 *
 * <p>These complement {@link RewriteSumOfExpressionPlusConstantGoldenTests}, which uses the full optimizer.</p>
 */
public class RewriteSumOfExpressionPlusConstantSubstitutionOnlyGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);

    @Override
    protected TestBuilder builder(String esqlQuery) {
        return super.builder(esqlQuery)
            .optimizer(TestSubstitutionOnlyOptimizer::new)
            .stages(STAGES)
            .transportVersion(TransportVersionUtils.randomVersionSupporting(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION));
    }

    @Override
    protected List<String> filteredWarnings() {
        // Shadow warnings from RemoveStatsOverride appear in some tests; filter to avoid failures.
        List<String> warnings = new ArrayList<>(super.filteredWarnings());
        warnings.add("shadowed by field at line");
        return warnings;
    }

    public void testSumOfFieldPlusConstant() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1), s2 = SUM(emp_no + 2)
            """).run();
    }

    public void testSumOfFieldMinusConstant() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no - 2), s2 = SUM(3 - emp_no)
            """).run();
    }

    public void testSumOfFieldPlusConstantWithGroupBy() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1), s2 = SUM(emp_no + 2) BY languages
            """).run();
    }

    public void testBareSumOfSameFieldNotSharedWithRewrite() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1), bare = SUM(emp_no), s2 = SUM(emp_no + 2)
            """).run();
    }

    public void testCountAlreadyInQueryNotReused() {
        builder("""
            FROM employees
            | STATS c = COUNT(emp_no), s1 = SUM(emp_no + 1), s2 = SUM(emp_no + 2)
            """).run();
    }

    public void testSumOfFieldPlusFoldableConstantExpression() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + (5 - 2)), s2 = SUM(emp_no + 1)
            """).run();
    }

    public void testSumOfFieldPlusFoldableFunctionCallConstant() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + LENGTH("abc")), s2 = SUM(emp_no + 1)
            """).run();
    }

    public void testSumOfNestedArithmeticSharesSvPairOnSameBase() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1 + 2), s2 = SUM(emp_no + 1 + 3)
            """).run();
    }

    public void testThreeSumsShareOneSvPair() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1), s2 = SUM(emp_no + 2), s3 = SUM(emp_no + 3)
            """).run();
    }

    public void testTwoFieldsGetIndependentSvPairs() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1), s2 = SUM(emp_no + 2), s3 = SUM(salary + 1), s4 = SUM(salary + 2)
            """).run();
    }

    public void testBothFoldableOperandsNotRewritten() {
        builder("""
            FROM employees
            | STATS s1 = SUM(null + 1), s2 = SUM(null + 2)
            """).run();
    }

    public void testSumWithGroupingExpressionAlias() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1), s2 = SUM(emp_no + 2) BY l = languages
            """).run();
    }

    public void testShadowedNonMatchingExprRuleFires() {
        builder("""
            FROM employees
            | STATS s1 = SUM(emp_no + 1), c = SUM(emp_no), c = COUNT(emp_no), s2 = SUM(emp_no + 2)
            | LIMIT 10
            """).run();
    }

    public void testInlineStatsWithSumOfFieldPlusConstant() {
        builder("""
            FROM employees
            | INLINE STATS s1 = SUM(emp_no + 1), s2 = SUM(emp_no + 2)
            """).run();
    }
}
