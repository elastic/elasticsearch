/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;

import java.util.EnumSet;

/**
 * Golden coverage for aggregations that {@code ReplaceAggregateAggExpressionWithEval} deduplicates by resolving aliases defined
 * below the aggregate. The cases that must not be deduplicated live in {@code DeduplicateAggsTests}.
 */
public class DeduplicateAggsGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);

    /**
     * {@code c} and {@code d} are the same aggregation of the first {@code STATS}, so {@code MIN(c)} and {@code MIN(d)} are too.
     */
    public void testChainedStatsDeduplicatesDuplicateInnerAggs() {
        runGoldenTest("""
            FROM employees
            | STATS c = COUNT(*), d = COUNT(*) BY languages
            | STATS mc = MIN(c), md = MIN(d)
            """, STAGES);
    }

    /**
     * {@code AVG(s2)} becomes {@code SUM(s2) / COUNT(s2)}, and {@code s2} is an alias of {@code salary}, so the {@code SUM} is the
     * same aggregation as {@code s}.
     */
    public void testAvgOverAliasReusesSumInSameAggregate() {
        builder("""
            FROM employees
            | EVAL s2 = salary
            | STATS s = SUM(salary), a = AVG(s2)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).run();
    }

    /**
     * {@code mc} keeps the {@code INLINE STATS} in the plan: without a reference to {@code c} it is pruned before the aggregate
     * is deduplicated.
     */
    public void testInlineStatsLeftSideAliasDeduplicates() {
        runGoldenTest("""
            FROM employees
            | EVAL s2 = salary
            | INLINE STATS c = COUNT(*) BY languages
            | STATS a = MIN(s2), b = MIN(salary), mc = MAX(c)
            """, STAGES);
    }

    /**
     * The subquery only filters the rows of the left-hand side, so {@code s2} still equals {@code salary} on every row that
     * reaches the aggregate.
     */
    public void testInSubqueryLeftSideAliasDeduplicates() {
        runGoldenTest("""
            FROM employees
            | EVAL s2 = salary
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | STATS a = MIN(s2), b = MIN(salary)
            """, STAGES);
    }

    /**
     * {@code s2} is computed, so it is not folded into the aggregate before {@code DeduplicateAggs} runs, and {@code x} is used,
     * so the {@code DISSECT} is not pruned: {@code s2} only resolves by walking through the {@code DISSECT}.
     */
    public void testAliasBelowDissectDeduplicates() {
        runGoldenTest("""
            FROM employees
            | EVAL s2 = salary * 2
            | DISSECT first_name "%{x}"
            | STATS a = MIN(s2), b = MIN(salary * 2), c = COUNT(x)
            """, STAGES);
    }

    /**
     * {@code a} and {@code b} are the same literal on the only row, so {@code MIN(a)} and {@code MIN(b)} are the same aggregation.
     */
    public void testRowFieldsDeduplicate() {
        runGoldenTest("""
            ROW a = 1, b = 1
            | STATS x = MIN(a), y = MIN(b)
            """, STAGES);
    }
}
