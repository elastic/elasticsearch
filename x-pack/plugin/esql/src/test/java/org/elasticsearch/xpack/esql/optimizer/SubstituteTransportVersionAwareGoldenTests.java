/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;

/**
 * Golden tests for the {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteTransportVersionAwareExpressions} rule.
 * <p>
 *     Plan output is identical for any version within the same support category (before/after the fix),
 *     so randomized versions produce deterministic golden file content.
 * </p>
 */
public class SubstituteTransportVersionAwareGoldenTests extends GoldenTestCase {
    private static final String ESQL_SUM_LONG_OVERFLOW_FIX = "esql_sum_long_overflow_fix";

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public SubstituteTransportVersionAwareGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testSumGetsReplacedWithSafeLong() {
        builder("""
            FROM employees
            | STATS sum = SUM(languages.long)
            """).expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX).run();
    }

    public void testSumGetsReplacedWithSafeLongAndMultipleAggsAndGroups() {
        builder("""
            FROM employees
            | STATS sum_a = SUM(languages.long), sum_b = SUM(emp_no) BY emp_no
            """).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).run();
    }
}
