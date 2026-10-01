/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/**
 * Golden tests for {@link FoldAggregatesOverConstants}. The local physical plans show what's left for the data nodes, e.g. a
 * row count that can be pushed to Lucene.
 */
public class FoldAggregatesOverConstantsGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public FoldAggregatesOverConstantsGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testIdempotentConstantWithoutGroupings() {
        builder("""
            FROM employees
            | STATS m = MAX(1), c = COUNT(*)
            """).stages(STAGES).run();
    }

    public void testIdempotentConstantsWithGroupings() {
        builder("""
            FROM employees
            | STATS m = MIN(2), v = VALUES("a"), d = COUNT_DISTINCT([1, 2, 1]) BY languages
            """).stages(STAGES).run();
    }

    public void testIdempotentConstantFiltered() {
        builder("""
            FROM employees
            | STATS m = MAX(1) WHERE salary > 50000, n = MIN(1) BY languages
            """).stages(STAGES).run();
    }

    public void testNonIdempotentConstantNotFolded() {
        builder("""
            FROM employees
            | STATS s = SUM(1), a = AVG(2), m = MAX(3)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).run();
    }

    public void testFalseFilterAndNullInput() {
        builder("""
            FROM employees
            | STATS c = COUNT_DISTINCT(salary) WHERE false, p = PRESENT(null), m = MAX(salary)
            """).stages(STAGES).run();
    }
}
