/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.UnmappedGoldenTestCase;

import java.util.EnumSet;

/**
 * A null-propagating expression over a NULL-typed {@code COALESCE} must be folded to null by {@link FoldNull}: any later
 * rule that folds it builds its evaluator, which has no NULL branch and throws {@code Unsupported type NULL}.
 */
public class FoldNullGoldenTests extends UnmappedGoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public FoldNullGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);
    private static final EnumSet<Stage> STAGES_LOCAL = EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    public void testNullTypedCoalesceInArithmetic() {
        runGoldenTest("""
            FROM employees
            | EVAL x = COALESCE(null, null) * 2
            | KEEP emp_no, x
            | SORT emp_no
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testNullTypedCoalesceInDenseVectorArithmetic() {
        runGoldenTest("""
            FROM employees
            | EVAL x = COALESCE(null, null) * TO_DENSE_VECTOR([1.0, 2.0])
            | KEEP emp_no, x
            | SORT emp_no
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testNullTypedCoalesceOverNullAggregateUnderSort() {
        runGoldenTest("""
            FROM employees
            | STATS m = MAX(null) BY languages
            | EVAL x = COALESCE(m, m) * 2
            | SORT languages
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testNullTypedCoalesceOverUnmappedAggregateUnderSort() {
        // MAX(does_not_exist) only becomes `m = null` midway through the operator batch, and SORT + LIMIT make
        // PruneConstantSortKeysFromOrderBy fold every constant alias under the sort, including `x`.
        runTestsNullifyOnly("""
            FROM employees
            | STATS m = MAX(does_not_exist) BY languages
            | EVAL x = COALESCE(m, m) * 2
            | SORT languages
            | LIMIT 20
            """, STAGES_LOCAL);
    }
}
