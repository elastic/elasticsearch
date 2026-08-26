/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class LimitByGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LimitByGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.ANALYSIS,
        Stage.LOGICAL_OPTIMIZATION,
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.LOCAL_PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE,
        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
    );

    public void testLimitByWithoutSort() {
        runGoldenTest("""
            FROM employees
            | LIMIT 5 BY emp_no + 4, languages
            """, STAGES, STATS);
    }

    public void testSortLimitBy() {
        runGoldenTest("""
            FROM employees
            | SORT salary
            | LIMIT 5 BY emp_no + 4, languages
            """, STAGES, STATS);
    }

    public void testLimitByBucket() {
        runGoldenTest("""
            FROM employees
            | LIMIT 1 BY BUCKET(hire_date, 1 year)
            """, STAGES, STATS);
    }

    /** Shows INITIAL (data node) and FINAL (coordinator) plan for {@code LIMIT N BY CATEGORIZE(...)}. */
    public void testLimitByCategorize() {
        runGoldenTest("""
            FROM sample_data
            | LIMIT 2 BY CATEGORIZE(message)
            """, STAGES);
    }

    /** Shows INITIAL (data node) and FINAL (coordinator) plan for {@code SORT ... | LIMIT N BY CATEGORIZE(...)}. */
    public void testSortLimitByCategorize() {
        runGoldenTest("""
            FROM sample_data
            | SORT @timestamp DESC
            | LIMIT 2 BY CATEGORIZE(message)
            """, STAGES);
    }

    /** CATEGORIZE alongside a plain grouping key. */
    public void testLimitByCategorizeWithExtraGroupKey() {
        runGoldenTest("""
            FROM sample_data
            | LIMIT 2 BY CATEGORIZE(message), client_ip
            """, STAGES);
    }

    /** CATEGORIZE alongside a plain grouping key, with sort. */
    public void testSortLimitByCategorizeWithExtraGroupKey() {
        runGoldenTest("""
            FROM sample_data
            | SORT @timestamp DESC
            | LIMIT 2 BY CATEGORIZE(message), client_ip
            """, STAGES);
    }

    /** Constant CATEGORIZE — pruned to a plain LIMIT by PruneLiteralsInLimitBy. */
    public void testLimitByCategorizeOnNonNullConstant() {
        runGoldenTest("""
            FROM sample_data
            | EVAL x = "Connection error"::keyword
            | LIMIT 2 BY CATEGORIZE(x)
            """, STAGES);
    }

    /** CATEGORIZE on a function expression (CONCAT). */
    public void testSortLimitByCategorizeWithFunctionArg() {
        runGoldenTest("""
            FROM sample_data
            | SORT @timestamp DESC
            | LIMIT 2 BY CATEGORIZE(CONCAT(message, " "))
            """, STAGES);
    }

    /** Four mixed groupings: expression, CATEGORIZE(attribute), attribute, CATEGORIZE(expression).
     *  expression should get extracted into its own eval since it's reused as the first groping and inside the CATEGORIZE
     *  in the fourth grouping
     * */
    public void testLimitByCategorizeMixedGroupings() {
        runGoldenTest("""
            FROM sample_data
            | LIMIT 1 BY CONCAT(message, " "), CATEGORIZE(message), client_ip, CATEGORIZE(CONCAT(message, " "))
            """, STAGES);
    }

    private static final EsqlTestUtils.TestSearchStatsWithMinMax STATS = new EsqlTestUtils.TestSearchStatsWithMinMax(
        Map.of("date", dateTimeToLong("2023-10-20T12:15:03.360Z")),
        Map.of("date", dateTimeToLong("2023-10-23T13:55:01.543Z"))
    );
}
