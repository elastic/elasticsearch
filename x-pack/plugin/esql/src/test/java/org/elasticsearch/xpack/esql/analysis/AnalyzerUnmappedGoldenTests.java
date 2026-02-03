/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

/**
 * Golden tests for analyzer behavior with unmapped fields using SET unmapped_fields="nullify" and "load".
 * These tests verify that unmapped fields are properly handled.
 */
public class AnalyzerUnmappedGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    public void testKeep() throws Exception {
        runTests("""
            FROM employees
            | KEEP does_not_exist_field
            """);
    }

    public void testKeepRepeated() throws Exception {
        runTests("""
            FROM employees
            | KEEP does_not_exist_field, does_not_exist_field
            """);
    }

    public void testKeepAndMatchingStar() throws Exception {
        runTests("""
            FROM employees
            | KEEP emp_*, does_not_exist_field
            """);
    }

    public void testEvalAndKeep() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field1::INTEGER + 42
            | KEEP does_not_exist_field1, does_not_exist_field2
            """);
    }

    public void testEvalAfterKeepStar() throws Exception {
        runTests("""
            FROM employees
            | KEEP *
            | EVAL x = emp_no + 1
            | EVAL y = does_not_exist_field::DOUBLE + 2
            """);
    }

    public void testEvalAfterMatchingKeepWithWildcard() throws Exception {
        runTests("""
            FROM employees
            | KEEP emp_no, *
            | EVAL x = emp_no + 1
            | EVAL y = emp_does_not_exist_field::DOUBLE + 2
            """);
    }

    public void testDrop() throws Exception {
        runTests("""
            FROM employees
            | DROP does_not_exist_field, emp_no
            """);
    }

    public void testDropAndMatchingStar() throws Exception {
        runTests("""
            FROM employees
            | DROP emp_*, does_not_exist_field
            """);
    }

    public void testRename() throws Exception {
        runTests("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, emp_no AS employee_number
            """);
    }

    public void testRenameRepeated() throws Exception {
        runTests("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, neither_does_this AS now_it_does, emp_no AS employee_number
            """);
    }

    public void testRenameEval() throws Exception {
        runTests("""
            FROM employees
            | RENAME emp_no AS employee_number
            | EVAL x = does_not_exist::DOUBLE + 1
            """);
    }

    public void testEval() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            """);
    }

    public void testMultipleEval() throws Exception {
        runTests("""
            FROM employees
            | EVAL a = 1
            | EVAL x = a + b::DOUBLE
            | EVAL y = b::DOUBLE + c::DOUBLE
            """);
    }

    public void testEvalCast() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field::LONG
            """);
    }

    public void testEvalCastInPlace() throws Exception {
        runTests("""
            FROM employees
            | EVAL does_not_exist_field::LONG
            """);
    }

    public void testEvalReplace() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            | EVAL does_not_exist_field = 42
            """);
    }

    public void testKeepThenEval() throws Exception {
        runTests("""
            FROM employees
            | KEEP does_not_exist_field
            | EVAL does_not_exist_field = 42
            """);
    }

    public void testStatsCount() throws Exception {
        runTests("""
            FROM employees
            | STATS cnt = COUNT(does_not_exist_field)
            """);
    }

    public void testStatsBy() throws Exception {
        runTests("""
            FROM employees
            | STATS BY does_not_exist_field
            """);
    }

    public void testStatsSumBy() throws Exception {
        runTests("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
            """);
    }

    public void testStatsSumByMultiple() throws Exception {
        runTests("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) + d2 BY d2 = does_not_exist2::DOUBLE, emp_no
            """);
    }

    public void testStatsSumByComplex() throws Exception {
        runTests("""
            FROM employees
            | STATS sum = SUM(does_not_exist1::DOUBLE) + s0 + s1 BY s0 = does_not_exist2::DOUBLE + does_not_exist3::DOUBLE, s1 = emp_no
            """);
    }

    public void testStatsMultiple() throws Exception {
        runTests("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """);
    }

    public void testInlineStats() throws Exception {
        runTests("""
            FROM employees
            | INLINE STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """);
    }

    public void testWhere() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist::LONG > 0
            """);
    }

    public void testWhereOr() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist::LONG > 0 OR emp_no > 0
            """);
    }

    public void testWhereComplex() throws Exception {
        runTests("""
            FROM employees
            | WHERE does_not_exist1::LONG > 0 OR emp_no > 0 AND does_not_exist2::LONG < 100
            """);
    }

    public void testStatsCountWhere() throws Exception {
        runTests("""
            FROM employees
            | STATS c = COUNT(*) WHERE does_not_exist1::LONG > 0
            """);
    }

    public void testStatsMultipleCountWhere() throws Exception {
        runTests("""
            FROM employees
            | STATS c1 = COUNT(*) WHERE does_not_exist1::LONG > 0 OR emp_no > 0 OR does_not_exist2::LONG < 100,
                    c2 = COUNT(*) WHERE does_not_exist3 IS NULL
            """);
    }

    public void testSort() throws Exception {
        runTests("""
            FROM employees
            | SORT does_not_exist ASC
            """);
    }

    public void testSortExpression() throws Exception {
        runTests("""
            FROM employees
            | SORT does_not_exist::LONG + 1
            """);
    }

    public void testSortMultiple() throws Exception {
        runTests("""
            FROM employees
            | SORT does_not_exist1::LONG + 1, does_not_exist2 DESC, emp_no ASC
            """);
    }

    public void testMvExpand() throws Exception {
        runTests("""
            FROM employees
            | MV_EXPAND does_not_exist
            """);
    }

    public void testLookupJoin() throws Exception {
        runTests("""
            FROM employees
            | EVAL language_code = does_not_exist::INTEGER
            | LOOKUP JOIN languages_lookup ON language_code
            """);
    }

    public void testLookupJoinWithFilter() throws Exception {
        runTests("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE does_not_exist::LONG > 0
            """);
    }

    public void testSubqueryKeepUnmapped() throws Exception {
        runTests("""
            FROM employees, (FROM languages | KEEP language_code, does_not_exist)
            | KEEP emp_no, language_code, does_not_exist
            """);
    }

    public void testSubqueryWithStats() throws Exception {
        runTests("""
            FROM employees, (FROM sample_data | STATS max_ts = MAX(@timestamp) BY does_not_exist)
            | KEEP emp_no, max_ts, does_not_exist
            """);
    }

    public void testSubqueryKeepMultipleUnmapped() throws Exception {
        runTests("""
            FROM employees,
                (FROM languages | KEEP language_code, unmapped1, unmapped2)
            | KEEP emp_no, language_code, unmapped1, unmapped2
            """);
    }

    public void testFork() throws Exception {
        runTests("""
            FROM employees
            | FORK (WHERE does_not_exist::LONG > 0)
                   (WHERE emp_no > 0)
            """);
    }

    public void testForkWithEval() throws Exception {
        runTests("""
            FROM employees
            | FORK (EVAL x = does_not_exist::DOUBLE + 1)
                   (EVAL y = emp_no + 1)
            """);
    }

    public void testForkWithStats() throws Exception {
        runTests("""
            FROM employees
            | FORK (STATS c = COUNT(*) BY does_not_exist)
                   (STATS d = AVG(salary::DOUBLE))
            | SORT does_not_exist
            """);
    }

    public void testCoalesce() throws Exception {
        runTests("""
            FROM employees
            | EVAL x = COALESCE(does_not_exist::LONG, emp_no, 0)
            | KEEP emp_no, x
            """);
    }

    public void testTBucketGroupByUnmapped() throws Exception {
        runTests("""
            FROM sample_data
            | STATS c = COUNT(*) BY tbucket(1 hour), does_not_exist
            """);
    }

    public void testTBucketAggregateUnmapped() throws Exception {
        runTests("""
            FROM sample_data
            | STATS s = SUM(does_not_exist::DOUBLE), c = COUNT(*) BY tbucket(1 day)
            """);
    }

    public void testTimeSeriesRateUnmapped() throws Exception {
        runTestsNullifyOnly("""
            TS k8s
            | STATS r = RATE(does_not_exist) BY tbucket(1 hour)
            """);
    }

    public void testTimeSeriesFirstOverTimeUnmapped() throws Exception {
        runTests("""
            TS k8s
            | STATS f = FIRST_OVER_TIME(does_not_exist::DOUBLE) BY tbucket(1 hour)
            """);
    }

    public void testPartiallyMappedField() throws Exception {
        runTests("""
            FROM sample_data, partial_mapping_sample_data
            | KEEP @timestamp, message, unmapped_message
            """);
    }

    public void testPartiallyMappedFieldWithStats() throws Exception {
        runTests("""
            FROM sample_data, partial_mapping_sample_data
            | STATS c = COUNT(*) BY unmapped_message
            """);
    }

    public void testPartiallyMappedFieldWithEval() throws Exception {
        runTests("""
            FROM sample_data, partial_mapping_sample_data
            | EVAL x = COALESCE(unmapped_message, "missing")
            | KEEP @timestamp, x
            """);
    }

    public void testDifferentTypesAcrossIndices() throws Exception {
        runTests("""
            FROM sample_data, sample_data_ts_long
            | KEEP @timestamp, message
            """);
    }

    public void testDifferentTypesWithCast() throws Exception {
        runTests("""
            FROM sample_data, sample_data_ts_long
            | EVAL ts = @timestamp::STRING
            | KEEP ts, message
            """);
    }

    @Override
    protected java.util.List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static String setUnmappedNullify(String query) {
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        return "SET unmapped_fields=\"load\"; " + query;
    }

    private void runTests(String query) {
        runTestsNullifyOnly(query);
        if (EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled()) {
            builder(setUnmappedLoad(query)).nestedPath("load").stages(STAGES).run();
        }
    }

    private void runTestsNullifyOnly(String query) {
        if (EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled()) {
            builder(setUnmappedNullify(query)).nestedPath("nullify").stages(STAGES).run();
        }
    }
}
