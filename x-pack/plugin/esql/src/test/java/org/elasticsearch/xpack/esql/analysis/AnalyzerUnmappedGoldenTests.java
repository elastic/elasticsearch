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
 * Golden tests for analyzer behavior with unmapped fields using SET unmapped_fields="nullify".
 * These tests verify that unmapped fields are properly handled by converting them to null literals.
 */
public class AnalyzerUnmappedGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    public void testKeep() throws Exception {
        runTests("""
            FROM test
            | KEEP does_not_exist_field
            """);
    }

    public void testKeepRepeated() throws Exception {
        runTests("""
            FROM test
            | KEEP does_not_exist_field, does_not_exist_field
            """);
    }

    public void testKeepAndMatchingStar() throws Exception {
        runTests("""
            FROM test
            | KEEP emp_*, does_not_exist_field
            """);
    }

    public void testEvalAndKeep() throws Exception {
        runTests("""
            FROM test
            | EVAL x = does_not_exist_field1::INTEGER + 42
            | KEEP does_not_exist_field1, does_not_exist_field2
            """);
    }

    public void testEvalAfterKeepStar() throws Exception {
        runTests("""
            FROM test
            | KEEP *
            | EVAL x = emp_no + 1
            | EVAL y = does_not_exist_field + 2
            """);
    }

    public void testEvalAfterMatchingKeepWithWildcard() throws Exception {
        runTests("""
            FROM test
            | KEEP emp_no, *
            | EVAL x = emp_no + 1
            | EVAL y = emp_does_not_exist_field + 2
            """);
    }

    public void testDrop() throws Exception {
        runTests("""
            FROM test
            | DROP does_not_exist_field, emp_no
            """);
    }

    public void testDropAndMatchingStar() throws Exception {
        runTests("""
            FROM test
            | DROP emp_*, does_not_exist_field
            """);
    }

    public void testRename() throws Exception {
        runTests("""
            FROM test
            | RENAME does_not_exist_field AS now_it_does, emp_no AS employee_number
            """);
    }

    public void testRenameRepeated() throws Exception {
        runTests("""
            FROM test
            | RENAME does_not_exist_field AS now_it_does, neither_does_this AS now_it_does, emp_no AS employee_number
            """);
    }

    public void testRenameEval() throws Exception {
        runTests("""
            FROM test
            | RENAME emp_no AS employee_number
            | EVAL x = does_not_exist + 1
            """);
    }

    public void testEval() throws Exception {
        runTests("""
            FROM test
            | EVAL x = does_not_exist_field + 1
            """);
    }

    public void testMultipleEval() throws Exception {
        runTests("""
            FROM test
            | EVAL a = 1
            | EVAL x = a + b
            | EVAL y = b + c
            """);
    }

    public void testEvalCast() throws Exception {
        runTests("""
            FROM test
            | EVAL x = does_not_exist_field::LONG
            """);
    }

    public void testEvalCastInPlace() throws Exception {
        runTests("""
            FROM test
            | EVAL does_not_exist_field::LONG
            """);
    }

    public void testEvalReplace() throws Exception {
        runTests("""
            FROM test
            | EVAL x = does_not_exist_field + 1
            | EVAL does_not_exist_field = 42
            """);
    }

    public void testKeepThenEval() throws Exception {
        runTests("""
            FROM test
            | KEEP does_not_exist_field
            | EVAL does_not_exist_field = 42
            """);
    }

    public void testStatsCount() throws Exception {
        runTests("""
            FROM test
            | STATS cnt = COUNT(does_not_exist_field)
            """);
    }

    public void testStatsBy() throws Exception {
        runTests("""
            FROM test
            | STATS BY does_not_exist_field
            """);
    }

    public void testStatsSumBy() throws Exception {
        runTests("""
            FROM test
            | STATS s = SUM(does_not_exist1) BY does_not_exist2
            """);
    }

    public void testStatsSumByMultiple() throws Exception {
        runTests("""
            FROM test
            | STATS s = SUM(does_not_exist1) + d2 BY d2 = does_not_exist2, emp_no
            """);
    }

    public void testStatsSumByComplex() throws Exception {
        runTests("""
            FROM test
            | STATS sum = SUM(does_not_exist1) + s0 + s1 BY s0 = does_not_exist2 + does_not_exist3, s1 = emp_no
            """);
    }

    public void testStatsMultiple() throws Exception {
        runTests("""
            FROM test
            | STATS s = SUM(does_not_exist1), c = COUNT(*) BY does_not_exist2, emp_no
            """);
    }

    public void testInlineStats() throws Exception {
        runTests("""
            FROM test
            | INLINE STATS s = SUM(does_not_exist1), c = COUNT(*) BY does_not_exist2, emp_no
            """);
    }

    public void testWhere() throws Exception {
        runTests("""
            FROM test
            | WHERE does_not_exist::LONG > 0
            """);
    }

    public void testWhereOr() throws Exception {
        runTests("""
            FROM test
            | WHERE does_not_exist::LONG > 0 OR emp_no > 0
            """);
    }

    public void testWhereComplex() throws Exception {
        runTests("""
            FROM test
            | WHERE does_not_exist1::LONG > 0 OR emp_no > 0 AND does_not_exist2::LONG < 100
            """);
    }

    public void testStatsCountWhere() throws Exception {
        runTests("""
            FROM test
            | STATS c = COUNT(*) WHERE does_not_exist1::LONG > 0
            """);
    }

    public void testStatsMultipleCountWhere() throws Exception {
        runTests("""
            FROM test
            | STATS c1 = COUNT(*) WHERE does_not_exist1::LONG > 0 OR emp_no > 0 OR does_not_exist2::LONG < 100,
                    c2 = COUNT(*) WHERE does_not_exist3 IS NULL
            """);
    }

    public void testSort() throws Exception {
        runTests("""
            FROM test
            | SORT does_not_exist ASC
            """);
    }

    public void testSortExpression() throws Exception {
        runTests("""
            FROM test
            | SORT does_not_exist::LONG + 1
            """);
    }

    public void testSortMultiple() throws Exception {
        runTests("""
            FROM test
            | SORT does_not_exist1::LONG + 1, does_not_exist2 DESC, emp_no ASC
            """);
    }

    public void testMvExpand() throws Exception {
        runTests("""
            FROM test
            | MV_EXPAND does_not_exist
            """);
    }

    public void testRow() throws Exception {
        runTests("""
            ROW x = 1
            | EVAL y = does_not_exist_field1::INTEGER + x
            | KEEP *, does_not_exist_field2
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
        if (EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled()) {
            builder(setUnmappedNullify(query)).nestedPath("nullify").stages(STAGES).run();
        }
        if (EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled()) {
            builder(setUnmappedLoad(query)).nestedPath("load").stages(STAGES).run();
        }
    }
}
