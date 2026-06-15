/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/**
 * Golden tests for {@link ReplaceMvCountOfValuesWithCountDistinct}. The {@code ANALYSIS} stage captures the
 * pre-rewrite plan ({@code MV_COUNT(VALUES(x))}) and {@code LOGICAL_OPTIMIZATION} captures the result, so the
 * expected files show the full rewrite: a single {@code COUNT_DISTINCT} aggregate plus the
 * {@code CASE(... == 0, null, TO_INTEGER(...))} eval that preserves the {@code integer} type and the null-on-empty
 * group semantics.
 */
public class ReplaceMvCountOfValuesWithCountDistinctGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testRewrite() {
        runGoldenTest("""
            FROM employees
            | STATS c = MV_COUNT(VALUES(salary))
            """, STAGES);
    }

    public void testRewriteWithGrouping() {
        runGoldenTest("""
            FROM employees
            | STATS c = MV_COUNT(VALUES(salary)) BY gender
            """, STAGES);
    }

    public void testRewriteKeywordField() {
        runGoldenTest("""
            FROM employees
            | STATS c = MV_COUNT(VALUES(gender))
            """, STAGES);
    }

    public void testDuplicatesDeduplicatedToSingleCountDistinct() {
        runGoldenTest("""
            FROM employees
            | STATS a = MV_COUNT(VALUES(salary)), b = MV_COUNT(VALUES(salary))
            """, STAGES);
    }

    public void testMixedWithOtherAggregates() {
        runGoldenTest("""
            FROM employees
            | STATS c = MV_COUNT(VALUES(salary)), m = MAX(salary), n = COUNT(*) BY gender
            """, STAGES);
    }

    public void testFilterCarriedOntoCountDistinct() {
        runGoldenTest("""
            FROM employees
            | STATS c = MV_COUNT(VALUES(salary)) WHERE gender == "M"
            """, STAGES);
    }

    public void testInlineStats() {
        runGoldenTest("""
            FROM employees
            | INLINE STATS c = MV_COUNT(VALUES(salary))
            """, STAGES);
    }

    // VALUES is also emitted, so its materialization is needed regardless: MV_COUNT keeps the exact path.
    public void testValuesEmittedElsewhereNotRewritten() {
        runGoldenTest("""
            FROM employees
            | STATS v = VALUES(salary), c = MV_COUNT(VALUES(salary))
            """, STAGES);
    }

    // COUNT_DISTINCT does not support geo_point, so the exact VALUES path is preserved.
    public void testUnsupportedTypeNotRewritten() {
        runGoldenTest("""
            FROM airports
            | STATS c = MV_COUNT(VALUES(location))
            """, STAGES);
    }

    // MV_COUNT over a non-VALUES aggregate must be left untouched.
    public void testMvCountNotOverValuesUntouched() {
        runGoldenTest("""
            FROM employees
            | STATS c = MV_COUNT(TOP(salary, 3, "asc"))
            """, STAGES);
    }
}
