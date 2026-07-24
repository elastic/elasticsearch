/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

/**
 * Analyzer behavior for nonbranching aggregations with unmapped fields.
 */
public class AnalyzerUnmappedStatsGoldenTests extends AnalyzerUnmappedGoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerUnmappedStatsGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testStatsAggNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS cnt = COUNT(does_not_exist_field)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAgg").nestedPath("nullify").run();
    }

    public void testStatsAggLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS cnt = COUNT(does_not_exist_field)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAgg").nestedPath("load").run();
    }

    public void testStatsGroupNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS BY does_not_exist_field
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsGroup").nestedPath("nullify").run();
    }

    public void testStatsGroupLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS BY does_not_exist_field
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsGroup").nestedPath("load").run();
    }

    public void testDoesNotExistAfterInlineStatsNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | INLINE STATS COUNT(*) BY emp_no
            | EVAL x = does_not_exist_field
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testDoesNotExistAfterInlineStats").nestedPath("nullify").run();
    }

    public void testDoesNotExistAfterInlineStatsLoad() throws Exception {
        builder(load("""
            FROM employees
            | INLINE STATS COUNT(*) BY emp_no
            | EVAL x = does_not_exist_field
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testDoesNotExistAfterInlineStats").nestedPath("load").run();
    }

    public void testStatsAggAndGroupNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndGroup").nestedPath("nullify").run();
    }

    public void testStatsAggAndGroupLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndGroup").nestedPath("load").run();
    }

    public void testInlineStatsAggAndGroupNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | INLINE STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testInlineStatsAggAndGroup").nestedPath("nullify").run();
    }

    public void testInlineStatsAggAndGroupLoad() throws Exception {
        builder(load("""
            FROM employees
            | INLINE STATS s = SUM(does_not_exist1::DOUBLE) BY does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testInlineStatsAggAndGroup").nestedPath("load").run();
    }

    public void testStatsAggAndAliasedGroupNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) + d2 BY d2 = does_not_exist2::DOUBLE, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedGroup").nestedPath("nullify").run();
    }

    public void testStatsAggAndAliasedGroupLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE) + d2 BY d2 = does_not_exist2::DOUBLE, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedGroup").nestedPath("load").run();
    }

    public void testStatsAggAndAliasedGroupWithExpressionNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS sum = SUM(does_not_exist1::DOUBLE) + s0 + s1 BY s0 = does_not_exist2::DOUBLE + does_not_exist3::DOUBLE, s1 = emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedGroupWithExpression")
            .nestedPath("nullify")
            .run();
    }

    public void testStatsAggAndAliasedGroupWithExpressionLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS sum = SUM(does_not_exist1::DOUBLE) + s0 + s1 BY s0 = does_not_exist2::DOUBLE + does_not_exist3::DOUBLE, s1 = emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedGroupWithExpression").nestedPath("load").run();
    }

    public void testStatsMixedNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsMixed").nestedPath("nullify").run();
    }

    public void testStatsMixedLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsMixed").nestedPath("load").run();
    }

    public void testStatsMixedAndExpressionsNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS s = SUM(does_not_exist1) + does_not_exist2, c = COUNT(*) BY does_not_exist3, emp_no, does_not_exist2
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsMixedAndExpressions").nestedPath("nullify").run();
    }

    public void testInlineStatsMixedNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | INLINE STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testInlineStatsMixed").nestedPath("nullify").run();
    }

    public void testInlineStatsMixedLoad() throws Exception {
        builder(load("""
            FROM employees
            | INLINE STATS s = SUM(does_not_exist1::DOUBLE), c = COUNT(*) BY does_not_exist2, emp_no
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testInlineStatsMixed").nestedPath("load").run();
    }

    public void testAggsFilteringNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS c = COUNT(*) WHERE does_not_exist1::LONG > 0
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testAggsFiltering").nestedPath("nullify").run();
    }

    public void testAggsFilteringLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS c = COUNT(*) WHERE does_not_exist1::LONG > 0
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testAggsFiltering").nestedPath("load").run();
    }

    public void testAggsFilteringMultipleFieldsNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | STATS c1 = COUNT(*) WHERE does_not_exist1::LONG > 0 OR emp_no > 0 OR does_not_exist2::LONG < 100,
                    c2 = COUNT(*) WHERE does_not_exist3 IS NULL
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testAggsFilteringMultipleFields").nestedPath("nullify").run();
    }

    public void testAggsFilteringMultipleFieldsLoad() throws Exception {
        builder(load("""
            FROM employees
            | STATS c1 = COUNT(*) WHERE does_not_exist1::LONG > 0 OR emp_no > 0 OR does_not_exist2::LONG < 100,
                    c2 = COUNT(*) WHERE does_not_exist3 IS NULL
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testAggsFilteringMultipleFields").nestedPath("load").run();
    }

    public void testStatsAggAndAliasedShadowingGroupOverExpressionNullify() throws Exception {
        builder(nullify("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) + language_code
                    BY language_code = does_not_exist1::INTEGER + does_not_exist2::INTEGER + language_code, language_name
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedShadowingGroupOverExpression")
            .nestedPath("nullify")
            .run();
    }

    public void testStatsAggAndAliasedShadowingGroupOverExpressionLoad() throws Exception {
        builder(load("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) + language_code
                    BY language_code = does_not_exist1::INTEGER + does_not_exist2::INTEGER + language_code, language_name
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedShadowingGroupOverExpression")
            .nestedPath("load")
            .run();
    }

    public void testStatsAggAndAliasedShadowingGroupNullify() throws Exception {
        builder(nullify("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) BY language_code = does_not_exist, language_name
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedShadowingGroup").nestedPath("nullify").run();
    }

    public void testStatsAggAndAliasedShadowingGroupLoad() throws Exception {
        builder(load("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) BY language_code = does_not_exist, language_name
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsAggAndAliasedShadowingGroup").nestedPath("load").run();
    }

    public void testTBucketGroupByUnmappedNullify() throws Exception {
        builder(nullify("""
            FROM sample_data
            | STATS c = COUNT(*) BY tbucket(1 hour), does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTBucketGroupByUnmapped").nestedPath("nullify").run();
    }

    public void testTBucketGroupByUnmappedLoad() throws Exception {
        builder(load("""
            FROM sample_data
            | STATS c = COUNT(*) BY tbucket(1 hour), does_not_exist
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTBucketGroupByUnmapped").nestedPath("load").run();
    }

    public void testTBucketAggregateUnmappedNullify() throws Exception {
        builder(nullify("""
            FROM sample_data
            | STATS s = SUM(does_not_exist::DOUBLE), c = COUNT(*) BY tbucket(1 day)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTBucketAggregateUnmapped").nestedPath("nullify").run();
    }

    public void testTBucketAggregateUnmappedLoad() throws Exception {
        builder(load("""
            FROM sample_data
            | STATS s = SUM(does_not_exist::DOUBLE), c = COUNT(*) BY tbucket(1 day)
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTBucketAggregateUnmapped").nestedPath("load").run();
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/143991
     * Unmapped fields with dotted names (e.g. host.entity.id) should be nullified in STATS WHERE, even when an EVAL before the STATS
     * creates a field whose name is a suffix of the unmapped field name (e.g. entity.id).
     */
    public void testStatsFilteredAggAfterEvalWithDottedUnmappedFieldNullify() throws Exception {
        builder(nullify("""
            ROW x = 1
            | EVAL entity.id = "foo"
            | STATS host.entity.id = VALUES(host.entity.id) WHERE host.entity.id IS NOT NULL BY entity.id
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsFilteredAggAfterEvalWithDottedUnmappedField")
            .nestedPath("nullify")
            .run();
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/143991
     * Same as {@link #testStatsFilteredAggAfterEvalWithDottedUnmappedFieldNullify()} but with FROM instead of ROW.
     * The nullify and load variants have the same plan shape; only the field type in the EsRelation differs.
     */
    public void testStatsFilteredAggAfterEvalWithDottedUnmappedFieldFromIndexNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL entity.id = "foo"
            | STATS host.entity.id = VALUES(host.entity.id) WHERE host.entity.id IS NOT NULL BY entity.id
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsFilteredAggAfterEvalWithDottedUnmappedFieldFromIndex")
            .nestedPath("nullify")
            .run();
    }

    public void testStatsFilteredAggAfterEvalWithDottedUnmappedFieldFromIndexLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL entity.id = "foo"
            | STATS host.entity.id = VALUES(host.entity.id) WHERE host.entity.id IS NOT NULL BY entity.id
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testStatsFilteredAggAfterEvalWithDottedUnmappedFieldFromIndex")
            .nestedPath("load")
            .run();
    }
}
