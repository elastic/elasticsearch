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
 * Nonbranching analyzer behavior for core commands and single-flow pipelines with unmapped fields.
 * New features should use a focused {@link AnalyzerUnmappedGoldenTestCase} subclass instead of growing this legacy core suite.
 */
public class AnalyzerUnmappedLinearGoldenTests extends AnalyzerUnmappedGoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerUnmappedLinearGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testKeepNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | keep does_not_exist_field
            """)).run();
    }

    public void testKeepLoad() throws Exception {
        builder(load("""
            FROM employees
            | keep does_not_exist_field
            """)).run();
    }

    public void testKeepRepeatedNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | KEEP does_not_exist_field, does_not_exist_field
            """)).run();
    }

    public void testKeepRepeatedLoad() throws Exception {
        builder(load("""
            FROM employees
            | KEEP does_not_exist_field, does_not_exist_field
            """)).run();
    }

    public void testKeepAndMatchingStarNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | KEEP emp_*, does_not_exist_field
            """)).run();
    }

    public void testKeepAndMatchingStarLoad() throws Exception {
        builder(load("""
            FROM employees
            | KEEP emp_*, does_not_exist_field
            """)).run();
    }

    public void testEvalAndKeepNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL x = does_not_exist_field1::INTEGER + 42
            | KEEP does_not_exist_field1, does_not_exist_field2
            """)).run();
    }

    public void testEvalAndKeepLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL x = does_not_exist_field1::INTEGER + 42
            | KEEP does_not_exist_field1, does_not_exist_field2
            """)).run();
    }

    public void testEvalAfterKeepStarNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | KEEP *
            | EVAL x = emp_no + 1
            | EVAL y = does_not_exist_field::DOUBLE + 2
            """)).run();
    }

    public void testEvalAfterKeepStarLoad() throws Exception {
        builder(load("""
            FROM employees
            | KEEP *
            | EVAL x = emp_no + 1
            | EVAL y = does_not_exist_field::DOUBLE + 2
            """)).run();
    }

    public void testEvalAfterMatchingKeepWithWildcardNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | KEEP emp_no, *
            | EVAL x = emp_no + 1
            | EVAL y = emp_does_not_exist_field::DOUBLE + 2
            """)).run();
    }

    public void testEvalAfterMatchingKeepWithWildcardLoad() throws Exception {
        builder(load("""
            FROM employees
            | KEEP emp_no, *
            | EVAL x = emp_no + 1
            | EVAL y = emp_does_not_exist_field::DOUBLE + 2
            """)).run();
    }

    public void testDropAnotherFieldNullify() throws Exception {
        builder(nullify("""
            FROM employees | DROP does_not_exist_field, does_not_exist_field2
            """)).run();
    }

    public void testDropEmptyNullify() throws Exception {
        builder(nullify("""
            FROM employees | DROP does_not_exist_field
            """)).run();
    }

    public void testDropExistsNullify() throws Exception {
        builder(nullify("""
            FROM employees | DROP does_not_exist_field, emp_no
            """)).run();
    }

    public void testDropSameFieldNullify() throws Exception {
        builder(nullify("""
            FROM employees | DROP does_not_exist_field, does_not_exist_field
            """)).run();
    }

    public void testDropAnotherFieldLoad() throws Exception {
        builder(load("""
            FROM employees | DROP does_not_exist_field, does_not_exist_field2
            """)).run();
    }

    public void testDropEmptyLoad() throws Exception {
        builder(load("""
            FROM employees | DROP does_not_exist_field
            """)).run();
    }

    public void testDropExistsLoad() throws Exception {
        builder(load("""
            FROM employees | DROP does_not_exist_field, emp_no
            """)).run();
    }

    public void testDropSameFieldLoad() throws Exception {
        builder(load("""
            FROM employees | DROP does_not_exist_field, does_not_exist_field
            """)).run();
    }

    public void testDropWithMatchingStarNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | DROP emp_*, does_not_exist_field
            """)).run();
    }

    public void testDropWithMatchingStarLoad() throws Exception {
        builder(load("""
            FROM employees
            | DROP emp_*, does_not_exist_field
            """)).run();
    }

    // A pattern DROP that doesn't match the missing field still lets nullify inject it for a later KEEP.
    public void testDropPatternThenKeepMissingNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | DROP emp_*
            | KEEP does_not_exist_field
            """)).run();
    }

    public void testRenameNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, emp_no AS employee_number
            """)).run();
    }

    public void testRenameLoad() throws Exception {
        builder(load("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, emp_no AS employee_number
            """)).run();
    }

    public void testRenameShadowedNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, neither_does_this AS now_it_does, emp_no AS employee_number
            """)).run();
    }

    public void testRenameShadowedLoad() throws Exception {
        builder(load("""
            FROM employees
            | RENAME does_not_exist_field AS now_it_does, neither_does_this AS now_it_does, emp_no AS employee_number
            """)).run();
    }

    public void testEvalAfterRenameNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | RENAME emp_no AS employee_number
            | EVAL x = does_not_exist::DOUBLE + 1
            """)).run();
    }

    public void testEvalAfterRenameLoad() throws Exception {
        builder(load("""
            FROM employees
            | RENAME emp_no AS employee_number
            | EVAL x = does_not_exist::DOUBLE + 1
            """)).run();
    }

    public void testEvalNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            """)).run();
    }

    public void testEvalLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            """)).run();
    }

    public void testEvalReplacesUnmappedFieldFromEmptyMappingLoad() throws Exception {
        builder(load("""
            FROM no_mapping_date_extract_fields
            | EVAL date_string = date_string::date
            """)).run();
    }

    public void testMultipleEvalNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL a = 1
            | EVAL x = a + b::DOUBLE
            | EVAL y = b::DOUBLE + c::DOUBLE
            """)).run();
    }

    public void testMultipleEvalLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL a = 1
            | EVAL x = a + b::DOUBLE
            | EVAL y = b::DOUBLE + c::DOUBLE
            """)).run();
    }

    public void testCastingNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL x = does_not_exist_field::LONG
            """)).run();
    }

    public void testCastingLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL x = does_not_exist_field::LONG
            """)).run();
    }

    public void testCastingNoAliasingNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL does_not_exist_field::LONG
            """)).run();
    }

    public void testCastingNoAliasingLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL does_not_exist_field::LONG
            """)).run();
    }

    public void testShadowingAfterEvalNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            | EVAL does_not_exist_field = 42
            """)).run();
    }

    public void testShadowingAfterEvalLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL x = does_not_exist_field::DOUBLE + 1
            | EVAL does_not_exist_field = 42
            """)).run();
    }

    public void testShadowingAfterKeepNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | KEEP does_not_exist_field
            | EVAL does_not_exist_field = 42
            """)).run();
    }

    public void testShadowingAfterKeepLoad() throws Exception {
        builder(load("""
            FROM employees
            | KEEP does_not_exist_field
            | EVAL does_not_exist_field = 42
            """)).run();
    }

    public void testWhereNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE does_not_exist::LONG > 0
            """)).run();
    }

    public void testWhereLoad() throws Exception {
        builder(load("""
            FROM employees
            | WHERE does_not_exist::LONG > 0
            """)).run();
    }

    public void testWhereOrNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE does_not_exist::LONG > 0 OR emp_no > 0
            """)).run();
    }

    public void testWhereOrLoad() throws Exception {
        builder(load("""
            FROM employees
            | WHERE does_not_exist::LONG > 0 OR emp_no > 0
            """)).run();
    }

    public void testWhereComplexNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE does_not_exist1::LONG > 0 OR emp_no > 0 AND does_not_exist2::LONG < 100
            """)).run();
    }

    public void testWhereComplexLoad() throws Exception {
        builder(load("""
            FROM employees
            | WHERE does_not_exist1::LONG > 0 OR emp_no > 0 AND does_not_exist2::LONG < 100
            """)).run();
    }

    public void testSortNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | SORT does_not_exist ASC
            """)).run();
    }

    public void testSortLoad() throws Exception {
        builder(load("""
            FROM employees
            | SORT does_not_exist ASC
            """)).run();
    }

    public void testSortExpressionNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | SORT does_not_exist::LONG + 1
            """)).run();
    }

    public void testSortExpressionLoad() throws Exception {
        builder(load("""
            FROM employees
            | SORT does_not_exist::LONG + 1
            """)).run();
    }

    public void testSortExpressionMultipleFieldsNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | SORT does_not_exist1::LONG + 1, does_not_exist2 DESC, emp_no ASC
            """)).run();
    }

    public void testSortExpressionMultipleFieldsLoad() throws Exception {
        builder(load("""
            FROM employees
            | SORT does_not_exist1::LONG + 1, does_not_exist2 DESC, emp_no ASC
            """)).run();
    }

    public void testMvExpandNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | MV_EXPAND does_not_exist
            """)).run();
    }

    public void testMvExpandLoad() throws Exception {
        builder(load("""
            FROM employees
            | MV_EXPAND does_not_exist
            """)).run();
    }

    public void testLookupJoinNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL language_code = does_not_exist :: INTEGER
            | LOOKUP JOIN languages_lookup ON language_code
            """)).run();
    }

    public void testLookupJoinWithFilterNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE does_not_exist::LONG > 0
            """)).run();
    }

    public void testCoalesceNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL x = COALESCE(does_not_exist::LONG, emp_no, 0)
            | KEEP emp_no, x
            """)).run();
    }

    public void testCoalesceLoad() throws Exception {
        builder(load("""
            FROM employees
            | EVAL x = COALESCE(does_not_exist::LONG, emp_no, 0)
            | KEEP emp_no, x
            """)).run();
    }

    public void testEnrichNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | EVAL x = does_not_exist::KEYWORD
            | ENRICH languages ON x
            """)).run();
    }

    public void testSemanticTextNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | WHERE KNN(does_not_exist, [0, 1, 2])
            """)).run();
    }

    public void testRowNullify() throws Exception {
        builder(nullify("""
            ROW x = 1
            | EVAL y = does_not_exist_field1::INTEGER + x
            | KEEP *, does_not_exist_field2
            """)).run();
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/142968
     * KQL (and QSTR) functions should be allowed in WHERE immediately after FROM,
     * even when an unmapped field is referenced later in the query.
     */
    public void testKqlWithUnmappedFieldInEvalNullify() throws Exception {
        // This should NOT throw a verification exception.
        // The KQL function is correctly placed in a WHERE directly after FROM.
        builder(nullify("""
            FROM employees
            | WHERE kql("first_name: test")
            | EVAL x = does_not_exist_field + 1
            """)).run();
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/142959
     * QSTR functions should be allowed after SORT, even when an unmapped field is used later.
     */
    public void testQstrAfterSortWithUnmappedFieldNullify() throws Exception {
        builder(nullify("""
            FROM employees
            | SORT first_name
            | WHERE qstr("first_name: test")
            | EVAL x = does_not_exist_field + 1
            """)).run();
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/141870
     * ResolveRefs processes the EVAL only after ImplicitCasting processes the implicit cast in the WHERE.
     * This means that ResolveUnmapped will see the EVAL with a yet-to-be-resolved reference to nanos.
     * It should not treat it as unmapped, because there is clearly a nanos attribute in the EVAL's input.
     */
    public void testDoNotResolveUnmappedFieldPresentInChildrenNullify() throws Exception {
        builder(nullify("""
            ROW millis = "1970-01-01T00:00:00Z"::date, nanos = "1970-01-01T00:00:00Z"::date_nanos
            | SORT millis ASC
            | WHERE millis < "2000-01-01"
            | EVAL nanos = MV_MIN(nanos)
            """)).run();
    }

    public void testDoNotResolveUnmappedFieldPresentInChildrenLoad() throws Exception {
        builder(load("""
            ROW millis = "1970-01-01T00:00:00Z"::date, nanos = "1970-01-01T00:00:00Z"::date_nanos
            | SORT millis ASC
            | WHERE millis < "2000-01-01"
            | EVAL nanos = MV_MIN(nanos)
            """)).run();
    }
}
