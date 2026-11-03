/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.EnumSet;

public class HoistProjectsGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> LOGICAL = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);
    private static final EnumSet<Stage> LOGICAL_AND_LOCAL_PHYSICAL = EnumSet.of(
        Stage.LOGICAL_OPTIMIZATION,
        Stage.LOCAL_PHYSICAL_OPTIMIZATION
    );

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public HoistProjectsGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testThreeLookupBlocks() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | DROP language_code
            | EVAL language_code = languages - 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_one
            | DROP language_code
            | EVAL language_code = languages - 2
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_two
            | DROP language_code
            """, LOGICAL_AND_LOCAL_PHYSICAL);
    }

    public void testThreeLookupBlocksThenStats() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | DROP language_code
            | EVAL language_code = languages - 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_one
            | DROP language_code
            | EVAL language_code = languages - 2
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_two
            | DROP language_code
            | STATS c = COUNT(*) BY lang, lang_minus_one, lang_minus_two
            """, LOGICAL_AND_LOCAL_PHYSICAL);
    }

    public void testThreeLookupBlocksParkingASourceColumnNamedLikeTheKey() {
        runGoldenTest("""
            FROM employees
            | RENAME languages AS language_code
            | RENAME language_code AS parked
            | EVAL language_code = parked
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | DROP language_code
            | RENAME parked AS language_code
            | RENAME language_code AS parked
            | EVAL language_code = parked - 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_one
            | DROP language_code
            | RENAME parked AS language_code
            | RENAME language_code AS parked
            | EVAL language_code = parked - 2
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_two
            | DROP language_code
            | RENAME parked AS language_code
            """, LOGICAL);
    }

    public void testThreeLookupBlocksWithFilterAndKeep() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, languages, salary
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | WHERE salary > 40000
            | EVAL language_code = languages - 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_one
            | LIMIT 100
            | EVAL language_code = languages - 2
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_two
            | DROP language_code
            """, LOGICAL_AND_LOCAL_PHYSICAL);
    }

    public void testLookupBlocksWithFilterDissectAndEnrichOnLookedUpColumns() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | DROP language_code
            | WHERE lang IS NOT NULL
            | DISSECT lang "%{lang_head}ch"
            | EVAL language_code = languages - 1
            | ENRICH languages ON language_code WITH enriched_minus_one = language_name
            | DROP language_code
            | EVAL language_code = languages - 2
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_two
            | DROP language_code
            """, LOGICAL);
    }

    public void testLookupBlocksShadowingSourceColumns() {
        runGoldenTest("""
            FROM employees
            | RENAME first_name AS original_first_name
            | LOOKUP JOIN test_lookup ON emp_no
            | RENAME first_name AS looked_up_once
            | LOOKUP JOIN test_lookup ON emp_no
            | RENAME first_name AS looked_up_twice
            | LOOKUP JOIN test_lookup ON emp_no
            | KEEP emp_no, original_first_name, looked_up_once, looked_up_twice, first_name
            """, LOGICAL);
    }

    public void testProjectBelowMvExpandStaysBelowIt() {
        runGoldenTest("""
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | MV_EXPAND job_positions
            | EVAL language_code = languages - 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang_minus_one
            | DROP language_code
            """, LOGICAL);
    }
}
