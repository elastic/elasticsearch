/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

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

    public void testProjectBelowMvExpandIsHoistedPastIt() {
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

    /**
     * Nothing below the Project is called {@code fn}, so the MV_EXPAND keeps its expanded attribute.
     */
    public void testMvExpandOverRenameIsHoistedPast() {
        runGoldenTest("""
            FROM employees
            | RENAME first_name AS fn
            | MV_EXPAND fn
            """, LOGICAL);
    }

    /**
     * Below the Project, {@code last_name} is still the original column that the RENAME shadows, so the expanded attribute
     * gets a temporary name and is renamed back above the MV_EXPAND.
     */
    public void testMvExpandShadowingAColumnBelowTheProjectGetsATemporaryName() {
        runGoldenTest("""
            FROM employees
            | RENAME first_name AS last_name
            | MV_EXPAND last_name
            """, LOGICAL);
    }

    /**
     * {@code original} still needs the unexpanded {@code first_name}, so it reads a copy of it instead. The expanded attribute
     * {@code fn} doesn't clash with anything and keeps its name.
     */
    public void testMvExpandOfAColumnStillNeededUnexpandedCopiesItFirst() {
        runGoldenTest("""
            FROM employees
            | EVAL original = first_name
            | RENAME first_name AS fn
            | MV_EXPAND fn
            """, LOGICAL);
    }

    /**
     * Copying the input and renaming the expanded attribute are independent; here, both are needed.
     */
    public void testMvExpandCopyingItsInputAndShadowingAColumnBelowTheProject() {
        runGoldenTest("""
            FROM employees
            | EVAL original = first_name
            | RENAME first_name AS last_name
            | MV_EXPAND last_name
            """, LOGICAL);
    }

    /**
     * The two aggregates are deduplicated into one column, so {@code b} is an alias of {@code a}. The first MV_EXPAND therefore
     * leaves a copy of {@code a} for {@code b}, and the second one expands that copy; the Project ends up above both.
     */
    public void testTwoMvExpandsOfDeduplicatedAggregates() {
        runGoldenTest("""
            ROW a = 1
            | STATS a = COUNT(*), b = COUNT(*)
            | MV_EXPAND a
            | MV_EXPAND b
            """, LOGICAL);
    }

    /**
     * The cast of the subquery union's {@code salary} is pushed into each union branch, which then outputs it as an extra
     * {@code $$salary$converted_to$keyword} column. The MV_EXPAND expands that column below the Project, where the union still
     * also outputs the original {@code salary}, so the expanded attribute gets a temporary name.
     */
    public void testMvExpandOfAUnionTypedColumnFromASubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runGoldenTest("""
            FROM employees, (FROM employees | KEEP salary)
            | EVAL salary = salary::keyword
            | KEEP salary
            | MV_EXPAND salary
            """, LOGICAL);
    }

    /**
     * Like {@link #testMvExpandOfAUnionTypedColumnFromASubquery}, but {@code tmp} still needs the unexpanded union-typed
     * column, so it reads a copy of it; the expanded attribute still gets a temporary name.
     */
    public void testMvExpandOfAUnionTypedColumnFromASubqueryStillNeededUnexpanded() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runGoldenTest("""
            FROM employees, (FROM employees | KEEP salary)
            | EVAL salary = salary::keyword
            | EVAL tmp = salary
            | KEEP salary, tmp
            | MV_EXPAND salary
            """, LOGICAL);
    }

    /**
     * Like {@link #testMvExpandOfAUnionTypedColumnFromASubqueryStillNeededUnexpanded}, but expanding the alias {@code tmp}: now
     * {@code salary} reads the copy, and {@code tmp} doesn't clash with anything below the Project, so it keeps its name.
     */
    public void testMvExpandOfAnAliasOfAUnionTypedColumnFromASubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runGoldenTest("""
            FROM employees, (FROM employees | KEEP salary)
            | EVAL salary = salary::keyword
            | EVAL tmp = salary
            | KEEP salary, tmp
            | MV_EXPAND tmp
            """, LOGICAL);
    }

    /**
     * The renamed {@code c} reads the original {@code a}, which the second EVAL shadows. The MV_EXPAND must expand the original
     * {@code a} below the Project, next to the shadowing one, which gets a temporary name.
     */
    public void testMvExpandOfARenameWhoseSourceColumnIsShadowed() {
        runGoldenTest("""
            ROW a = 2
            | EVAL c = a
            | EVAL a = 12
            | RENAME c AS b
            | KEEP *
            | MV_EXPAND b
            """, LOGICAL);
    }
}
