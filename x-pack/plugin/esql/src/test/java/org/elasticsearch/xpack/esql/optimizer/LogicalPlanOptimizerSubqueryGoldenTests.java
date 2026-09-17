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

/**
 * Captures the analyzed and logically-optimized plans for subquery-in-{@code FROM} scenarios.
 * Negative tests live in {@code LogicalPlanOptimizerSubqueryTests}.
 */
public class LogicalPlanOptimizerSubqueryGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LogicalPlanOptimizerSubqueryGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    public void testMatchAfterSubquerySortWithoutLimit() {
        runGoldenTest("""
            FROM (FROM employees | SORT first_name), (FROM employees | WHERE emp_no > 0)
            | WHERE match(first_name, "Meditation")
            """, STAGES);
    }

    public void testMatchOperatorAfterSubquerySortWithoutLimit() {
        runGoldenTest("""
            FROM (FROM employees | SORT first_name), (FROM employees | WHERE emp_no > 0)
            | WHERE first_name:"Meditation"
            """, STAGES);
    }

    public void testMatchPhraseAfterSubquerySortWithoutLimit() {
        runGoldenTest("""
            FROM (FROM employees | SORT first_name), (FROM employees | WHERE emp_no > 0)
            | WHERE match_phrase(first_name, "Meditation")
            """, STAGES);
    }
}
