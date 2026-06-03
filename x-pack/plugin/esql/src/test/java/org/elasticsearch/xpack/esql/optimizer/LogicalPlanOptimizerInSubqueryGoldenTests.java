/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.util.EnumSet;

/**
 * Captures the analyzed and logically-optimized plans for IN/NOT IN subquery scenarios.
 */
public class LogicalPlanOptimizerInSubqueryGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN_SUBQUERY support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    public void testDisjunctiveInSubqueryAtTopLevel() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) OR salary > 50000
            """, STAGES);
    }

    public void testDisjunctiveInSubqueryInsideFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """, STAGES);
    }

    public void testDisjunctiveNotInSubqueryInsideFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """, STAGES);
    }

    public void testNestedDisjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM employees | KEEP salary) OR languages > 2
                | KEEP emp_no
              ) OR salary > 50000
            """, STAGES);
    }

    public void testDisjunctiveInSubqueryWithFork() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) OR salary > 50000
            | FORK (WHERE emp_no > 10000) (WHERE emp_no < 10050)
            """, STAGES);
    }

    public void testSortWithLimitInSubqueryIsAllowed() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | SORT emp_no | LIMIT 5 | KEEP emp_no)
            """, STAGES);
    }

    public void testStatsWithSortLimitInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | STATS m = MAX(emp_no) BY languages | SORT m | LIMIT 3 | KEEP m)
            """, STAGES);
    }

    public void testMultipleFiltersInSubqueryCombined() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | WHERE salary > 50000 | WHERE languages > 2 | KEEP emp_no)
            """, STAGES);
    }

    public void testCombineDisjunctionsInsideInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | WHERE salary == 50000 or salary == 10000 | KEEP emp_no)
            """, STAGES);
    }
}
