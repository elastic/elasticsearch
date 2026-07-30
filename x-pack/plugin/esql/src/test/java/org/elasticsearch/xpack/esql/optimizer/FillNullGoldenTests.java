/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.EnumSet;

/**
 * Golden coverage for the FILLNULL surrogate: ANALYSIS pins the fill aliases as plan-tree state and
 * LOGICAL_OPTIMIZATION pins the expansion into {@code Project(Eval(COALESCE...))}, including chained NameId stability.
 */
public class FillNullGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
    }

    public void testFillNullWithValueSingleField() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, gender
            | FILLNULL "Unknown" ON gender
            """, STAGES);
    }

    public void testFillNullAllFieldsTypeDefaults() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | FILLNULL DEFAULT ON *
            """, STAGES);
    }

    public void testFillNullAllFieldsWithValueSkipsIncompatible() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | FILLNULL "Unknown" ON *
            """, STAGES);
    }

    public void testFillNullChainedPreservesNameIds() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, gender, hire_date
            | FILLNULL "Unknown" ON gender
            | FILLNULL 0 ON emp_no
            """, STAGES);
    }

    public void testFillNullBetweenEvalsIsCombined() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, languages
            | EVAL languages = languages + 1
            | FILLNULL 0 ON languages
            | EVAL bonus = languages + 10
            """, STAGES);
    }

    public void testFillNullThenKeepReorders() {
        runGoldenTest("""
            FROM employees
            | FILLNULL "Unknown" ON gender
            | KEEP first_name, gender, emp_no
            """, STAGES);
    }
}
