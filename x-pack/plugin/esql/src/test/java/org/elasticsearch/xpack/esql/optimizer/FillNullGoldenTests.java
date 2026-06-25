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
 * Golden coverage for the FILLNULL surrogate command. The ANALYSIS stage pins how the fill aliases render as
 * proper plan-tree state (the {@code col = COALESCE(col, default)} aliases live in NodeInfo, like Eval.fields),
 * and the LOGICAL_OPTIMIZATION stage pins the surrogate expansion into {@code Project(Eval(COALESCE...))} after
 * {@code SubstituteSurrogatePlans}, including NameId stability across chained instances.
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
            | FILLNULL WITH "Unknown" gender
            """, STAGES);
    }

    public void testFillNullAllFieldsTypeDefaults() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | FILLNULL
            """, STAGES);
    }

    public void testFillNullAllFieldsWithValueSkipsIncompatible() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, salary, gender
            | FILLNULL WITH "Unknown"
            """, STAGES);
    }

    public void testFillNullChainedPreservesNameIds() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, gender, hire_date
            | FILLNULL WITH "Unknown" gender
            | FILLNULL WITH 0 emp_no
            """, STAGES);
    }

    public void testFillNullBetweenEvalsIsCombined() {
        runGoldenTest("""
            FROM employees
            | KEEP emp_no, languages
            | EVAL languages = languages + 1
            | FILLNULL WITH 0 languages
            | EVAL bonus = languages + 10
            """, STAGES);
    }

    public void testFillNullThenKeepReorders() {
        runGoldenTest("""
            FROM employees
            | FILLNULL WITH "Unknown" gender
            | KEEP first_name, gender, emp_no
            """, STAGES);
    }
}
