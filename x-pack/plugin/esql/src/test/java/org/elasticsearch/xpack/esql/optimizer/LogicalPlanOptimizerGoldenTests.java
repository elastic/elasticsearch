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
 * Golden tests for the {@link LogicalPlanOptimizer}.
 */
public class LogicalPlanOptimizerGoldenTests extends UnmappedGoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);

    public void testLookupJoinNullMatchField() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | EVAL language_code = does_not_exist :: INTEGER
            | LOOKUP JOIN languages_lookup ON language_code
            """, STAGES);
    }

    public void testEnrichNullMatchField() throws Exception {
        runTestsNullifyOnly("""
            FROM employees
            | ENRICH languages ON does_not_exist
            """, STAGES);
    }

    public void testInlineStatsGroupByNullField() throws Exception {
        assumeTrue("requires INLINE STATS", EsqlCapabilities.Cap.INLINESTATS.isEnabled());
        runTestsNullifyOnly("""
            FROM employees
            | INLINE STATS s = MAX(salary) BY does_not_exist
            """, STAGES);
    }
}
