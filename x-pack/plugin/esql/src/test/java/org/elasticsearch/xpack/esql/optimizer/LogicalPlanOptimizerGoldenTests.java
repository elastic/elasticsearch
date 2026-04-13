/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

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
}
