/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.optimizer.UnmappedGoldenTestCase;

import java.util.EnumSet;

public class PruneConstantSortKeysFromTopNGoldenTests extends UnmappedGoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);
    private static final EnumSet<Stage> STAGES_LOCAL = EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    public void testConstantSortKeyViaEval() {
        // EVAL x = null makes the sort key foldable; PruneConstantSortKeysFromTopN replaces TopN with Limit.
        runGoldenTest("""
            FROM employees
            | EVAL x = null
            | SORT x
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testPartiallyConstantSortKey() {
        // x=1 is a non-null constant and is pruned from the sort order; TopN is rebuilt with only emp_no.
        runGoldenTest("""
            FROM employees
            | EVAL x = 1
            | SORT x, emp_no
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testUnmappedSortKeyNullifiedAndPruned() {
        // With unmapped_fields="nullify", ReplaceFieldWithConstantOrNull turns the missing field into null in
        // the local optimizer; PruneConstantSortKeysFromTopN then prunes it, leaving only emp_no in the sort.
        // With unmapped_fields="load", the field is retained as a potentially-unmapped keyword, so the rule
        // does not fire and the TopN keeps both sort keys.
        runTestsNullifyAndLoad("""
            FROM employees
            | KEEP emp_no, does_not_exist
            | SORT does_not_exist, emp_no
            | LIMIT 20
            """, STAGES_LOCAL);
    }
}
