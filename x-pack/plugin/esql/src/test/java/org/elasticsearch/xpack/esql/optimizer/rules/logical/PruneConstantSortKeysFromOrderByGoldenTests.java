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

public class PruneConstantSortKeysFromOrderByGoldenTests extends UnmappedGoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);
    private static final EnumSet<Stage> STAGES_LOCAL = EnumSet.of(
        Stage.LOGICAL_OPTIMIZATION,
        Stage.LOCAL_PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE
    );

    public void testConstantSortKeyViaEval() {
        // EVAL x = null makes the sort key foldable, so the sort is dropped.
        runGoldenTest("""
            FROM employees
            | EVAL x = null
            | SORT x
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testPartiallyConstantSortKey() {
        // x = 1 is a non-null constant, so only that key is pruned; emp_no stays.
        runGoldenTest("""
            FROM employees
            | EVAL x = 1
            | SORT x, emp_no
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testSortKeyFoldedToNullByArithmetic() {
        // emp_no + null folds to null in expression simplification before this rule runs, so the key is
        // already Literal(null) — no alias resolution needed.
        runGoldenTest("""
            FROM employees
            | SORT emp_no + null, emp_no
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testUnmappedSortKeyNullifiedAndPruned() {
        // nullify: does_not_exist is MissingEsField (absent from every shard) → pruned.
        // load: PotentiallyUnmappedKeywordEsField (may exist on some shards) → kept.
        runTestsNullifyAndLoad("""
            FROM employees
            | KEEP emp_no, does_not_exist
            | SORT does_not_exist, emp_no
            | LIMIT 20
            """, STAGES_LOCAL, null);
    }
}
