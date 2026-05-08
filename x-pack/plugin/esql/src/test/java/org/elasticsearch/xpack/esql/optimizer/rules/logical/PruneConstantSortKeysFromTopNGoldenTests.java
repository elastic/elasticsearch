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
    private static final EnumSet<Stage> STAGES_LOCAL = EnumSet.of(
        Stage.LOGICAL_OPTIMIZATION,
        Stage.LOCAL_PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE
    );

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

    public void testSortKeyFoldedToNullByArithmetic() {
        // emp_no + null folds to null via null-propagation in arithmetic (any value + null = null).
        // This folding happens in expression simplification before our rule runs, so by the time
        // PruneConstantSortKeysFromTopN sees the plan, the sort key is already Literal(null) — directly
        // foldable without needing alias resolution.
        runGoldenTest("""
            FROM employees
            | SORT emp_no + null, emp_no
            | LIMIT 20
            """, STAGES, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public void testUnmappedSortKeyNullifiedAndPruned() {
        // With unmapped_fields="nullify", does_not_exist has MissingEsField type (established at analysis time
        // via field-caps, meaning the field is absent from every shard). PruneConstantSortKeysFromTopN detects
        // this at logical optimization time and removes it from the TopN sort keys, leaving only emp_no.
        // With unmapped_fields="load", the field is PotentiallyUnmappedKeywordEsField (may exist on some shards),
        // so the rule does not fire and the TopN keeps both sort keys.
        runTestsNullifyAndLoad("""
            FROM employees
            | KEEP emp_no, does_not_exist
            | SORT does_not_exist, emp_no
            | LIMIT 20
            """, STAGES_LOCAL);
    }
}
