/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class EsqlReductionLateMaterializationGoldenTests extends GoldenTestCase {
    /*
    For instance, I'd like to see the data (and reduce) plans for cases such as:
    queries with more than 1 topn
    queries that have mv_expand (known problem due to preventing projections from bubbling up)
    queries that have lookup joins on the data node
    queries that have subsequent reducers that should end up on the coordinator (e.g. topn first, then stats)
    queries where some fields need to be extracted before the topn, and others don't
    queries where the topn sorts on expressions and/or sorts on more than 1 field
    queries where the topn can be pushed down to Lucene in the data node plan, and queries where this isn't possible.
    queries where the fields required for the TopN are missing on the data node (interactions with ReplaceFieldWithConstantOrNull can be subtle and only pop up as bugs months later)
     */

    public void testMultipleTopN() throws Exception {
        String query = """
            FROM employees
            | SORT hire_date
            | LIMIT 20
            | SORT salary
            | LIMIT 10
            """;
        runGoldenTest(query, EnumSet.of(Stage.NODE_REDUCE));
    }
}
