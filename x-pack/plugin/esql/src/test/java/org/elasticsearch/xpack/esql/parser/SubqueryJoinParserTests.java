/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;

public class SubqueryJoinParserTests extends AbstractStatementParserTests {

    /**
     * The grammar accepts these targets, so parsing reaches {@link LogicalPlanBuilder#visitJoinCommand}, which rejects every
     * join type other than {@code LOOKUP}.
     */
    public void testSubqueryJoinTargets() {
        assumeTrue("requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
        for (String target : List.of(
            "(FROM right | KEEP key)",
            "(ROW key = 1 | KEEP key)",
            "(TS metrics | KEEP key)",
            "(FROM (FROM nested) | KEEP key)",
            "(FROM right /* nested comment */ | WHERE key > 1 | KEEP key)",
            "(FROM right | LOOKUP JOIN nested ON key)",
            "right",
            "right*",
            "\"right\"",
            "remote:right"
        )) {
            expectError("FROM left | LEFT OUTER JOIN " + target + " ON key", "only LOOKUP JOIN available, LEFT JOIN unsupported");
            expectError("FROM left | INNER JOIN " + target + " ON key", "only LOOKUP JOIN available, INNER JOIN unsupported");
        }
    }

    /**
     * {@code ON} accepts any boolean expression, so these reach the same rejection as {@link #testSubqueryJoinTargets}.
     */
    public void testExpressionJoinConditions() {
        assumeTrue("requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
        for (String condition : List.of("a1 == a2", "a1 == a2 AND b1 == b2 AND c1 > c2", "a1 == a2 OR b1 == b2")) {
            expectError("FROM left | LEFT OUTER JOIN (FROM right) ON " + condition, "only LOOKUP JOIN available, LEFT JOIN unsupported");
            expectError("FROM left | INNER JOIN (FROM right) ON " + condition, "only LOOKUP JOIN available, INNER JOIN unsupported");
        }
    }

    public void testOuterKeyword() {
        assumeTrue("requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
        expectError("FROM left | LEFT fake JOIN (FROM right) ON key", "no viable alternative at input 'LEFT fake'");
        expectError("FROM left | INNER OUTER JOIN (FROM right) ON key", "extraneous input 'OUTER' expecting 'join'");
    }

    public void testLookupJoinRejectsSubqueryTarget() {
        expectError("FROM main | LOOKUP JOIN (FROM sub) ON key", "mismatched input '('");
    }
}
