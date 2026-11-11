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

    public void testLookupJoinRejectsSubqueryTarget() {
        expectError("FROM main | LOOKUP JOIN (FROM sub) ON key", "mismatched input '('");
    }
}
