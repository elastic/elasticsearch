/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.client.ResponseException;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that run ESQL queries with a single command that holds a huge number of expression nodes. We want to make sure they don't
 * consume the entire heap and crash Elasticsearch.
 * <p>
 * The nesting of expressions is limited, but their size is not: only the length of the query bounds it. A function with many arguments
 * has hundreds of thousands of nodes in a query that is under the length limit. The limit on the nodes of the plan counts the nodes of
 * expressions too, so such a query is rejected by that limit specifically, not merely by some client error — asserting on the exact
 * message is what would catch a regression where the real protection stopped firing and the query was instead rejected for an
 * unrelated reason, or completed and used unbounded memory.
 */
@TimeoutSuite(millis = 10 * TimeUnits.MINUTE)
public class HeapAttackExpressionIT extends HeapAttackTestCase {

    /**
     * The longest query the parser accepts is a million characters, and the query below needs three for each argument.
     */
    private static final int ARGUMENTS = 250_000;

    @Before
    public void checkCapability() {
        assumeTrue("Run these tests in snapshot build", Build.current().isSnapshot());
    }

    /**
     * A function with a quarter of a million arguments.
     */
    public void testFunctionWithManyArguments() throws IOException {
        initSingleDocIndex();
        String arguments = String.join(", ", Collections.nCopies(ARGUMENTS, "a"));
        assertRejectedWithMaxPlanSizeError("COALESCE(" + arguments + ")");
    }

    /**
     * The query must be rejected specifically by the limit on the nodes of the plan ({@code ViewResolver#MAX_PLAN_NODES}), not merely
     * by any client error or circuit breaker: either of those could also mean the real protection against this heap-attack shape has
     * stopped firing, and the query is instead failing for an unrelated reason — or, if nothing failed it, completed and used
     * unbounded memory.
     */
    private void assertRejectedWithMaxPlanSizeError(String expression) throws IOException {
        StringBuilder query = startQuery();
        query.append("FROM single | EVAL y = ").append(expression).append(" | KEEP y \"}");
        ResponseException e = expectThrows(ResponseException.class, () -> query(query.toString(), "columns,values"));
        assertMap(
            responseAsMap(e.getResponse()),
            matchesMap().extraOk()
                .entry("status", 400)
                .entry(
                    "error",
                    matchesMap().extraOk()
                        .entry("type", "verification_exception")
                        .entry("reason", containsString("ES|QL query exceeded the maximum plan size allowed (10000 nodes)"))
                )
        );
    }
}
