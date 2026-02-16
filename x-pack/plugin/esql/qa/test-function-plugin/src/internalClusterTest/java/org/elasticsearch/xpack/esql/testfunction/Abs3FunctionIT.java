/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.testfunction;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test for the Abs3 runtime-generated function.
 * <p>
 * This test verifies that:
 * <ul>
 *   <li>The TestFunctionPlugin is loaded correctly</li>
 *   <li>The Abs3 function is discovered via EsqlFunctionProvider</li>
 *   <li>ES|QL queries using abs3() execute correctly</li>
 *   <li>Runtime-generated evaluators produce correct results</li>
 * </ul>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class Abs3FunctionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlPlugin.class);
        plugins.add(TestFunctionPlugin.class);
        return plugins;
    }

    /**
     * Helper method to run an ES|QL query and return the response.
     */
    private EsqlQueryResponse runQuery(String query) throws Exception {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);

        PlainActionFuture<EsqlQueryResponse> future = new PlainActionFuture<>();
        client().execute(EsqlQueryAction.INSTANCE, request, future);
        return future.get(30, TimeUnit.SECONDS);
    }

    /**
     * Test that abs3() function works with a simple literal value.
     * Query: ROW x = -5 | EVAL y = abs3(x)
     * Expected: y = 5
     */
    public void testAbs3WithLiteral() throws Exception {
        try (EsqlQueryResponse resp = runQuery("ROW x = -5 | EVAL y = abs3(x)")) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> rowList = new ArrayList<>();
                row.forEachRemaining(rowList::add);
                values.add(rowList);
            });

            assertThat(values.size(), equalTo(1));
            List<Object> row = values.get(0);
            assertThat(row.get(0), equalTo(-5)); // x
            assertThat(row.get(1), equalTo(5));  // y = abs3(x)
        }
    }

    /**
     * Test that abs3() produces the same results as abs() for various inputs.
     */
    public void testAbs3MatchesAbs() throws Exception {
        try (EsqlQueryResponse resp = runQuery("ROW x = -42.5 | EVAL abs_result = abs(x), abs3_result = abs3(x)")) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> rowList = new ArrayList<>();
                row.forEachRemaining(rowList::add);
                values.add(rowList);
            });

            assertThat(values.size(), equalTo(1));
            List<Object> row = values.get(0);
            assertThat((Double) row.get(1), closeTo(42.5, 0.0001));  // abs(x)
            assertThat((Double) row.get(2), closeTo(42.5, 0.0001));  // abs3(x)
        }
    }

    /**
     * Test abs3() with different data types.
     */
    public void testAbs3WithDifferentTypes() throws Exception {
        // Test with integer
        try (EsqlQueryResponse resp = runQuery("ROW x = -10 | EVAL y = abs3(x)")) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> rowList = new ArrayList<>();
                row.forEachRemaining(rowList::add);
                values.add(rowList);
            });
            assertThat(values.get(0).get(1), equalTo(10));
        }

        // Test with long
        try (EsqlQueryResponse resp = runQuery("ROW x = -9223372036854775807 | EVAL y = abs3(x)")) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> rowList = new ArrayList<>();
                row.forEachRemaining(rowList::add);
                values.add(rowList);
            });
            assertThat(values.get(0).get(1), equalTo(9223372036854775807L));
        }

        // Test with double
        try (EsqlQueryResponse resp = runQuery("ROW x = -3.14159 | EVAL y = abs3(x)")) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> rowList = new ArrayList<>();
                row.forEachRemaining(rowList::add);
                values.add(rowList);
            });
            assertThat((Double) values.get(0).get(1), closeTo(3.14159, 0.00001));
        }
    }

    /**
     * Test abs3() with positive values (should return unchanged).
     */
    public void testAbs3WithPositiveValues() throws Exception {
        try (EsqlQueryResponse resp = runQuery("ROW x = 42 | EVAL y = abs3(x)")) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> rowList = new ArrayList<>();
                row.forEachRemaining(rowList::add);
                values.add(rowList);
            });
            assertThat(values.get(0).get(1), equalTo(42));
        }
    }

    /**
     * Test abs3() with zero.
     */
    public void testAbs3WithZero() throws Exception {
        try (EsqlQueryResponse resp = runQuery("ROW x = 0 | EVAL y = abs3(x)")) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> rowList = new ArrayList<>();
                row.forEachRemaining(rowList::add);
                values.add(rowList);
            });
            assertThat(values.get(0).get(1), equalTo(0));
        }
    }
}
