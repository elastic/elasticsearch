/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.testfunction;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * REST-based integration tests for the abs3() function.
 * <p>
 * These tests verify that the abs3 function works correctly via the REST API,
 * which is the primary way external plugins will be used in production.
 * </p>
 */
public class Abs3RestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .plugin("esql-test-function")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Helper method to execute an ES|QL query via REST API.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());

        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /**
     * Test that abs3() function works with a simple literal value.
     * Query: ROW x = -5 | EVAL y = abs3(x)
     * Expected: y = 5
     */
    @SuppressWarnings("unchecked")
    public void testAbs3WithLiteral() throws IOException {
        Map<String, Object> result = runQuery("ROW x = -5 | EVAL y = abs3(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        assertThat(row.get(0), equalTo(-5)); // x
        assertThat(row.get(1), equalTo(5));  // y = abs3(x)
    }

    /**
     * Test that abs3() produces the same results as abs() for various inputs.
     */
    @SuppressWarnings("unchecked")
    public void testAbs3MatchesAbs() throws IOException {
        Map<String, Object> result = runQuery("ROW x = -42.5 | EVAL abs_result = abs(x), abs3_result = abs3(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        double absResult = ((Number) row.get(1)).doubleValue();
        double abs3Result = ((Number) row.get(2)).doubleValue();

        assertThat(absResult, closeTo(42.5, 0.0001));
        assertThat(abs3Result, closeTo(42.5, 0.0001));
        assertThat(abs3Result, closeTo(absResult, 0.0001));
    }

    /**
     * Test abs3() with different data types.
     */
    @SuppressWarnings("unchecked")
    public void testAbs3WithDifferentTypes() throws IOException {
        // Test with integer
        Map<String, Object> intResult = runQuery("ROW x = -10 | EVAL y = abs3(x)");
        List<List<Object>> intValues = (List<List<Object>>) intResult.get("values");
        assertThat(intValues.get(0).get(1), equalTo(10));

        // Test with long
        Map<String, Object> longResult = runQuery("ROW x = -9223372036854775807 | EVAL y = abs3(x)");
        List<List<Object>> longValues = (List<List<Object>>) longResult.get("values");
        assertThat(longValues.get(0).get(1), equalTo(9223372036854775807L));

        // Test with double
        Map<String, Object> doubleResult = runQuery("ROW x = -3.14159 | EVAL y = abs3(x)");
        List<List<Object>> doubleValues = (List<List<Object>>) doubleResult.get("values");
        double result = ((Number) doubleValues.get(0).get(1)).doubleValue();
        assertThat(result, closeTo(3.14159, 0.00001));
    }

    /**
     * Test abs3() with positive values (should return unchanged).
     */
    @SuppressWarnings("unchecked")
    public void testAbs3WithPositiveValues() throws IOException {
        Map<String, Object> result = runQuery("ROW x = 42 | EVAL y = abs3(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values.get(0).get(1), equalTo(42));
    }

    /**
     * Test abs3() with zero.
     */
    @SuppressWarnings("unchecked")
    public void testAbs3WithZero() throws IOException {
        Map<String, Object> result = runQuery("ROW x = 0 | EVAL y = abs3(x)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values.get(0).get(1), equalTo(0));
    }
}
