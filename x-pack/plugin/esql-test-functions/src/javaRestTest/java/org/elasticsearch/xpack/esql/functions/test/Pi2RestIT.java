/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

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
import static org.hamcrest.Matchers.hasSize;

/**
 * REST-based integration tests for the pi2() function.
 * <p>
 * These tests verify that the pi2 function (a zero-parameter function) works correctly
 * via the REST API. This tests the runtime evaluator generator's support for functions
 * with no input parameters.
 * </p>
 */
public class Pi2RestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
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
        // Allow warnings (e.g., "No limit defined, adding default limit of [1000]")
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());

        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /**
     * Test that pi2() returns the correct value of π.
     * Query: ROW x = pi2()
     * Expected: x ≈ 3.141592653589793
     */
    @SuppressWarnings("unchecked")
    public void testPi2ReturnsCorrectValue() throws IOException {
        Map<String, Object> result = runQuery("ROW x = pi2()");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        double piValue = ((Number) row.get(0)).doubleValue();
        assertThat(piValue, closeTo(Math.PI, 0.0000001));
    }

    /**
     * Test that pi2() produces the same result as the built-in pi() function.
     */
    @SuppressWarnings("unchecked")
    public void testPi2MatchesPi() throws IOException {
        Map<String, Object> result = runQuery("ROW pi_result = pi(), pi2_result = pi2()");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        double piResult = ((Number) row.get(0)).doubleValue();
        double pi2Result = ((Number) row.get(1)).doubleValue();

        assertThat(piResult, closeTo(Math.PI, 0.0000001));
        assertThat(pi2Result, closeTo(Math.PI, 0.0000001));
        assertThat(pi2Result, closeTo(piResult, 0.0000001));
    }

    /**
     * Test pi2() with multiple rows - each row should have the same pi value.
     */
    @SuppressWarnings("unchecked")
    public void testPi2WithMultipleRows() throws IOException {
        // Create test index
        Request createIndex = new Request("PUT", "/test-pi2");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value": { "type": "integer" }
                }
              }
            }
            """);
        client().performRequest(createIndex);

        // Index test documents
        for (int i = 1; i <= 3; i++) {
            Request indexDoc = new Request("POST", "/test-pi2/_doc");
            indexDoc.setJsonEntity("{\"value\": " + i + "}");
            client().performRequest(indexDoc);
        }

        // Refresh index
        Request refresh = new Request("POST", "/test-pi2/_refresh");
        client().performRequest(refresh);

        // Query with pi2 - all rows should have the same pi value
        Map<String, Object> result = runQuery("FROM test-pi2 | EVAL pi_val = pi2() | SORT value");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // Each row should have pi as the second column
        for (List<Object> row : values) {
            double piValue = ((Number) row.get(1)).doubleValue();
            assertThat(piValue, closeTo(Math.PI, 0.0000001));
        }
    }

    /**
     * Test pi2() used in a calculation.
     */
    @SuppressWarnings("unchecked")
    public void testPi2InCalculation() throws IOException {
        // Calculate circumference of a circle with radius 2: 2 * pi * r = 2 * pi * 2 = 4 * pi
        Map<String, Object> result = runQuery("ROW radius = 2 | EVAL circumference = 2 * pi2() * radius");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        double circumference = ((Number) row.get(1)).doubleValue();
        double expectedCircumference = 2 * Math.PI * 2;
        assertThat(circumference, closeTo(expectedCircumference, 0.0000001));
    }

    /**
     * Test pi2() combined with other functions.
     */
    @SuppressWarnings("unchecked")
    public void testPi2CombinedWithOtherFunctions() throws IOException {
        // Test sin(pi2()) which should be approximately 0
        Map<String, Object> result = runQuery("ROW x = sin(pi2())");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));

        List<Object> row = values.get(0);
        double sinPi = ((Number) row.get(0)).doubleValue();
        // sin(π) should be very close to 0
        assertThat(sinPi, closeTo(0.0, 0.0000001));
    }
}
