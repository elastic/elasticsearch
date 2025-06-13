/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class RestSampleTestCase extends ESRestTestCase {

    @Before
    public void skipWhenSampleDisabled() throws IOException {
        assumeTrue(
            "Requires SAMPLE capability",
            EsqlSpecTestCase.hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.SAMPLE_V2.capabilityName()))
        );
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    /**
     * Matcher for the results of sampling 50% of the elements 0,1,2,...,998,999.
     * The results should consist of unique numbers in [0,999]. Furthermore, the
     * size should on average be 500. Allowing for 10 stddev deviations, the size
     * should be in [250,750].
     */
    private static final TypeSafeMatcher<List<List<Integer>>> RESULT_MATCHER = new TypeSafeMatcher<>() {
        @Override
        public void describeTo(Description description) {
            description.appendText("a list with between 250 and 750 unique elements in [0,999]");
        }

        @Override
        protected boolean matchesSafely(List<List<Integer>> lists) {
            if (lists.size() < 250 || lists.size() > 750) {
                return false;
            }
            Set<Integer> values = new HashSet<>();
            for (List<Integer> list : lists) {
                if (list.size() != 1) {
                    return false;
                }
                Integer value = list.get(0);
                if (value == null || value < 0 || value >= 1000) {
                    return false;
                }
                values.add(value);
            }
            return values.size() == lists.size();
        }
    };

    /**
     * This tests sampling in the Lucene query.
     */
    public void testSample_withFrom() throws IOException {
        createTestIndex();
        test("FROM sample-test-index | SAMPLE 0.5 | LIMIT 1000");
        deleteTestIndex();
    }

    /**
     * This tests sampling in the ES|QL operator.
     */
    public void testSample_withRow() throws IOException {
        List<Integer> numbers = IntStream.range(0, 999).boxed().toList();
        test("ROW value = " + numbers + " | MV_EXPAND value | SAMPLE 0.5 | LIMIT 1000");
    }

    private void test(String query) throws IOException {
        int iterationCount = 1000;
        int totalResultSize = 0;
        for (int iteration = 0; iteration < iterationCount; iteration++) {
            Map<String, Object> result = runEsqlQuery(query);
            assertResultMap(result, defaultOutputColumns(), RESULT_MATCHER);
            totalResultSize += ((List<?>) result.get("values")).size();
        }
        // On average there's 500 elements in the results set.
        // Allowing for 10 stddev deviations, it should be in [490,510].
        assertThat(totalResultSize / iterationCount, both(greaterThan(490)).and(lessThan(510)));
    }

    private static List<Map<String, String>> defaultOutputColumns() {
        return List.of(Map.of("name", "value", "type", "integer"));
    }

    private Map<String, Object> runEsqlQuery(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder().query(query);
        return RestEsqlTestCase.runEsqlSync(builder);
    }

    private void createTestIndex() throws IOException {
        Request request = new Request("PUT", "/sample-test-index");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "value": { "type": "integer" }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        StringBuilder requestJsonEntity = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            requestJsonEntity.append("{ \"index\": {\"_id\": " + i + "} }\n");
            requestJsonEntity.append("{ \"value\": " + i + " }\n");
        }

        request = new Request("POST", "/sample-test-index/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(requestJsonEntity.toString());
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private void deleteTestIndex() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/sample-test-index"));
        } catch (ResponseException e) {
            throw e;
        }
    }
}
