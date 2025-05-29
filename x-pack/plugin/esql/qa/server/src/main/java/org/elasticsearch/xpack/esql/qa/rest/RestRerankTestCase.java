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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.createRerankInferenceEndpoint;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.deleteRerankInferenceEndpoint;
import static org.hamcrest.core.StringContains.containsString;

public class RestRerankTestCase extends ESRestTestCase {

    @Before
    public void skipWhenRerankDisabled() throws IOException {
        assumeTrue(
            "Requires RERANK capability",
            EsqlSpecTestCase.hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.RERANK.capabilityName()))
        );
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    @Before
    public void setUpInferenceEndpoint() throws IOException {
        createRerankInferenceEndpoint(adminClient());
    }

    @Before
    public void setUpTestIndex() throws IOException {
        Request request = new Request("PUT", "/rerank-test-index");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "title": { "type": "text" },
                  "author": { "type": "text" }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/rerank-test-index/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            { "index": {"_id": 1} }
            { "title": "The Future of Exploration", "author": "John Doe" }
            { "index": {"_id": 2} }
            { "title": "Deep Sea Exploration", "author": "Jane Smith" }
            { "index": {"_id": 3} }
            { "title": "History of Space Exploration", "author": "Alice Johnson" }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    @After
    public void wipeData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/rerank-test-index"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        deleteRerankInferenceEndpoint(adminClient());
    }

    public void testRerankWithSingleField() throws IOException {
        String query = """
            FROM rerank-test-index
            | WHERE match(title, "exploration")
            | RERANK "exploration" ON title WITH test_reranker
            | EVAL _score = ROUND(_score, 5)
            """;

        Map<String, Object> result = runEsqlQuery(query);

        var expectedValues = List.of(
            List.of("Jane Smith", "Deep Sea Exploration", 0.02941d),
            List.of("John Doe", "The Future of Exploration", 0.02632d),
            List.of("Alice Johnson", "History of Space Exploration", 0.02381d)
        );

        assertResultMap(result, defaultOutputColumns(), expectedValues);
    }

    public void testRerankWithMultipleFields() throws IOException {
        String query = """
            FROM rerank-test-index
            | WHERE match(title, "exploration")
            | RERANK "exploration" ON title, author WITH test_reranker
            | EVAL _score = ROUND(_score, 5)
            """;

        Map<String, Object> result = runEsqlQuery(query);
        ;
        var expectedValues = List.of(
            List.of("Jane Smith", "Deep Sea Exploration", 0.01818d),
            List.of("John Doe", "The Future of Exploration", 0.01754d),
            List.of("Alice Johnson", "History of Space Exploration", 0.01515d)
        );

        assertResultMap(result, defaultOutputColumns(), expectedValues);
    }

    public void testRerankWithPositionalParams() throws IOException {
        String query = """
            FROM rerank-test-index
            | WHERE match(title, "exploration")
            | RERANK ? ON title WITH ?
            | EVAL _score = ROUND(_score, 5)
            """;

        Map<String, Object> result = runEsqlQuery(query, "[\"exploration\", \"test_reranker\"]");

        var expectedValues = List.of(
            List.of("Jane Smith", "Deep Sea Exploration", 0.02941d),
            List.of("John Doe", "The Future of Exploration", 0.02632d),
            List.of("Alice Johnson", "History of Space Exploration", 0.02381d)
        );

        assertResultMap(result, defaultOutputColumns(), expectedValues);
    }

    public void testRerankWithNamedParams() throws IOException {
        String query = """
            FROM rerank-test-index
            | WHERE match(title, ?queryText)
            | RERANK ?queryText ON title WITH ?inferenceId
            | EVAL _score = ROUND(_score, 5)
            """;

        Map<String, Object> result = runEsqlQuery(query, "[{\"queryText\": \"exploration\"}, {\"inferenceId\": \"test_reranker\"}]");

        var expectedValues = List.of(
            List.of("Jane Smith", "Deep Sea Exploration", 0.02941d),
            List.of("John Doe", "The Future of Exploration", 0.02632d),
            List.of("Alice Johnson", "History of Space Exploration", 0.02381d)
        );

        assertResultMap(result, defaultOutputColumns(), expectedValues);
    }

    public void testRerankWithMissingInferenceId() {
        String query = """
            FROM rerank-test-index
            | WHERE match(title, "exploration")
            | RERANK "exploration" ON title WITH test_missing
            | EVAL _score = ROUND(_score, 5)
            """;

        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlQuery(query));
        assertThat(re.getMessage(), containsString("Inference endpoint not found"));
    }

    private static List<Map<String, String>> defaultOutputColumns() {
        return List.of(
            Map.of("name", "author", "type", "text"),
            Map.of("name", "title", "type", "text"),
            Map.of("name", "_score", "type", "double")
        );
    }

    private Map<String, Object> runEsqlQuery(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder().query(query);
        return RestEsqlTestCase.runEsqlSync(builder);
    }

    private Map<String, Object> runEsqlQuery(String query, String params) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder().query(query).params(params);
        return RestEsqlTestCase.runEsqlSync(builder);
    }
}
