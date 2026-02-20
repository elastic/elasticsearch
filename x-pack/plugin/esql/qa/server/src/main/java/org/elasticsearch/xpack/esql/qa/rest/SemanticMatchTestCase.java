/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringContains.containsString;

public abstract class SemanticMatchTestCase extends ESRestTestCase {

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    public void testWithMultipleInferenceIds() throws IOException {
        assumeTrue("semantic text capability not available", EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS.isEnabled());

        var request1 = new Request("POST", "/test-semantic1/_doc/id-1");
        request1.addParameter("refresh", "true");
        request1.setJsonEntity("{\"semantic_text_field\": \"inference test 1\"}");
        assertEquals(201, adminClient().performRequest(request1).getStatusLine().getStatusCode());

        var request2 = new Request("POST", "/test-semantic2/_doc/id-2");
        request2.addParameter("refresh", "true");
        request2.setJsonEntity("{\"semantic_text_field\": \"inference test 2\"}");
        assertEquals(201, adminClient().performRequest(request2).getStatusLine().getStatusCode());

        String query = """
            from test-semantic1,test-semantic2
            | where match(semantic_text_field, "something")
            | SORT semantic_text_field ASC
            """;
        Map<String, Object> result = runEsqlQuery(query);

        assertResultMap(
            result,
            matchesList().item(matchesMap().entry("name", "semantic_text_field").entry("type", "text")),
            matchesList(List.of(List.of("inference test 1"), List.of("inference test 2")))
        );
    }

    public void testWithInferenceNotConfigured() {
        assumeTrue("semantic text capability not available", EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS.isEnabled());

        String query = """
            from test-semantic3
            | where match(semantic_text_field, "something")
            """;
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlQuery(query));

        assertThat(re.getMessage(), containsString("Inference endpoint not found"));
        assertEquals(404, re.getResponse().getStatusLine().getStatusCode());
    }

    public void testCopyToWithSemanticText() throws IOException {
        assumeTrue("semantic text capability not available", EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS.isEnabled());

        var request = new Request("POST", "/test-semantic4/_doc/id-1");
        request.addParameter("refresh", "true");
        request.setJsonEntity("{\"text_field\": \"inference test\"}");
        assertEquals(201, adminClient().performRequest(request).getStatusLine().getStatusCode());

        String query = """
            from test-semantic4
            """;
        Map<String, Object> result = runEsqlQuery(query);

        assertResultMap(
            result,
            matchesList().item(matchesMap().entry("name", "semantic_text_field").entry("type", "text"))
                .item(matchesMap().entry("name", "text_field").entry("type", "text")),
            List.of(List.of("inference test", "inference test"))
        );
    }

    public void testDenseSemanticTextPerformsAutoPrefiltering() throws IOException {
        assumeTrue("semantic text capability not available", EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS.isEnabled());

        var request = new Request("POST", "/test-semantic2/_mapping");
        request.setJsonEntity("""
            {
                "properties": {
                    "keyword_field": { "type": "keyword" }
                }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_bulk?refresh=true");
        request.setJsonEntity("""
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_1"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "violins", "keyword_field": "label_2"}
            {"index": { "_index": "test-semantic2" } }
            {"semantic_text_field": "pianos", "keyword_field": "label_1"}
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        String query = """
            FROM test-semantic2 METADATA _score
            | WHERE keyword_field == "label_1"
            | WHERE MATCH(semantic_text_field, "violins")
            | SORT _score DESC
            | KEEP keyword_field, semantic_text_field
            | LIMIT 10
            """;

        Map<String, Object> result = runEsqlQuery(query);
        assertResultMap(
            result,
            matchesList().item(matchesMap().entry("name", "keyword_field").entry("type", "keyword"))
                .item(matchesMap().entry("name", "semantic_text_field").entry("type", "text")),
            List.of(List.of("label_1", "violins"), List.of("label_1", "pianos"))
        );
    }

    @Before
    public void setUpIndices() throws IOException {
        var settings = Settings.builder().build();

        String mapping1 = """
                "properties": {
                  "semantic_text_field": {
                   "type": "semantic_text",
                   "inference_id": "test_sparse_inference"
                  }
                }
            """;
        createIndex(adminClient(), "test-semantic1", settings, mapping1);

        String mapping2 = """
                 "properties": {
                   "semantic_text_field": {
                    "type": "semantic_text",
                    "inference_id": "test_dense_inference"
                   }
                 }
            """;
        createIndex(adminClient(), "test-semantic2", settings, mapping2);

        String mapping3 = """
                 "properties": {
                   "semantic_text_field": {
                    "type": "semantic_text",
                    "inference_id": "inexistent"
                   }
                 }
            """;
        createIndex(adminClient(), "test-semantic3", settings, mapping3);

        String mapping4 = """
                "properties": {
                  "semantic_text_field": {
                   "type": "semantic_text",
                   "inference_id": "test_dense_inference"
                  },
                  "text_field": {
                   "type": "text",
                   "copy_to": "semantic_text_field"
                  }
                }
            """;
        createIndex(adminClient(), "test-semantic4", settings, mapping4);
    }

    @Before
    public void setUpSparseEmbeddingInferenceEndpoint() throws IOException {
        Request request = new Request("PUT", "/_inference/sparse_embedding/test_sparse_inference");
        request.setJsonEntity("""
                  {
                   "service": "test_service",
                   "service_settings": {
                     "model": "my_model",
                     "api_key": "abc64"
                   },
                   "task_settings": {
                   }
                 }
            """);
        try {
            adminClient().performRequest(request);
        } catch (ResponseException exc) {
            // in case the removal failed
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    @Before
    public void setUpTextEmbeddingInferenceEndpoint() throws IOException {
        Request request = new Request("PUT", "/_inference/text_embedding/test_dense_inference");
        request.setJsonEntity("""
                  {
                   "service": "text_embedding_test_service",
                   "service_settings": {
                     "model": "my_model",
                     "api_key": "abc64",
                     "dimensions": 128
                   },
                   "task_settings": {
                   }
                 }
            """);
        try {
            adminClient().performRequest(request);
        } catch (ResponseException exc) {
            // in case the removal failed
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    @After
    public void wipeData() throws IOException {
        adminClient().performRequest(new Request("DELETE", "*"));

        try {
            adminClient().performRequest(new Request("DELETE", "/_inference/test_sparse_inference"));
        } catch (ResponseException e) {
            // 404 here means the endpoint was not created
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        try {
            adminClient().performRequest(new Request("DELETE", "/_inference/test_dense_inference"));
        } catch (ResponseException e) {
            // 404 here means the endpoint was not created
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    private Map<String, Object> runEsqlQuery(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder().query(query);
        return RestEsqlTestCase.runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);
    }
}
