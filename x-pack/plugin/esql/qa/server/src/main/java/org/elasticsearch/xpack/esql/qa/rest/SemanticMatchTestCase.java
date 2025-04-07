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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringContains.containsString;

public abstract class SemanticMatchTestCase extends ESRestTestCase {
    public void testWithMultipleInferenceIds() throws IOException {
        assumeTrue("semantic text capability not available", EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS.isEnabled());

        String query = """
            from test-semantic1,test-semantic2
            | where match(semantic_text_field, "something")
            """;
        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlQuery(query));

        assertThat(re.getMessage(), containsString("Field [semantic_text_field] has multiple inference IDs associated with it"));

        assertEquals(400, re.getResponse().getStatusLine().getStatusCode());
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
    }

    @Before
    public void setUpTextEmbeddingInferenceEndpoint() throws IOException {
        Request request = new Request("PUT", "_inference/text_embedding/test_dense_inference");
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
            adminClient().performRequest(new Request("DELETE", "_inference/test_dense_inference"));
        } catch (ResponseException e) {
            // 404 here means the endpoint was not created
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    private Map<String, Object> runEsqlQuery(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder().query(query);
        return RestEsqlTestCase.runEsqlSync(builder);
    }
}
