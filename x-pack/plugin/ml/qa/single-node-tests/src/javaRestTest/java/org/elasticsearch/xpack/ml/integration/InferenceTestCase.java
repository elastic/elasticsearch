/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class InferenceTestCase extends ESRestTestCase {

    protected final Set<String> createdPipelines = new HashSet<>();

    @After
    public void cleanup() throws Exception {
        for (String createdPipeline : createdPipelines) {
            deletePipeline(createdPipeline);
        }
        createdPipelines.clear();
        waitForStats();
        adminClient().performRequest(new Request("POST", "/_features/_reset"));
    }

    void deletePipeline(String pipelineId) throws IOException {
        try {
            Request deletePipeline = new Request("DELETE", "_ingest/pipeline/" + pipelineId);
            assertThat(client().performRequest(deletePipeline).getStatusLine().getStatusCode(), equalTo(200));
        } catch (ResponseException ex) {
            if (ex.getResponse().getStatusLine().getStatusCode() != 404) {
                throw ex;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void waitForStats() throws Exception {
        assertBusy(() -> {
            Map<String, Object> updatedStatsMap = null;
            try {
                ensureGreen(".ml-stats-*");
                updatedStatsMap = getStats("_all");
            } catch (ResponseException e) {
                // the search may fail because the index is not ready yet in which case retry
                if (e.getMessage().contains("search_phase_execution_exception")) {
                    fail("search failed- retry");
                } else {
                    throw e;
                }
            }

            List<Map<String, Object>> inferenceStats = (List<Map<String, Object>>) XContentMapValues.extractValue(
                "trained_model_stats.inference_stats",
                updatedStatsMap
            );
            assertNotNull(inferenceStats);
        });
    }

    Map<String, Object> getStats(String modelId) throws IOException {
        Request getStats = new Request("GET", "_ml/trained_models/" + modelId + "/_stats");
        Response statsResponse = client().performRequest(getStats);
        return entityAsMap(statsResponse);
    }

    void putRegressionModel(String modelId) throws IOException {
        putRegressionModel(modelId, """
            {
              "description": "empty model for tests",
              "tags": ["regression", "tag1"],
              "input": {"field_names": ["field1", "field2"]},
              "inference_config": { "regression": {"results_field": "my_regression"}},
              "definition": {
                 "preprocessors": [],
                 "trained_model": {
                    "tree": {
                       "feature_names": ["field1", "field2"],
                       "tree_structure": [
                          {"node_index": 0, "leaf_value": 42}
                       ],
                       "target_type": "regression"
                    }
                 }
              }
            }
            """);
    }

    void putRegressionModel(String modelId, String body) throws IOException {
        Request model = new Request("PUT", "_ml/trained_models/" + modelId);
        model.setJsonEntity(body);
        assertThat(client().performRequest(model).getStatusLine().getStatusCode(), equalTo(200));
    }

}
