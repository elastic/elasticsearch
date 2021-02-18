/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;


public class InferenceProcessorIT extends ESRestTestCase {
    private static final Set<String> NOT_DELETED_TRAINED_MODELS = Collections.singleton("lang_ident_model_1");

    private static final String MODEL_ID = "a-perfect-regression-model";
    private final Set<String> createdPipelines = new HashSet<>();

    @Before
    public void enableLogging() throws IOException {
        Request setTrace = new Request("PUT", "_cluster/settings");
        setTrace.setJsonEntity(
            "{\"transient\": {\"logger.org.elasticsearch.xpack.ml.inference\": \"TRACE\"}}"
        );
        assertThat(client().performRequest(setTrace).getStatusLine().getStatusCode(), equalTo(200));
    }

    @SuppressWarnings("unchecked")
    @After
    public void cleanup() throws Exception {
        for (String createdPipeline : createdPipelines) {
            deletePipeline(createdPipeline);
        }
        createdPipelines.clear();
        waitForStats();
        final Request getTrainedModels = new Request("GET", "/_ml/trained_models");
        getTrainedModels.addParameter("size", "10000");
        final Response trainedModelsResponse = adminClient().performRequest(getTrainedModels);
        final List<Map<String, Object>> models = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "trained_model_configs",
            ESRestTestCase.entityAsMap(trainedModelsResponse)
        );
        if (models == null || models.isEmpty()) {
            return;
        }
        for (Map<String, Object> model : models) {
            String modelId = (String) model.get("model_id");
            if (NOT_DELETED_TRAINED_MODELS.contains(modelId)) {
                continue;
            }
            adminClient().performRequest(new Request("DELETE", "/_ml/trained_models/" + modelId));
        }
    }

    private void putModelAlias(String modelAlias, String newModel) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + newModel + "/model_aliases/" + modelAlias + "?reassign=true");
        client().performRequest(request);
    }

    private void putRegressionModel() throws IOException {
        Request model = new Request("PUT", "_ml/trained_models/" + MODEL_ID);
        model.setJsonEntity(
            "  {\n" +
                "    \"description\": \"empty model for tests\",\n" +
                "    \"tags\": [\"regression\", \"tag1\"],\n" +
                "    \"input\": {\"field_names\": [\"field1\", \"field2\"]},\n" +
                "    \"inference_config\": { \"regression\": {\"results_field\": \"my_regression\"}},\n" +
                "    \"definition\": {\n" +
                "       \"preprocessors\": [],\n" +
                "       \"trained_model\": {\n" +
                "          \"tree\": {\n" +
                "             \"feature_names\": [\"field1\", \"field2\"],\n" +
                "             \"tree_structure\": [\n" +
                "                {\"node_index\": 0, \"leaf_value\": 42}\n" +
                "             ],\n" +
                "             \"target_type\": \"regression\"\n" +
                "          }\n" +
                "       }\n" +
                "    }\n" +
                "  }"
        );

        assertThat(client().performRequest(model).getStatusLine().getStatusCode(), equalTo(200));
    }

    @SuppressWarnings("unchecked")
    public void testCreateAndDeletePipelineWithInferenceProcessor() throws Exception {
        putRegressionModel();
        String pipelineId = "regression-model-pipeline";
        createdPipelines.add(pipelineId);
        putPipeline(MODEL_ID, pipelineId);

        Map<String, Object> statsAsMap = getStats();
        List<Integer> pipelineCount =
            (List<Integer>)XContentMapValues.extractValue("trained_model_stats.pipeline_count", statsAsMap);
        assertThat(pipelineCount.get(0), equalTo(1));

        List<Map<String, Object>> counts =
            (List<Map<String, Object>>)XContentMapValues.extractValue("trained_model_stats.ingest.total", statsAsMap);
        assertThat(counts.get(0).get("count"), equalTo(0));
        assertThat(counts.get(0).get("time_in_millis"), equalTo(0));
        assertThat(counts.get(0).get("current"), equalTo(0));
        assertThat(counts.get(0).get("failed"), equalTo(0));

        // using the model will ensure it is loaded and stats will be written before it is deleted
        infer("regression-model-pipeline");

        deletePipeline(pipelineId);
        createdPipelines.remove(pipelineId);

        // check stats are updated
        assertBusy(() -> {
            Map<String, Object> updatedStatsMap = null;
            try {
                updatedStatsMap = getStats();
            } catch (ResponseException e) {
                // the search may fail because the index is not ready yet in which case retry
                if (e.getMessage().contains("search_phase_execution_exception")) {
                    fail("search failed- retry");
                } else {
                    throw e;
                }
            }

            List<Integer> updatedPipelineCount =
                (List<Integer>) XContentMapValues.extractValue("trained_model_stats.pipeline_count", updatedStatsMap);
            assertThat(updatedPipelineCount.get(0), equalTo(0));

            List<Map<String, Object>> inferenceStats =
                (List<Map<String, Object>>) XContentMapValues.extractValue("trained_model_stats.inference_stats", updatedStatsMap);
            assertNotNull(inferenceStats);
            assertThat(inferenceStats, hasSize(1));
            assertThat(inferenceStats.get(0).get("inference_count"), equalTo(1));
        });
    }

    @SuppressWarnings("unchecked")
    public void testCreateAndDeletePipelineWithInferenceProcessorByName() throws Exception {
        putRegressionModel();

        putModelAlias("regression_first", MODEL_ID);
        putModelAlias("regression_second", MODEL_ID);
        createdPipelines.add("first_pipeline");
        putPipeline("regression_first", "first_pipeline");
        createdPipelines.add("second_pipeline");
        putPipeline("regression_second", "second_pipeline");

        Map<String, Object> statsAsMap = getStats();
        List<Integer> pipelineCount =
            (List<Integer>)XContentMapValues.extractValue("trained_model_stats.pipeline_count", statsAsMap);
        assertThat(pipelineCount.get(0), equalTo(2));

        List<Map<String, Object>> counts =
            (List<Map<String, Object>>)XContentMapValues.extractValue("trained_model_stats.ingest.total", statsAsMap);
        assertThat(counts.get(0).get("count"), equalTo(0));
        assertThat(counts.get(0).get("time_in_millis"), equalTo(0));
        assertThat(counts.get(0).get("current"), equalTo(0));
        assertThat(counts.get(0).get("failed"), equalTo(0));

        // using the model will ensure it is loaded and stats will be written before it is deleted
        infer("first_pipeline");
        deletePipeline("first_pipeline");
        createdPipelines.remove("first_pipeline");

        infer("second_pipeline");
        deletePipeline("second_pipeline");
        createdPipelines.remove("second_pipeline");

        // check stats are updated
        assertBusy(() -> {
            Map<String, Object> updatedStatsMap = null;
            try {
                updatedStatsMap = getStats();
            } catch (ResponseException e) {
                // the search may fail because the index is not ready yet in which case retry
                if (e.getMessage().contains("search_phase_execution_exception")) {
                    fail("search failed- retry");
                } else {
                    throw e;
                }
            }

            List<Integer> updatedPipelineCount =
                (List<Integer>) XContentMapValues.extractValue("trained_model_stats.pipeline_count", updatedStatsMap);
            assertThat(updatedPipelineCount.get(0), equalTo(0));

            List<Map<String, Object>> inferenceStats =
                (List<Map<String, Object>>) XContentMapValues.extractValue("trained_model_stats.inference_stats", updatedStatsMap);
            assertNotNull(inferenceStats);
            assertThat(inferenceStats, hasSize(1));
            assertThat(inferenceStats.toString(), inferenceStats.get(0).get("inference_count"), equalTo(2));
        });
    }

    public void testDeleteModelWhileAliasReferencedByPipeline() throws Exception {
        putRegressionModel();
        putModelAlias("regression_first", MODEL_ID);
        createdPipelines.add("first_pipeline");
        putPipeline("regression_first", "first_pipeline");
        Exception ex = expectThrows(Exception.class,
            () -> client().performRequest(new Request("DELETE", "_ml/trained_models/" + MODEL_ID)));
        assertThat(ex.getMessage(),
            containsString("Cannot delete model ["
                + MODEL_ID
                + "] as it has a model_alias [regression_first] that is still referenced by ingest processors"));
        infer("first_pipeline");
        deletePipeline("first_pipeline");
        waitForStats();
    }

    public void testDeleteModelWhileReferencedByPipeline() throws Exception {
        putRegressionModel();
        createdPipelines.add("first_pipeline");
        putPipeline(MODEL_ID, "first_pipeline");
        Exception ex = expectThrows(Exception.class,
            () -> client().performRequest(new Request("DELETE", "_ml/trained_models/" + MODEL_ID)));
        assertThat(ex.getMessage(),
            containsString("Cannot delete model ["
                + MODEL_ID
                + "] as it is still referenced by ingest processors"));
        infer("first_pipeline");
        deletePipeline("first_pipeline");
        waitForStats();
    }

    @SuppressWarnings("unchecked")
    public void testCreateProcessorWithDeprecatedFields() throws Exception {
        putRegressionModel();

        createdPipelines.add("regression-model-deprecated-pipeline");
        Request putPipeline = new Request("PUT", "_ingest/pipeline/regression-model-deprecated-pipeline");
        putPipeline.setJsonEntity(
                "{\n" +
                "  \"processors\": [\n" +
                "    {\n" +
                "      \"inference\" : {\n" +
                "        \"model_id\" : \"" + MODEL_ID + "\",\n" +
                "        \"inference_config\": {\"regression\": {}},\n" +
                "        \"field_mappings\": {}\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}"
        );

        RequestOptions ro = expectWarnings("Deprecated field [field_mappings] used, expected [field_map] instead");
        putPipeline.setOptions(ro);
        Response putResponse = client().performRequest(putPipeline);
        assertThat(putResponse.getStatusLine().getStatusCode(), equalTo(200));

        // using the model will ensure it is loaded and stats will be written before it is deleted
        infer("regression-model-deprecated-pipeline");

        deletePipeline("regression-model-deprecated-pipeline");
        createdPipelines.remove("regression-model-deprecated-pipeline");
        waitForStats();
        assertBusy(() -> {
            Map<String, Object> updatedStatsMap = null;
            try {
                updatedStatsMap = getStats();
            } catch (ResponseException e) {
                // the search may fail because the index is not ready yet in which case retry
                if (e.getMessage().contains("search_phase_execution_exception")) {
                    fail("search failed- retry");
                } else {
                    throw e;
                }
            }

            List<Integer> updatedPipelineCount =
                (List<Integer>) XContentMapValues.extractValue("trained_model_stats.pipeline_count", updatedStatsMap);
            assertThat(updatedPipelineCount.get(0), equalTo(0));

            List<Map<String, Object>> inferenceStats =
                (List<Map<String, Object>>) XContentMapValues.extractValue("trained_model_stats.inference_stats", updatedStatsMap);
            assertNotNull(inferenceStats);
            assertThat(inferenceStats, hasSize(1));
            assertThat(inferenceStats.get(0).get("inference_count"), equalTo(1));
        });
    }

    private void infer(String pipelineId) throws IOException {
        Request putDoc = new Request("POST", "any_index/_doc?pipeline=" + pipelineId);
        putDoc.setJsonEntity("{\"field1\": 1, \"field2\": 2}");

        Response response = client().performRequest(putDoc);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
    }

    private void putPipeline(String modelId, String pipelineName) throws IOException {
        Request putPipeline = new Request("PUT", "_ingest/pipeline/" + pipelineName);
        putPipeline.setJsonEntity(
            "          {\n" +
                "            \"processors\": [\n" +
                "              {\n" +
                "                \"inference\" : {\n" +
                "                  \"model_id\" : \"" + modelId + "\",\n" +
                "                  \"inference_config\": {\"regression\": {}},\n" +
                "                  \"target_field\": \"regression_field\",\n" +
                "                  \"field_map\": {}\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          }"
        );

        assertThat(client().performRequest(putPipeline).getStatusLine().getStatusCode(), equalTo(200));
    }

    private void deletePipeline(String pipelineId) throws IOException {
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
    private void waitForStats() throws Exception {
        assertBusy(() -> {
            Map<String, Object> updatedStatsMap = null;
            try {
                ensureGreen(".ml-stats-*");
                updatedStatsMap = getStats();
            } catch (ResponseException e) {
                // the search may fail because the index is not ready yet in which case retry
                if (e.getMessage().contains("search_phase_execution_exception")) {
                    fail("search failed- retry");
                } else {
                    throw e;
                }
            }

            List<Map<String, Object>> inferenceStats =
                (List<Map<String, Object>>) XContentMapValues.extractValue("trained_model_stats.inference_stats", updatedStatsMap);
            assertNotNull(inferenceStats);
        });
    }

    private Map<String, Object> getStats() throws IOException {
        Request getStats = new Request("GET", "_ml/trained_models/" + MODEL_ID + "/_stats");
        Response statsResponse = client().performRequest(getStats);
        return entityAsMap(statsResponse);
    }
}
