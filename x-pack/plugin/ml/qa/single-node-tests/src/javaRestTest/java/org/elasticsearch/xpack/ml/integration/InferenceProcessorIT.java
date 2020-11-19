/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;


public class InferenceProcessorIT extends ESRestTestCase {

    private static final String MODEL_ID = "a-perfect-regression-model";

    @Before
    public void enableLogging() throws IOException {
        Request setTrace = new Request("PUT", "_cluster/settings");
        setTrace.setJsonEntity(
            "{\"transient\": {\"logger.org.elasticsearch.xpack.ml.inference\": \"TRACE\"}}"
        );
        assertThat(client().performRequest(setTrace).getStatusLine().getStatusCode(), equalTo(200));
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

    public void testCreateAndDeletePipelineWithInferenceProcessor() throws IOException {
        putRegressionModel();

        Request putPipeline = new Request("PUT", "_ingest/pipeline/regression-model-pipeline");
        putPipeline.setJsonEntity(
                "          {\n" +
                "            \"processors\": [\n" +
                "              {\n" +
                "                \"inference\" : {\n" +
                "                  \"model_id\" : \"a-perfect-regression-model\",\n" +
                "                  \"inference_config\": {\"regression\": {}},\n" +
                "                  \"target_field\": \"regression_field\",\n" +
                "                  \"field_map\": {}\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          }"
        );

        assertThat(client().performRequest(putPipeline).getStatusLine().getStatusCode(), equalTo(200));

        // using the model will ensure it is loaded and stats will be written before it is deleted
        infer("regression-model-pipeline");

        Request deletePipeline = new Request("DELETE", "_ingest/pipeline/regression-model-pipeline");
        assertThat(client().performRequest(deletePipeline).getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testCreateProcessorWithDeprecatedFields() throws IOException {
        putRegressionModel();

        Request putPipeline = new Request("PUT", "_ingest/pipeline/regression-model-deprecated-pipeline");
        putPipeline.setJsonEntity(
                "{\n" +
                "  \"processors\": [\n" +
                "    {\n" +
                "      \"inference\" : {\n" +
                "        \"model_id\" : \"a-perfect-regression-model\",\n" +
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

        Request deletePipeline = new Request("DELETE", "_ingest/pipeline/regression-model-deprecated-pipeline");
        Response deleteResponse = client().performRequest(deletePipeline);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), equalTo(200));
    }

    public void infer(String pipelineId) throws IOException {
        Request putDoc = new Request("POST", "any_index/_doc?pipeline=" + pipelineId);
        putDoc.setJsonEntity("{\"field1\": 1, \"field2\": 2}");

        Response response = client().performRequest(putDoc);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
    }

    @After
    @SuppressWarnings("unchecked")
    public void waitForStatsDoc() throws Exception {
        assertBusy( () -> {
            Request searchForStats = new Request("GET", ".ml-stats-*/_search?rest_total_hits_as_int");
            searchForStats.setJsonEntity(
                    "{\n" +
                    "  \"query\": {\n" +
                    "    \"bool\": {\n" +
                    "      \"filter\": [\n" +
                    "        {\n" +
                    "          \"term\": {\n" +
                    "            \"type\": \"inference_stats\"\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"term\": {\n" +
                    "            \"model_id\": \"" + MODEL_ID + "\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}"
            );

            try {
                Response searchResponse = client().performRequest(searchForStats);

                Map<String, Object> responseAsMap = entityAsMap(searchResponse);
                Map<String, Object> hits = (Map<String, Object>)responseAsMap.get("hits");
                assertThat(responseAsMap.toString(), hits.get("total"), equalTo(1));
            } catch (ResponseException e) {
                // the search may fail because the index is not ready yet in which case retry
                if (e.getMessage().contains("search_phase_execution_exception") == false) {
                    throw e;
                }
            }
        });
    }
}
