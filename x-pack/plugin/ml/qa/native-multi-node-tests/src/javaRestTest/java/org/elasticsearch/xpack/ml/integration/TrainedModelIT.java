/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TrainedModelIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = UsernamePasswordToken.basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    public void testGetTrainedModels() throws IOException {
        String modelId = "a_test_regression_model";
        String modelId2 = "a_test_regression_model-2";
        putRegressionModel(modelId);
        putRegressionModel(modelId2);
        Response getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "trained_models/" + modelId));

        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));
        String response = EntityUtils.toString(getModel.getEntity());

        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"count\":1"));

        getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "trained_models/a_test_regression*"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model-2\""));
        assertThat(response, not(containsString("\"definition\"")));
        assertThat(response, containsString("\"count\":2"));

        getModel = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "trained_models/a_test_regression_model?human=true&include=definition")
        );
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"model_size_bytes\""));
        assertThat(response, containsString("\"model_size\""));
        assertThat(response, containsString("\"model_type\":\"tree_ensemble\""));
        assertThat(response, containsString("\"definition\""));
        assertThat(response, not(containsString("\"compressed_definition\"")));
        assertThat(response, containsString("\"count\":1"));

        getModel = client().performRequest(
            new Request(
                "GET",
                MachineLearning.BASE_PATH + "trained_models/a_test_regression_model?decompress_definition=false&include=definition"
            )
        );
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"model_size_bytes\""));
        assertThat(response, containsString("\"compressed_definition\""));
        assertThat(response, not(containsString("\"definition\"")));
        assertThat(response, containsString("\"count\":1"));

        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(
                new Request("GET", MachineLearning.BASE_PATH + "trained_models/a_test_regression*?human=true&include=definition")
            )
        );
        assertThat(
            EntityUtils.toString(responseException.getResponse().getEntity()),
            containsString(Messages.INFERENCE_TOO_MANY_DEFINITIONS_REQUESTED)
        );

        getModel = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "trained_models/a_test_regression_model,a_test_regression_model-2")
        );
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model-2\""));
        assertThat(response, containsString("\"count\":2"));

        getModel = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "trained_models/classification*?allow_no_match=true")
        );
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"count\":0"));

        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> client().performRequest(
                new Request("GET", MachineLearning.BASE_PATH + "trained_models/classification*?allow_no_match=false")
            )
        );
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "trained_models?from=0&size=1"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"count\":3"));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, not(containsString("\"model_id\":\"a_test_regression_model-2\"")));

        getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "trained_models?from=1&size=1"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"count\":3"));
        assertThat(response, not(containsString("\"model_id\":\"a_test_regression_model\"")));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model-2\""));
    }

    public void testDeleteTrainedModels() throws IOException {
        String modelId = "test_delete_regression_model";
        putRegressionModel(modelId);

        Response delModel = client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "trained_models/" + modelId));
        String response = EntityUtils.toString(delModel.getEntity());
        assertThat(response, containsString("\"acknowledged\":true"));

        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "trained_models/" + modelId))
        );
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(
                new Request("GET", InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + TrainedModelDefinitionDoc.docId(modelId, 0))
            )
        );
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + modelId))
        );
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testGetPrePackagedModels() throws IOException {
        Response getModel = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "trained_models/lang_ident_model_1?human=true&include=definition")
        );

        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));
        String response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("lang_ident_model_1"));
        assertThat(response, containsString("\"definition\""));
    }

    @SuppressWarnings("unchecked")
    public void testExportImportModel() throws IOException {
        String modelId = "regression_model_to_export";
        putRegressionModel(modelId);
        Response getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "trained_models/" + modelId));

        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));
        String response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"regression_model_to_export\""));
        assertThat(response, containsString("\"count\":1"));

        getModel = client().performRequest(
            new Request(
                "GET",
                MachineLearning.BASE_PATH
                    + "trained_models/"
                    + modelId
                    + "?include=definition&decompress_definition=false&exclude_generated=true"
            )
        );
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> exportedModel = entityAsMap(getModel);
        Map<String, Object> modelDefinition = ((List<Map<String, Object>>) exportedModel.get("trained_model_configs")).get(0);
        modelDefinition.remove("model_id");

        String importedModelId = "regression_model_to_import";
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(modelDefinition);
            Request model = new Request("PUT", "_ml/trained_models/" + importedModelId);
            model.setJsonEntity(XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON));
            assertThat(client().performRequest(model).getStatusLine().getStatusCode(), equalTo(200));
        }
        getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "trained_models/regression*"));

        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));
        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"regression_model_to_export\""));
        assertThat(response, containsString("\"model_id\":\"regression_model_to_import\""));
        assertThat(response, containsString("\"count\":2"));
    }

    private void putRegressionModel(String modelId) throws IOException {
        String modelConfig = """
                     {
                        "definition": {
                            "trained_model": {
                                "ensemble": {
                                    "feature_names": ["field.foo", "field.bar", "animal_cat", "animal_dog"],
                                    "trained_models": [{
                                        "tree": {
                                            "feature_names": ["field.foo", "field.bar", "animal_cat", "animal_dog"],
                                            "tree_structure": [{
                                                "threshold": 0.5,
                                                "split_feature": 0,
                                                "node_index": 0,
                                                "left_child": 1,
                                                "right_child": 2
                                            }, {
                                                "node_index": 1,
                                                "leaf_value": [0.3]
                                            }, {
                                                "threshold": 0.0,
                                                "split_feature": 3,
                                                "node_index": 2,
                                                "left_child": 3,
                                                "right_child": 4
                                            }, {
                                                "node_index": 3,
                                                "leaf_value": [0.1]
                                            }, {
                                                "node_index": 4,
                                                "leaf_value": [0.2]
                                            }]
                                        }
                                    }, {
                                        "tree": {
                                            "feature_names": ["field.foo", "field.bar", "animal_cat", "animal_dog"],
                                            "tree_structure": [{
                                                "threshold": 1.0,
                                                "split_feature": 2,
                                                "node_index": 0,
                                                "left_child": 1,
                                                "right_child": 2
                                            }, {
                                                "node_index": 1,
                                                "leaf_value": [1.5]
                                            }, {
                                                "node_index": 2,
                                                "leaf_value": [0.9]
                                            }]
                                        }
                                    }, {
                                        "tree": {
                                            "feature_names": ["field.foo", "field.bar", "animal_cat", "animal_dog"],
                                            "tree_structure": [{
                                                "threshold": 0.2,
                                                "split_feature": 1,
                                                "node_index": 0,
                                                "left_child": 1,
                                                "right_child": 2
                                            }, {
                                                "node_index": 1,
                                                "leaf_value": [1.5]
                                            }, {
                                                "node_index": 2,
                                                "leaf_value": [0.9]
                                            }]
                                        }
                                    }],
                                    "aggregate_output": {
                                        "weighted_sum": {
                                            "weights": [0.5, 0.5, 0.5]
                                        }
                                    },
                                    "target_type": "regression"
                                }
                            },
                            "preprocessors": []
                        },
                        "input": {
                            "field_names": ["col1", "col2", "col3"]
                        },
                        "inference_config": {
                            "regression": {}
                        }
                    }
            """;

        Request model = new Request("PUT", "_ml/trained_models/" + modelId);
        model.setJsonEntity(modelConfig);
        assertThat(client().performRequest(model).getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testStartDeploymentWithInconsistentTotalLengths() throws IOException {
        String modelId = "inconsistent-size-model";
        putPyTorchModel(modelId);

        putModelDefinitionPart(modelId, 500, 3, 0);
        putModelDefinitionPart(modelId, 500, 3, 1);
        putModelDefinitionPart(modelId, 600, 3, 2);

        ResponseException responseException = expectThrows(ResponseException.class, () -> startDeployment(modelId));
        assertThat(
            responseException.getMessage(),
            containsString(
                "[total_definition_length] must be the same in all model definition parts. "
                    + "The value [600] in model definition part [2] does not match the value [500] in part [0]"
            )
        );

    }

    private void putPyTorchModel(String modelId) throws IOException {
        Request request = new Request("PUT", "/_ml/trained_models/" + modelId);
        request.setJsonEntity("""
            {
              "description": "simple model for testing",
              "model_type": "pytorch",
              "inference_config": {
                "pass_through": {}
              }
            }""");
        client().performRequest(request);
    }

    private void putModelDefinitionPart(String modelId, int totalSize, int numParts, int partNumber) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId + "/definition/" + partNumber);
        request.setJsonEntity("""
            {
              "total_definition_length": %s,
              "definition": "UEsDBAAACAgAAAAAAAAAAAAAAAAAAAAAAAAUAA4Ac2ltcGxlbW9kZW==",
              "total_parts": %s
            }""".formatted(totalSize, numParts));
        client().performRequest(request);
    }

    private void startDeployment(String modelId) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/deployment/_start?timeout=40s");
        client().performRequest(request);
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        ESRestTestCase.waitForPendingTasks(adminClient());
    }
}
