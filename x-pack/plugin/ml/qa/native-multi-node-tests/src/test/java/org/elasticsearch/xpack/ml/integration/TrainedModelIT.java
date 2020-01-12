/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.client.ml.inference.TrainedModelDefinition;
import org.elasticsearch.client.ml.inference.TrainedModelInput;
import org.elasticsearch.client.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.client.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.client.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.client.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TrainedModelIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user",
            SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);

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
        Response getModel = client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/" + modelId));

        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));
        String response = EntityUtils.toString(getModel.getEntity());

        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"count\":1"));

        getModel = client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/a_test_regression*"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model-2\""));
        assertThat(response, not(containsString("\"definition\"")));
        assertThat(response, containsString("\"count\":2"));

        getModel = client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/a_test_regression_model?human=true&include_model_definition=true"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"estimated_heap_memory_usage_bytes\""));
        assertThat(response, containsString("\"estimated_heap_memory_usage\""));
        assertThat(response, containsString("\"definition\""));
        assertThat(response, containsString("\"count\":1"));

        getModel = client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/a_test_regression_model?decompress_definition=false&include_model_definition=true"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"estimated_heap_memory_usage_bytes\""));
        assertThat(response, containsString("\"compressed_definition\""));
        assertThat(response, not(containsString("\"definition\"")));
        assertThat(response, containsString("\"count\":1"));

        ResponseException responseException = expectThrows(ResponseException.class, () ->
            client().performRequest(new Request("GET",
                MachineLearning.BASE_PATH + "inference/a_test_regression*?human=true&include_model_definition=true")));
        assertThat(EntityUtils.toString(responseException.getResponse().getEntity()),
            containsString(Messages.INFERENCE_TOO_MANY_DEFINITIONS_REQUESTED));

        getModel = client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/a_test_regression_model,a_test_regression_model-2"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model-2\""));
        assertThat(response, containsString("\"count\":2"));

        getModel = client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/classification*?allow_no_match=true"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"count\":0"));

        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/classification*?allow_no_match=false")));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "inference?from=0&size=1"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"count\":3"));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model\""));
        assertThat(response, not(containsString("\"model_id\":\"a_test_regression_model-2\"")));

        getModel = client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "inference?from=1&size=1"));
        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));

        response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("\"count\":3"));
        assertThat(response, not(containsString("\"model_id\":\"a_test_regression_model\"")));
        assertThat(response, containsString("\"model_id\":\"a_test_regression_model-2\""));
    }

    public void testDeleteTrainedModels() throws IOException {
        String modelId = "test_delete_regression_model";
        putRegressionModel(modelId);

        Response delModel = client().performRequest(new Request("DELETE",
            MachineLearning.BASE_PATH + "inference/" + modelId));
        String response = EntityUtils.toString(delModel.getEntity());
        assertThat(response, containsString("\"acknowledged\":true"));

        ResponseException responseException = expectThrows(ResponseException.class,
            () -> client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "inference/" + modelId)));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        responseException = expectThrows(ResponseException.class,
            () -> client().performRequest(
                new Request("GET",
                    InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + TrainedModelDefinitionDoc.docId(modelId, 0))));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        responseException = expectThrows(ResponseException.class,
            () -> client().performRequest(
                new Request("GET",
                    InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + modelId)));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testGetPrePackagedModels() throws IOException {
        Response getModel = client().performRequest(new Request("GET",
            MachineLearning.BASE_PATH + "inference/lang_ident_model_1?human=true&include_model_definition=true"));

        assertThat(getModel.getStatusLine().getStatusCode(), equalTo(200));
        String response = EntityUtils.toString(getModel.getEntity());
        assertThat(response, containsString("lang_ident_model_1"));
        assertThat(response, containsString("\"definition\""));
    }

    private void putRegressionModel(String modelId) throws IOException {
        try(XContentBuilder builder = XContentFactory.jsonBuilder()) {
            TrainedModelDefinition.Builder definition = new TrainedModelDefinition.Builder()
                .setPreProcessors(Collections.emptyList())
                .setTrainedModel(buildRegression());
            TrainedModelConfig.builder()
                .setDefinition(definition)
                .setModelId(modelId)
                .setInput(new TrainedModelInput(Arrays.asList("col1", "col2", "col3")))
                .build().toXContent(builder, ToXContent.EMPTY_PARAMS);
            Request model = new Request("PUT", "_ml/inference/" + modelId);
            model.setJsonEntity(XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON));
            assertThat(client().performRequest(model).getStatusLine().getStatusCode(), equalTo(200));
        }
    }

    private static TrainedModel buildRegression() {
        List<String> featureNames = Arrays.asList("field.foo", "field.bar", "animal_cat", "animal_dog");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5),
                TreeNode.builder(1).setLeafValue(0.3),
                TreeNode.builder(2)
                .setThreshold(0.0)
                .setSplitFeature(3)
                .setLeftChild(3)
                .setRightChild(4),
                TreeNode.builder(3).setLeafValue(0.1),
                TreeNode.builder(4).setLeafValue(0.2))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(2)
                .setThreshold(1.0),
                TreeNode.builder(1).setLeafValue(1.5),
                TreeNode.builder(2).setLeafValue(0.9))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(1)
                .setThreshold(0.2),
                TreeNode.builder(1).setLeafValue(1.5),
                TreeNode.builder(2).setLeafValue(0.9))
            .build();
        return Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedSum(Arrays.asList(0.5, 0.5, 0.5)))
            .build();
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).clearMlMetadata();
        ESRestTestCase.waitForPendingTasks(adminClient());
    }
}
