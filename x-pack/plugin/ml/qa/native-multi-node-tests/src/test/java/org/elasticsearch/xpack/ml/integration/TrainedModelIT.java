/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.License;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.InferenceToXContentCompressor;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModelTests;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

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
        Request model1 = new Request("PUT",
            InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + modelId);
        model1.setJsonEntity(buildRegressionModel(modelId));
        assertThat(client().performRequest(model1).getStatusLine().getStatusCode(), equalTo(201));

        Request modelDefinition1 = new Request("PUT",
            InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + TrainedModelDefinitionDoc.docId(modelId, 0));
        modelDefinition1.setJsonEntity(buildRegressionModelDefinitionDoc(modelId));
        assertThat(client().performRequest(modelDefinition1).getStatusLine().getStatusCode(), equalTo(201));

        Request model2 = new Request("PUT",
            InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + modelId2);
        model2.setJsonEntity(buildRegressionModel(modelId2));
        assertThat(client().performRequest(model2).getStatusLine().getStatusCode(), equalTo(201));

        adminClient().performRequest(new Request("POST", InferenceIndexConstants.LATEST_INDEX_NAME + "/_refresh"));
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
        Request model1 = new Request("PUT",
            InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + modelId);
        model1.setJsonEntity(buildRegressionModel(modelId));
        assertThat(client().performRequest(model1).getStatusLine().getStatusCode(), equalTo(201));

        Request modelDefinition1 = new Request("PUT",
            InferenceIndexConstants.LATEST_INDEX_NAME + "/_doc/" + TrainedModelDefinitionDoc.docId(modelId, 0));
        modelDefinition1.setJsonEntity(buildRegressionModelDefinitionDoc(modelId));
        assertThat(client().performRequest(modelDefinition1).getStatusLine().getStatusCode(), equalTo(201));

        adminClient().performRequest(new Request("POST", InferenceIndexConstants.LATEST_INDEX_NAME + "/_refresh"));

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

    private static String buildRegressionModel(String modelId) throws IOException {
        try(XContentBuilder builder = XContentFactory.jsonBuilder()) {
            TrainedModelConfig.builder()
                .setModelId(modelId)
                .setInput(new TrainedModelInput(Arrays.asList("col1", "col2", "col3")))
                .setCreatedBy("ml_test")
                .setVersion(Version.CURRENT)
                .setCreateTime(Instant.now())
                .setEstimatedOperations(0)
                .setLicenseLevel(License.OperationMode.PLATINUM.description())
                .setEstimatedHeapMemory(0)
                .build()
                .toXContent(builder, new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")));
            return XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
        }
    }

    private static String buildRegressionModelDefinitionDoc(String modelId) throws IOException {
        try(XContentBuilder builder = XContentFactory.jsonBuilder()) {
            TrainedModelDefinition definition = new TrainedModelDefinition.Builder()
                .setPreProcessors(Collections.emptyList())
                .setTrainedModel(LocalModelTests.buildRegression())
                .build();
            String compressedString = InferenceToXContentCompressor.deflate(definition);
            TrainedModelDefinitionDoc doc = new TrainedModelDefinitionDoc.Builder().setDocNum(0)
                .setCompressedString(compressedString)
                .setTotalDefinitionLength(compressedString.length())
                .setDefinitionLength(compressedString.length())
                .setCompressionVersion(1)
                .setModelId(modelId).build();
            doc.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")));
            return XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON);
        }
    }


    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).clearMlMetadata();
        ESRestTestCase.waitForPendingTasks(adminClient());
    }
}
