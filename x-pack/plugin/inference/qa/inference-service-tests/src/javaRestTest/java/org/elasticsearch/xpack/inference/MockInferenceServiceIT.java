/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class MockInferenceServiceIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .plugin("org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    static String mockServiceModelConfig() {
        return Strings.format("""
            {
              "service": "test_service",
              "service_settings": {
                "model": "my_model",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """);
    }

    public void testMockService() throws IOException {
        String modelId = "test-mock";
        var putModel = putModel(modelId, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var getModel = getModel(modelId, TaskType.SPARSE_EMBEDDING);
        assertEquals(putModel, getModel);

        for (var modelMap : List.of(putModel, getModel)) {
            assertEquals(modelId, modelMap.get("model_id"));
            assertEquals(TaskType.SPARSE_EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("test_service", modelMap.get("service"));
        }

        // The response is randomly generated, the input can be anything
        var inference = inferOnMockService(modelId, TaskType.SPARSE_EMBEDDING, List.of(randomAlphaOfLength(10)));
        assertNonEmptyInferenceResults(inference, TaskType.SPARSE_EMBEDDING);
    }

    @SuppressWarnings("unchecked")
    public void testMockServiceWithMultipleInputs() throws IOException {
        String modelId = "test-mock-with-multi-inputs";
        var putModel = putModel(modelId, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);

        // The response is randomly generated, the input can be anything
        var inference = inferOnMockService(
            modelId,
            TaskType.SPARSE_EMBEDDING,
            List.of(randomAlphaOfLength(5), randomAlphaOfLength(10), randomAlphaOfLength(15))
        );

        var tokens = (List<Map<String, Object>>) inference.get("inference_results");
        assertThat(tokens, hasSize(3));
        assertNonEmptyInferenceResults(inference, TaskType.SPARSE_EMBEDDING);
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesNotReturnSecretsInGetResponse() throws IOException {
        String modelId = "test-mock";
        var putModel = putModel(modelId, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var getModel = getModel(modelId, TaskType.SPARSE_EMBEDDING);

        var serviceSettings = (Map<String, Object>) getModel.get("service_settings");
        assertNull(serviceSettings.get("api_key"));
        assertNotNull(serviceSettings.get("model"));

        var putServiceSettings = (Map<String, Object>) putModel.get("service_settings");
        assertNull(putServiceSettings.get("api_key"));
        assertNotNull(putServiceSettings.get("model"));
    }

    private Map<String, Object> putModel(String modelId, String modelConfig, TaskType taskType) throws IOException {
        String endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("PUT", endpoint);
        request.setJsonEntity(modelConfig);
        var reponse = client().performRequest(request);
        assertOkWithErrorMessage(reponse);
        return entityAsMap(reponse);
    }

    public Map<String, Object> getModel(String modelId, TaskType taskType) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("GET", endpoint);
        var reponse = client().performRequest(request);
        assertOkWithErrorMessage(reponse);
        return entityAsMap(reponse);
    }

    private Map<String, Object> inferOnMockService(String modelId, TaskType taskType, List<String> input) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        var request = new Request("POST", endpoint);

        var bodyBuilder = new StringBuilder("{\"input\": [");
        for (var in : input) {
            bodyBuilder.append('"').append(in).append('"').append(',');
        }
        // remove last comma
        bodyBuilder.deleteCharAt(bodyBuilder.length() - 1);
        bodyBuilder.append("]}");

        System.out.println("body_request:" + bodyBuilder);
        request.setJsonEntity(bodyBuilder.toString());
        var reponse = client().performRequest(request);
        assertOkWithErrorMessage(reponse);
        return entityAsMap(reponse);
    }

    @SuppressWarnings("unchecked")
    protected void assertNonEmptyInferenceResults(Map<String, Object> resultMap, TaskType taskType) {
        if (taskType == TaskType.SPARSE_EMBEDDING) {
            var tokens = (List<Map<String, Object>>) resultMap.get("inference_results");
            tokens.forEach(result -> { assertThat(result.keySet(), not(empty())); });
        } else {
            fail("test with task type [" + taskType + "] are not supported yet");
        }
    }

    protected static void assertOkWithErrorMessage(Response response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 || statusCode == 201) {
            return;
        }

        String responseStr = EntityUtils.toString(response.getEntity());
        assertThat(responseStr, response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }
}
