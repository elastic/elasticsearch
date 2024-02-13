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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class InferenceBaseRestTest extends ESRestTestCase {
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
        return mockServiceModelConfig(null);
    }

    static String mockServiceModelConfig(@Nullable TaskType taskTypeInBody) {
        var taskType = taskTypeInBody == null ? "" : "\"task_type\": \"" + taskTypeInBody + "\",";
        return Strings.format("""
            {
              %s
              "service": "test_service",
              "service_settings": {
                "model": "my_model",
                "hidden_field": "my_hidden_value",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """, taskType);
    }

    static String mockServiceModelConfig(@Nullable TaskType taskTypeInBody, boolean shouldReturnHiddenField) {
        var taskType = taskTypeInBody == null ? "" : "\"task_type\": \"" + taskTypeInBody + "\",";
        return Strings.format("""
            {
              %s
              "service": "test_service",
              "service_settings": {
                "model": "my_model",
                "hidden_field": "my_hidden_value",
                "should_return_hidden_field": %s,
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """, taskType, shouldReturnHiddenField);
    }

    protected void deleteModel(String modelId) throws IOException {
        var request = new Request("DELETE", "_inference/" + modelId);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
    }

    protected void deleteModel(String modelId, TaskType taskType) throws IOException {
        var request = new Request("DELETE", Strings.format("_inference/%s/%s", taskType, modelId));
        var response = client().performRequest(request);
        assertOkOrCreated(response);
    }

    protected Map<String, Object> putModel(String modelId, String modelConfig, TaskType taskType) throws IOException {
        String endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        return putModelInternal(endpoint, modelConfig);
    }

    /**
     * Task type should be in modelConfig
     */
    protected Map<String, Object> putModel(String modelId, String modelConfig) throws IOException {
        String endpoint = Strings.format("_inference/%s", modelId);
        return putModelInternal(endpoint, modelConfig);
    }

    private Map<String, Object> putModelInternal(String endpoint, String modelConfig) throws IOException {
        var request = new Request("PUT", endpoint);
        request.setJsonEntity(modelConfig);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> putE5TrainedModels() throws IOException {
        var request = new Request("PUT", "_ml/trained_models/.multilingual-e5-small?wait_for_completion=true");

        String body = """
                {
                    "input": {
                    "field_names": ["text_field"]
                    }
                }
            """;

        request.setJsonEntity(body);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> deployE5TrainedModels() throws IOException {
        var request = new Request("POST", "_ml/trained_models/.multilingual-e5-small/deployment/_start?wait_for=fully_allocated");

        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> getModel(String modelId) throws IOException {
        var endpoint = Strings.format("_inference/%s", modelId);
        return getAllModelInternal(endpoint);
    }

    protected Map<String, Object> getModels(String modelId, TaskType taskType) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        return getAllModelInternal(endpoint);
    }

    protected Map<String, Object> getAllModels() throws IOException {
        var endpoint = Strings.format("_inference/_all");
        return getAllModelInternal("_inference/_all");
    }

    private Map<String, Object> getAllModelInternal(String endpoint) throws IOException {
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    protected Map<String, Object> inferOnMockService(String modelId, List<String> input) throws IOException {
        var endpoint = Strings.format("_inference/%s", modelId);
        return inferOnMockServiceInternal(endpoint, input);
    }

    protected Map<String, Object> inferOnMockService(String modelId, TaskType taskType, List<String> input) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        return inferOnMockServiceInternal(endpoint, input);
    }

    private Map<String, Object> inferOnMockServiceInternal(String endpoint, List<String> input) throws IOException {
        var request = new Request("POST", endpoint);

        var bodyBuilder = new StringBuilder("{\"input\": [");
        for (var in : input) {
            bodyBuilder.append('"').append(in).append('"').append(',');
        }
        // remove last comma
        bodyBuilder.deleteCharAt(bodyBuilder.length() - 1);
        bodyBuilder.append("]}");

        request.setJsonEntity(bodyBuilder.toString());
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    @SuppressWarnings("unchecked")
    protected void assertNonEmptyInferenceResults(Map<String, Object> resultMap, int expectedNumberOfResults, TaskType taskType) {
        if (taskType == TaskType.SPARSE_EMBEDDING) {
            var results = (List<Map<String, Object>>) resultMap.get(TaskType.SPARSE_EMBEDDING.toString());
            assertThat(results, hasSize(expectedNumberOfResults));
        } else {
            fail("test with task type [" + taskType + "] are not supported yet");
        }
    }

    protected static void assertOkOrCreated(Response response) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        // Once EntityUtils.toString(entity) is called the entity cannot be reused.
        // Avoid that call with check here.
        if (statusCode == 200 || statusCode == 201) {
            return;
        }

        String responseStr = EntityUtils.toString(response.getEntity());
        assertThat(responseStr, response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    protected Map<String, Object> getTrainedModel(String inferenceEntityId) throws IOException {
        var endpoint = Strings.format("_ml/trained_models/%s/_stats", inferenceEntityId);
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }
}
