/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class InferenceBaseRestTest extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CLIENT_SOCKET_TIMEOUT, "120s")  // Long timeout for model download
            .build();
    }

    static String mockSparseServiceModelConfig() {
        return mockSparseServiceModelConfig(null);
    }

    static String mockSparseServiceModelConfig(@Nullable TaskType taskTypeInBody) {
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

    static String updateConfig(@Nullable TaskType taskTypeInBody, String apiKey, int temperature) {
        var taskType = taskTypeInBody == null ? "" : "\"task_type\": \"" + taskTypeInBody + "\",";
        return Strings.format("""
            {
              %s
              "service_settings": {
                "api_key": "%s"
              },
              "task_settings": {
                "temperature": %d
              }
            }
            """, taskType, apiKey, temperature);
    }

    static String mockCompletionServiceModelConfig(@Nullable TaskType taskTypeInBody) {
        var taskType = taskTypeInBody == null ? "" : "\"task_type\": \"" + taskTypeInBody + "\",";
        return Strings.format("""
            {
              %s
              "service": "streaming_completion_test_service",
              "service_settings": {
                "model": "my_model",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """, taskType);
    }

    static String mockSparseServiceModelConfig(@Nullable TaskType taskTypeInBody, boolean shouldReturnHiddenField) {
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

    static String mockDenseServiceModelConfig() {
        return """
            {
              "task_type": "text_embedding",
              "service": "text_embedding_test_service",
              "service_settings": {
                "model": "my_dense_vector_model",
                "api_key": "abc64",
                "dimensions": 246
              },
              "task_settings": {
              }
            }
            """;
    }

    protected void deleteModel(String modelId) throws IOException {
        var request = new Request("DELETE", "_inference/" + modelId);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
    }

    protected Response deleteModel(String modelId, String queryParams) throws IOException {
        var request = new Request("DELETE", "_inference/" + modelId + "?" + queryParams);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return response;
    }

    protected void deleteModel(String modelId, TaskType taskType) throws IOException {
        var request = new Request("DELETE", Strings.format("_inference/%s/%s", taskType, modelId));
        var response = client().performRequest(request);
        assertOkOrCreated(response);
    }

    protected void putSemanticText(String endpointId, String indexName) throws IOException {
        var request = new Request("PUT", Strings.format("%s", indexName));
        String body = Strings.format("""
            {
                "mappings": {
                "properties": {
                    "inference_field": {
                        "type": "semantic_text",
                            "inference_id": "%s"
                    }
                }
                }
            }
            """, endpointId);
        request.setJsonEntity(body);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
    }

    protected void putSemanticText(String endpointId, String searchEndpointId, String indexName) throws IOException {
        var request = new Request("PUT", Strings.format("%s", indexName));
        String body = Strings.format("""
            {
                "mappings": {
                "properties": {
                    "inference_field": {
                        "type": "semantic_text",
                            "inference_id": "%s",
                            "search_inference_id": "%s"
                    }
                }
                }
            }
            """, endpointId, searchEndpointId);
        request.setJsonEntity(body);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
    }

    protected Map<String, Object> putModel(String modelId, String modelConfig, TaskType taskType) throws IOException {
        String endpoint = Strings.format("_inference/%s/%s?error_trace", taskType, modelId);
        return putRequest(endpoint, modelConfig);
    }

    protected Map<String, Object> updateEndpoint(String inferenceID, String modelConfig, TaskType taskType) throws IOException {
        String endpoint = Strings.format("_inference/%s/%s/_update", taskType, inferenceID);
        return putRequest(endpoint, modelConfig);
    }

    protected Map<String, Object> putPipeline(String pipelineId, String modelId) throws IOException {
        String endpoint = Strings.format("_ingest/pipeline/%s", pipelineId);
        String body = """
            {
              "description": "Test pipeline",
              "processors": [
                {
                  "inference": {
                    "model_id": "%s"
                  }
                }
              ]
            }
            """.formatted(modelId);
        return putRequest(endpoint, body);
    }

    protected void deletePipeline(String pipelineId) throws IOException {
        var request = new Request("DELETE", Strings.format("_ingest/pipeline/%s", pipelineId));
        var response = client().performRequest(request);
        assertOkOrCreated(response);
    }

    /**
     * Task type should be in modelConfig
     */
    protected Map<String, Object> putModel(String modelId, String modelConfig) throws IOException {
        String endpoint = Strings.format("_inference/%s", modelId);
        return putRequest(endpoint, modelConfig);
    }

    Map<String, Object> putRequest(String endpoint, String body) throws IOException {
        var request = new Request("PUT", endpoint);
        request.setJsonEntity(body);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    Map<String, Object> postRequest(String endpoint, String body) throws IOException {
        var request = new Request("POST", endpoint);
        request.setJsonEntity(body);
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

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getModel(String modelId) throws IOException {
        var endpoint = Strings.format("_inference/%s?error_trace", modelId);
        return ((List<Map<String, Object>>) getInternalAsMap(endpoint).get("endpoints")).get(0);
    }

    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> getModels(String modelId, TaskType taskType) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        return (List<Map<String, Object>>) getInternalAsMap(endpoint).get("endpoints");
    }

    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> getAllModels() throws IOException {
        var endpoint = Strings.format("_inference/_all");
        return (List<Map<String, Object>>) getInternalAsMap("_inference/_all").get("endpoints");
    }

    protected List<Object> getAllServices() throws IOException {
        var endpoint = Strings.format("_inference/_services");
        return getInternalAsList(endpoint);
    }

    @SuppressWarnings("unchecked")
    protected List<Object> getServices(TaskType taskType) throws IOException {
        var endpoint = Strings.format("_inference/_services/%s", taskType);
        return getInternalAsList(endpoint);
    }

    private Map<String, Object> getInternalAsMap(String endpoint) throws IOException {
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    private List<Object> getInternalAsList(String endpoint) throws IOException {
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsList(response);
    }

    protected Map<String, Object> infer(String modelId, List<String> input) throws IOException {
        var endpoint = Strings.format("_inference/%s", modelId);
        return inferInternal(endpoint, input, Map.of());
    }

    protected Deque<ServerSentEvent> streamInferOnMockService(String modelId, TaskType taskType, List<String> input) throws Exception {
        var endpoint = Strings.format("_inference/%s/%s/_stream", taskType, modelId);
        return callAsync(endpoint, input);
    }

    private Deque<ServerSentEvent> callAsync(String endpoint, List<String> input) throws Exception {
        var responseConsumer = new AsyncInferenceResponseConsumer();
        var request = new Request("POST", endpoint);
        request.setJsonEntity(jsonBody(input));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(() -> responseConsumer).build());
        var latch = new CountDownLatch(1);
        client().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                latch.countDown();
            }
        });
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        return responseConsumer.events();
    }

    protected Map<String, Object> infer(String modelId, TaskType taskType, List<String> input) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", taskType, modelId);
        return inferInternal(endpoint, input, Map.of());
    }

    protected Map<String, Object> infer(String modelId, TaskType taskType, List<String> input, Map<String, String> queryParameters)
        throws IOException {
        var endpoint = Strings.format("_inference/%s/%s?error_trace", taskType, modelId);
        return inferInternal(endpoint, input, queryParameters);
    }

    protected Request createInferenceRequest(String endpoint, List<String> input, Map<String, String> queryParameters) {
        var request = new Request("POST", endpoint);
        request.setJsonEntity(jsonBody(input));
        if (queryParameters.isEmpty() == false) {
            request.addParameters(queryParameters);
        }
        return request;
    }

    private Map<String, Object> inferInternal(String endpoint, List<String> input, Map<String, String> queryParameters) throws IOException {
        var request = createInferenceRequest(endpoint, input, queryParameters);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    private String jsonBody(List<String> input) {
        var bodyBuilder = new StringBuilder("{\"input\": [");
        for (var in : input) {
            bodyBuilder.append('"').append(in).append('"').append(',');
        }
        // remove last comma
        bodyBuilder.deleteCharAt(bodyBuilder.length() - 1);
        bodyBuilder.append("]}");
        return bodyBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    protected void assertNonEmptyInferenceResults(Map<String, Object> resultMap, int expectedNumberOfResults, TaskType taskType) {
        switch (taskType) {
            case SPARSE_EMBEDDING -> {
                var results = (List<Map<String, Object>>) resultMap.get(TaskType.SPARSE_EMBEDDING.toString());
                assertThat(results, hasSize(expectedNumberOfResults));
            }
            case TEXT_EMBEDDING -> {
                var results = (List<Map<String, Object>>) resultMap.get(TaskType.TEXT_EMBEDDING.toString());
                assertThat(results, hasSize(expectedNumberOfResults));
            }
            default -> fail("test with task type [" + taskType + "] are not supported yet");
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
