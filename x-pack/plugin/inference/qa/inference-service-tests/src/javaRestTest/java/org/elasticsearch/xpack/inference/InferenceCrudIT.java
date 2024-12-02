/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;

public class InferenceCrudIT extends InferenceBaseRestTest {

    @SuppressWarnings("unchecked")
    public void testCRUD() throws IOException {
        for (int i = 0; i < 5; i++) {
            putModel("se_model_" + i, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            putModel("te_model_" + i, mockSparseServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        }

        var getAllModels = getAllModels();
        int numModels = 11;
        assertThat(getAllModels, hasSize(numModels));

        var getSparseModels = getModels("_all", TaskType.SPARSE_EMBEDDING);
        int numSparseModels = 6;
        assertThat(getSparseModels, hasSize(numSparseModels));
        for (var sparseModel : getSparseModels) {
            assertEquals("sparse_embedding", sparseModel.get("task_type"));
        }

        var getDenseModels = getModels("_all", TaskType.TEXT_EMBEDDING);
        int numDenseModels = 5;
        assertThat(getDenseModels, hasSize(numDenseModels));
        for (var denseModel : getDenseModels) {
            assertEquals("text_embedding", denseModel.get("task_type"));
        }
        String oldApiKey;
        {
            var singleModel = getModels("se_model_1", TaskType.SPARSE_EMBEDDING);
            assertThat(singleModel, hasSize(1));
            assertEquals("se_model_1", singleModel.get(0).get("inference_id"));
            oldApiKey = (String) singleModel.get(0).get("api_key");
        }
        var newApiKey = randomAlphaOfLength(10);
        int temperature = randomIntBetween(1, 10);
        Map<String, Object> updatedEndpoint = updateEndpoint(
            "se_model_1",
            updateConfig(TaskType.SPARSE_EMBEDDING, newApiKey, temperature),
            TaskType.SPARSE_EMBEDDING
        );
        Map<String, Objects> updatedTaskSettings = (Map<String, Objects>) updatedEndpoint.get("task_settings");
        assertEquals(temperature, updatedTaskSettings.get("temperature"));
        {
            var singleModel = getModels("se_model_1", TaskType.SPARSE_EMBEDDING);
            assertThat(singleModel, hasSize(1));
            assertEquals("se_model_1", singleModel.get(0).get("inference_id"));
            assertNotEquals(oldApiKey, newApiKey);
            assertEquals(updatedEndpoint, singleModel.get(0));
        }
        for (int i = 0; i < 5; i++) {
            deleteModel("se_model_" + i, TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            deleteModel("te_model_" + i, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testGetModelWithWrongTaskType() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> getModels("sparse_embedding_model", TaskType.TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the inference endpoint's task type [sparse_embedding]")
        );
    }

    public void testDeleteModelWithWrongTaskType() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> deleteModel("sparse_embedding_model", TaskType.TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the inference endpoint's task type [sparse_embedding]")
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetModelWithAnyTaskType() throws IOException {
        String inferenceEntityId = "sparse_embedding_model";
        putModel(inferenceEntityId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var singleModel = getModels(inferenceEntityId, TaskType.ANY);
        assertEquals(inferenceEntityId, singleModel.get(0).get("inference_id"));
        assertEquals(TaskType.SPARSE_EMBEDDING.toString(), singleModel.get(0).get("task_type"));
    }

    @SuppressWarnings("unchecked")
    public void testApisWithoutTaskType() throws IOException {
        String modelId = "no_task_type_in_url";
        putModel(modelId, mockSparseServiceModelConfig(TaskType.SPARSE_EMBEDDING));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("inference_id"));
        assertEquals(TaskType.SPARSE_EMBEDDING.toString(), singleModel.get("task_type"));

        var inference = infer(modelId, List.of(randomAlphaOfLength(10)));
        assertNonEmptyInferenceResults(inference, 1, TaskType.SPARSE_EMBEDDING);
        deleteModel(modelId);
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithoutTaskType() throws IOException {
        List<Object> services = getAllServices();
        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            assertThat(services.size(), equalTo(18));
        } else {
            assertThat(services.size(), equalTo(17));
        }

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("provider");
        }

        Arrays.sort(providers);

        var providerList = new ArrayList<>(
            Arrays.asList(
                "alibabacloud-ai-search",
                "amazonbedrock",
                "anthropic",
                "azureaistudio",
                "azureopenai",
                "cohere",
                "elasticsearch",
                "googleaistudio",
                "googlevertexai",
                "hugging_face",
                "mistral",
                "openai",
                "streaming_completion_test_service",
                "test_reranking_service",
                "test_service",
                "text_embedding_test_service",
                "watsonxai"
            )
        );
        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            providerList.add(6, "elastic");
        }
        assertArrayEquals(providers, providerList.toArray());
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithTextEmbeddingTaskType() throws IOException {
        List<Object> services = getServices(TaskType.TEXT_EMBEDDING);
        assertThat(services.size(), equalTo(13));

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("provider");
        }

        Arrays.sort(providers);
        assertArrayEquals(
            providers,
            List.of(
                "alibabacloud-ai-search",
                "amazonbedrock",
                "azureaistudio",
                "azureopenai",
                "cohere",
                "elasticsearch",
                "googleaistudio",
                "googlevertexai",
                "hugging_face",
                "mistral",
                "openai",
                "text_embedding_test_service",
                "watsonxai"
            ).toArray()
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithRerankTaskType() throws IOException {
        List<Object> services = getServices(TaskType.RERANK);
        assertThat(services.size(), equalTo(5));

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("provider");
        }

        Arrays.sort(providers);
        assertArrayEquals(
            providers,
            List.of("alibabacloud-ai-search", "cohere", "elasticsearch", "googlevertexai", "test_reranking_service").toArray()
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithCompletionTaskType() throws IOException {
        List<Object> services = getServices(TaskType.COMPLETION);
        assertThat(services.size(), equalTo(9));

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("provider");
        }

        Arrays.sort(providers);
        assertArrayEquals(
            providers,
            List.of(
                "alibabacloud-ai-search",
                "amazonbedrock",
                "anthropic",
                "azureaistudio",
                "azureopenai",
                "cohere",
                "googleaistudio",
                "openai",
                "streaming_completion_test_service"
            ).toArray()
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetServicesWithSparseEmbeddingTaskType() throws IOException {
        List<Object> services = getServices(TaskType.SPARSE_EMBEDDING);

        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            assertThat(services.size(), equalTo(5));
        } else {
            assertThat(services.size(), equalTo(4));
        }

        String[] providers = new String[services.size()];
        for (int i = 0; i < services.size(); i++) {
            Map<String, Object> serviceConfig = (Map<String, Object>) services.get(i);
            providers[i] = (String) serviceConfig.get("provider");
        }

        Arrays.sort(providers);

        var providerList = new ArrayList<>(Arrays.asList("alibabacloud-ai-search", "elasticsearch", "hugging_face", "test_service"));
        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            providerList.add(1, "elastic");
        }
        assertArrayEquals(providers, providerList.toArray());
    }

    public void testSkipValidationAndStart() throws IOException {
        String openAiConfigWithBadApiKey = """
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX"
                },
                "task_settings": {
                   "model": "text-embedding-ada-002"
                }
            }
            """;

        updateClusterSettings(Settings.builder().put("xpack.inference.skip_validate_and_start", true).build());

        // We would expect an error about the invalid API key if the validation occurred
        putModel("unvalidated", openAiConfigWithBadApiKey, TaskType.TEXT_EMBEDDING);
    }

    public void testDeleteEndpointWhileReferencedByPipeline() throws IOException {
        String endpointId = "endpoint_referenced_by_pipeline";
        putModel(endpointId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var pipelineId = "pipeline_referencing_model";
        putPipeline(pipelineId, endpointId);

        {
            var errorString = new StringBuilder().append("Inference endpoint ")
                .append(endpointId)
                .append(" is referenced by pipelines: ")
                .append(Set.of(pipelineId))
                .append(". ")
                .append("Ensure that no pipelines are using this inference endpoint, ")
                .append("or use force to ignore this warning and delete the inference endpoint.");
            var e = expectThrows(ResponseException.class, () -> deleteModel(endpointId));
            assertThat(e.getMessage(), containsString(errorString.toString()));
        }
        {
            var response = deleteModel(endpointId, "dry_run=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString(pipelineId));
            assertThat(entityString, containsString("\"acknowledged\":false"));
        }
        {
            var response = deleteModel(endpointId, "force=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString("\"acknowledged\":true"));
        }
        deletePipeline(pipelineId);
    }

    public void testDeleteEndpointWhileReferencedBySemanticText() throws IOException {
        final String endpointId = "endpoint_referenced_by_semantic_text";
        final String searchEndpointId = "search_endpoint_referenced_by_semantic_text";
        final String indexName = randomAlphaOfLength(10).toLowerCase();
        final Function<String, String> buildErrorString = endpointName -> " Inference endpoint "
            + endpointName
            + " is being used in the mapping for indexes: "
            + Set.of(indexName)
            + ". Ensure that no index mappings are using this inference endpoint, or use force to ignore this warning and delete the"
            + " inference endpoint.";

        putModel(endpointId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        putSemanticText(endpointId, indexName);
        {
            var e = expectThrows(ResponseException.class, () -> deleteModel(endpointId));
            assertThat(e.getMessage(), containsString(buildErrorString.apply(endpointId)));
        }
        {
            var response = deleteModel(endpointId, "dry_run=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString("\"acknowledged\":false"));
            assertThat(entityString, containsString(indexName));
            assertThat(entityString, containsString(endpointId));
        }
        {
            var response = deleteModel(endpointId, "force=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString("\"acknowledged\":true"));
        }
        deleteIndex(indexName);

        putModel(searchEndpointId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        putSemanticText(endpointId, searchEndpointId, indexName);
        {
            var e = expectThrows(ResponseException.class, () -> deleteModel(searchEndpointId));
            assertThat(e.getMessage(), containsString(buildErrorString.apply(searchEndpointId)));
        }
        {
            var response = deleteModel(searchEndpointId, "dry_run=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString("\"acknowledged\":false"));
            assertThat(entityString, containsString(indexName));
            assertThat(entityString, containsString(searchEndpointId));
        }
        {
            var response = deleteModel(searchEndpointId, "force=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString("\"acknowledged\":true"));
        }
        deleteIndex(indexName);
    }

    public void testDeleteEndpointWhileReferencedBySemanticTextAndPipeline() throws IOException {
        String endpointId = "endpoint_referenced_by_semantic_text";
        putModel(endpointId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        String indexName = randomAlphaOfLength(10).toLowerCase();
        putSemanticText(endpointId, indexName);
        var pipelineId = "pipeline_referencing_model";
        putPipeline(pipelineId, endpointId);
        {

            var errorString = new StringBuilder().append("Inference endpoint ")
                .append(endpointId)
                .append(" is referenced by pipelines: ")
                .append(Set.of(pipelineId))
                .append(". ")
                .append("Ensure that no pipelines are using this inference endpoint, ")
                .append("or use force to ignore this warning and delete the inference endpoint.")
                .append(" Inference endpoint ")
                .append(endpointId)
                .append(" is being used in the mapping for indexes: ")
                .append(Set.of(indexName))
                .append(". ")
                .append("Ensure that no index mappings are using this inference endpoint, ")
                .append("or use force to ignore this warning and delete the inference endpoint.");

            var e = expectThrows(ResponseException.class, () -> deleteModel(endpointId));
            assertThat(e.getMessage(), containsString(errorString.toString()));
        }
        {
            var response = deleteModel(endpointId, "dry_run=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString("\"acknowledged\":false"));
            assertThat(entityString, containsString(indexName));
            assertThat(entityString, containsString(pipelineId));
            assertThat(entityString, containsString(endpointId));
        }
        {
            var response = deleteModel(endpointId, "force=true");
            var entityString = EntityUtils.toString(response.getEntity());
            assertThat(entityString, containsString("\"acknowledged\":true"));
        }
        deletePipeline(pipelineId);
        deleteIndex(indexName);
    }

    public void testUnsupportedStream() throws Exception {
        String modelId = "streaming";
        putModel(modelId, mockCompletionServiceModelConfig(TaskType.SPARSE_EMBEDDING));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("inference_id"));
        assertEquals(TaskType.SPARSE_EMBEDDING.toString(), singleModel.get("task_type"));

        try {
            var events = streamInferOnMockService(modelId, TaskType.SPARSE_EMBEDDING, List.of(randomUUID()));
            assertThat(events.size(), equalTo(2));
            events.forEach(event -> {
                switch (event.name()) {
                    case EVENT -> assertThat(event.value(), equalToIgnoringCase("error"));
                    case DATA -> assertThat(
                        event.value(),
                        containsString(
                            "Streaming is not allowed for service [streaming_completion_test_service] and task [sparse_embedding]"
                        )
                    );
                }
            });
        } finally {
            deleteModel(modelId);
        }
    }

    public void testSupportedStream() throws Exception {
        String modelId = "streaming";
        putModel(modelId, mockCompletionServiceModelConfig(TaskType.COMPLETION));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("inference_id"));
        assertEquals(TaskType.COMPLETION.toString(), singleModel.get("task_type"));

        var input = IntStream.range(1, 2 + randomInt(8)).mapToObj(i -> randomUUID()).toList();
        try {
            var events = streamInferOnMockService(modelId, TaskType.COMPLETION, input);

            var expectedResponses = Stream.concat(
                input.stream().map(String::toUpperCase).map(str -> "{\"completion\":[{\"delta\":\"" + str + "\"}]}"),
                Stream.of("[DONE]")
            ).iterator();
            assertThat(events.size(), equalTo((input.size() + 1) * 2));
            events.forEach(event -> {
                switch (event.name()) {
                    case EVENT -> assertThat(event.value(), equalToIgnoringCase("message"));
                    case DATA -> assertThat(event.value(), equalTo(expectedResponses.next()));
                }
            });
        } finally {
            deleteModel(modelId);
        }
    }

    public void testGetZeroModels() throws IOException {
        var models = getModels("_all", TaskType.RERANK);
        assertThat(models, empty());
    }
}
