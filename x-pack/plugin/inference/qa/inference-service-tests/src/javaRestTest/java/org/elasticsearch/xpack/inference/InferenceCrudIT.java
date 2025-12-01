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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.SPARSE_EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class InferenceCrudIT extends InferenceBaseRestTest {

    private static final Consumer<Response> VALIDATE_ELASTIC_PRODUCT_HEADER_CONSUMER = (r) -> assertThat(
        r.getHeader("X-elastic-product"),
        is("Elasticsearch")
    );

    @SuppressWarnings("unchecked")
    public void testCRUD() throws IOException {
        String sparseEmbeddingModelBase = "se_model_";
        int sparseModelsToCreate = 5;
        for (int i = 0; i < sparseModelsToCreate; i++) {
            putModel(sparseEmbeddingModelBase + i, mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
        }
        String textEmbeddingModelBase = "te_model_";
        int textEmbeddingModelsToCreate = 4;
        for (int i = 0; i < textEmbeddingModelsToCreate; i++) {
            putModel(textEmbeddingModelBase + i, mockTextEmbeddingServiceModelConfig(), TEXT_EMBEDDING);
        }
        String rerankModelBase = "re_model_";
        int rerankModelsToCreate = 3;
        for (int i = 0; i < rerankModelsToCreate; i++) {
            putModel(rerankModelBase + i, mockRerankServiceModelConfig(), RERANK);
        }

        String embeddingModelBase = "e_model_";
        int embeddingModelsToCreate = 2;
        for (int i = 0; i < embeddingModelsToCreate; i++) {
            putModel(embeddingModelBase + i, mockEmbeddingServiceModelConfig(), TaskType.EMBEDDING);
        }

        var getAllModels = getAllModels();
        // The three extra models come from the default sparse embedding, text embedding and rerank models
        int numModels = sparseModelsToCreate + textEmbeddingModelsToCreate + rerankModelsToCreate + embeddingModelsToCreate + 3;
        assertThat(getAllModels, hasSize(numModels));

        // Add one for the default sparse embedding model
        assertModelCount(SPARSE_EMBEDDING, sparseModelsToCreate + 1);

        // Add one for the default text embedding model
        assertModelCount(TEXT_EMBEDDING, textEmbeddingModelsToCreate + 1);

        // Add one for the default rerank model
        assertModelCount(RERANK, rerankModelsToCreate + 1);

        assertModelCount(EMBEDDING, embeddingModelsToCreate);

        String oldApiKey;
        String sparseEmbeddingModelId = sparseEmbeddingModelBase + 1;
        {
            var singleModel = getModels(sparseEmbeddingModelId, SPARSE_EMBEDDING);
            assertThat(singleModel, hasSize(1));
            assertEquals(sparseEmbeddingModelId, singleModel.get(0).get("inference_id"));
            oldApiKey = (String) singleModel.get(0).get("api_key");
        }
        var newApiKey = randomAlphaOfLength(10);
        int temperature = randomIntBetween(1, 10);
        Map<String, Object> updatedEndpoint = updateEndpoint(
            sparseEmbeddingModelId,
            updateConfig(SPARSE_EMBEDDING, newApiKey, temperature),
            SPARSE_EMBEDDING
        );
        Map<String, Objects> updatedTaskSettings = (Map<String, Objects>) updatedEndpoint.get("task_settings");
        assertEquals(temperature, updatedTaskSettings.get("temperature"));
        {
            var singleModel = getModels(sparseEmbeddingModelId, SPARSE_EMBEDDING);
            assertThat(singleModel, hasSize(1));
            assertEquals(sparseEmbeddingModelId, singleModel.get(0).get("inference_id"));
            assertNotEquals(oldApiKey, newApiKey);
            assertEquals(updatedEndpoint, singleModel.get(0));
        }
        for (int i = 0; i < sparseModelsToCreate; i++) {
            deleteModel(sparseEmbeddingModelBase + i, SPARSE_EMBEDDING);
        }
        for (int i = 0; i < textEmbeddingModelsToCreate; i++) {
            deleteModel(textEmbeddingModelBase + i, TEXT_EMBEDDING);
        }
        for (int i = 0; i < rerankModelsToCreate; i++) {
            deleteModel(rerankModelBase + i, RERANK);
        }
        for (int i = 0; i < embeddingModelsToCreate; i++) {
            deleteModel(embeddingModelBase + i, TaskType.EMBEDDING);
        }
    }

    private static void assertModelCount(TaskType taskType, int expectedNumberOfModels) throws IOException {
        var models = getModels("_all", taskType);
        assertThat(models, hasSize(expectedNumberOfModels));
        for (var model : models) {
            assertEquals(taskType.toString(), model.get("task_type"));
        }
    }

    public void testGetModelWithWrongTaskType() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> getModels("sparse_embedding_model", TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the inference endpoint's task type [sparse_embedding]")
        );
    }

    public void testDeleteModelWithWrongTaskType() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> deleteModel("sparse_embedding_model", TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the inference endpoint's task type [sparse_embedding]")
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetModelWithAnyTaskType() throws IOException {
        String inferenceEntityId = "sparse_embedding_model";
        putModel(inferenceEntityId, mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
        var singleModel = getModels(inferenceEntityId, TaskType.ANY);
        assertEquals(inferenceEntityId, singleModel.get(0).get("inference_id"));
        assertEquals(SPARSE_EMBEDDING.toString(), singleModel.get(0).get("task_type"));
    }

    @SuppressWarnings("unchecked")
    public void testApisWithoutTaskType() throws IOException {
        String modelId = "no_task_type_in_url";
        putModel(modelId, mockSparseServiceModelConfig(SPARSE_EMBEDDING));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("inference_id"));
        assertEquals(SPARSE_EMBEDDING.toString(), singleModel.get("task_type"));

        var inference = infer(modelId, List.of(randomAlphaOfLength(10)));
        assertNonEmptyInferenceResults(inference, 1, SPARSE_EMBEDDING);
        deleteModel(modelId);
    }

    public void testSkipValidationAndStart() throws IOException {
        String openAiConfigWithBadApiKey = """
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX",
                    "dimensions": 128,
                    "similarity": "cosine"
                },
                "task_settings": {
                   "model": "text-embedding-ada-002"
                }
            }
            """;

        updateClusterSettings(Settings.builder().put("xpack.inference.skip_validate_and_start", true).build());

        // We would expect an error about the invalid API key if the validation occurred
        putModel("unvalidated", openAiConfigWithBadApiKey, TEXT_EMBEDDING);
    }

    public void testDeleteEndpointWhileReferencedByPipeline() throws IOException {
        String endpointId = "endpoint_referenced_by_pipeline";
        putModel(endpointId, mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
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
        final Function<String, String> buildErrorString = endpointName -> "Inference endpoint "
            + endpointName
            + " is being used in the mapping for indexes: "
            + Set.of(indexName)
            + ". Ensure that no index mappings are using this inference endpoint, or use force to ignore this warning and delete the"
            + " inference endpoint.";

        putModel(endpointId, mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
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

        putModel(searchEndpointId, mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
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
        putModel(endpointId, mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
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

    public void testCreateEndpoint_withInferenceIdReferencedBySemanticText() throws IOException {
        final String endpointId = "endpoint_referenced_by_semantic_text";
        final String otherEndpointId = "other_endpoint_referenced_by_semantic_text";
        final String indexName1 = randomAlphaOfLength(10).toLowerCase();
        final String indexName2 = randomValueOtherThan(indexName1, () -> randomAlphaOfLength(10).toLowerCase());

        putModel(endpointId, mockTextEmbeddingServiceModelConfig(128), TEXT_EMBEDDING);
        putModel(otherEndpointId, mockTextEmbeddingServiceModelConfig(), TEXT_EMBEDDING);
        // Create two indices, one where the inference ID of the endpoint we'll be deleting and
        // recreating is used for inference_id and one where it's used for search_inference_id
        putSemanticText(endpointId, otherEndpointId, indexName1);
        putSemanticText(otherEndpointId, endpointId, indexName2);

        // Confirm that we can create the endpoint with different settings if there
        // are documents in the indices which do not use the semantic text field
        var request = new Request("PUT", indexName1 + "/_create/1");
        request.setJsonEntity("{\"non_inference_field\": \"value\"}");
        assertStatusOkOrCreated(client().performRequest(request));

        request = new Request("PUT", indexName2 + "/_create/1");
        request.setJsonEntity("{\"non_inference_field\": \"value\"}");
        assertStatusOkOrCreated(client().performRequest(request));

        assertStatusOkOrCreated(client().performRequest(new Request("GET", "_refresh")));

        deleteModel(endpointId, "force=true");
        putModel(endpointId, mockTextEmbeddingServiceModelConfig(64), TEXT_EMBEDDING);

        // Index a document with the semantic text field into each index
        request = new Request("PUT", indexName1 + "/_create/2");
        request.setJsonEntity("{\"inference_field\": \"value\"}");
        assertStatusOkOrCreated(client().performRequest(request));

        request = new Request("PUT", indexName2 + "/_create/2");
        request.setJsonEntity("{\"inference_field\": \"value\"}");
        assertStatusOkOrCreated(client().performRequest(request));

        assertStatusOkOrCreated(client().performRequest(new Request("GET", "_refresh")));

        deleteModel(endpointId, "force=true");

        // Try to create an inference endpoint with the same ID but different dimensions
        // from when the document with the semantic text field was indexed
        ResponseException responseException = assertThrows(
            ResponseException.class,
            () -> putModel(endpointId, mockTextEmbeddingServiceModelConfig(128), TEXT_EMBEDDING)
        );
        assertThat(
            responseException.getMessage(),
            containsString(
                "Inference endpoint ["
                    + endpointId
                    + "] could not be created because the inference_id is being used in mappings with incompatible settings for indices: ["
            )
        );
        assertThat(responseException.getMessage(), containsString(indexName1));
        assertThat(responseException.getMessage(), containsString(indexName2));
        assertThat(
            responseException.getMessage(),
            containsString("Please either use a different inference_id or update the index mappings to refer to a different inference_id.")
        );

        deleteIndex(indexName1);
        deleteIndex(indexName2);

        deleteModel(otherEndpointId, "force=true");
    }

    public void testUnsupportedStream() throws Exception {
        String modelId = "streaming";
        putModel(modelId, mockCompletionServiceModelConfig(SPARSE_EMBEDDING, "streaming_completion_test_service"));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("inference_id"));
        assertEquals(SPARSE_EMBEDDING.toString(), singleModel.get("task_type"));

        try {
            var events = streamInferOnMockService(modelId, SPARSE_EMBEDDING, List.of(randomUUID()), null);
            assertThat(events.size(), equalTo(1));
            events.forEach(event -> {
                assertThat(event.type(), equalToIgnoringCase("error"));
                assertThat(
                    event.data(),
                    containsString("Streaming is not allowed for service [streaming_completion_test_service] and task [sparse_embedding]")
                );
            });
        } finally {
            deleteModel(modelId);
        }
    }

    public void testSupportedStream() throws Exception {
        testSupportedStream("streaming_completion_test_service");
    }

    public void testSupportedStreamForAlias() throws Exception {
        testSupportedStream("streaming_completion_test_service_alias");
    }

    private void testSupportedStream(String serviceName) throws Exception {
        String modelId = "streaming";
        putModel(modelId, mockCompletionServiceModelConfig(TaskType.COMPLETION, serviceName));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("inference_id"));
        assertEquals(TaskType.COMPLETION.toString(), singleModel.get("task_type"));

        var input = IntStream.range(1, 2 + randomInt(8)).mapToObj(i -> randomAlphanumericOfLength(5)).toList();
        try {
            var events = streamInferOnMockService(modelId, TaskType.COMPLETION, input, VALIDATE_ELASTIC_PRODUCT_HEADER_CONSUMER);

            var expectedResponses = Stream.concat(
                input.stream().map(s -> s.toUpperCase(Locale.ROOT)).map(str -> "{\"completion\":[{\"delta\":\"" + str + "\"}]}"),
                Stream.of("[DONE]")
            ).iterator();
            assertThat(events.size(), equalTo(input.size() + 1));
            events.forEach(event -> {
                assertThat(event.type(), equalToIgnoringCase("message"));
                assertThat(event.data(), equalTo(expectedResponses.next()));
            });
        } finally {
            deleteModel(modelId);
        }
    }

    public void testUnifiedCompletionInference() throws Exception {
        String modelId = "streaming";
        putModel(modelId, mockCompletionServiceModelConfig(TaskType.CHAT_COMPLETION, "streaming_completion_test_service"));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("inference_id"));
        assertEquals(TaskType.CHAT_COMPLETION.toString(), singleModel.get("task_type"));

        var input = IntStream.range(1, 2 + randomInt(8)).mapToObj(i -> randomAlphanumericOfLength(5)).toList();
        try {
            var events = unifiedCompletionInferOnMockService(
                modelId,
                TaskType.CHAT_COMPLETION,
                input,
                VALIDATE_ELASTIC_PRODUCT_HEADER_CONSUMER
            );
            var expectedResponses = expectedResultsIterator(input);
            assertThat(events.size(), equalTo(input.size() + 1));
            events.forEach(event -> {
                assertThat(event.type(), equalToIgnoringCase("message"));
                assertThat(event.data(), equalTo(expectedResponses.next()));
            });
        } finally {
            deleteModel(modelId);
        }
    }

    public void testEmbeddingInference() throws IOException {
        String modelId = "embedding_model";
        int dimensions = 128;
        putModel(modelId, mockEmbeddingServiceModelConfig(dimensions));
        var singleModel = getModel(modelId);
        assertThat(singleModel.get("inference_id"), is(modelId));
        assertThat(singleModel.get("task_type"), is(EMBEDDING.toString()));
        try {
            var input = List.of(
                new InferenceString(DataType.IMAGE, randomAlphaOfLength(5)),
                new InferenceString(DataType.TEXT, randomAlphaOfLength(15))
            );
            var resultMap = embedding(modelId, input);
            assertThat(resultMap.values(), hasSize(1));
            @SuppressWarnings("unchecked")
            var embeddings = (List<Map<String, List<Float>>>) resultMap.get(GenericDenseEmbeddingFloatResults.EMBEDDINGS);
            assertThat(embeddings, hasSize(input.size()));
            for (var embedding : embeddings) {
                assertThat(embedding.get(EmbeddingResults.EMBEDDING), hasSize(dimensions));
            }
        } finally {
            deleteModel(modelId);
        }
    }

    public void testUpdateEndpointWithWrongTaskTypeInURL() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
        var e = expectThrows(
            ResponseException.class,
            () -> updateEndpoint(
                "sparse_embedding_model",
                updateConfig(null, randomAlphaOfLength(10), randomIntBetween(1, 10)),
                TEXT_EMBEDDING
            )
        );
        assertThat(e.getMessage(), containsString("Task type must match the task type of the existing endpoint"));
    }

    public void testUpdateEndpointWithWrongTaskTypeInBody() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), SPARSE_EMBEDDING);
        var e = expectThrows(
            ResponseException.class,
            () -> updateEndpoint("sparse_embedding_model", updateConfig(TEXT_EMBEDDING, randomAlphaOfLength(10), randomIntBetween(1, 10)))
        );
        assertThat(e.getMessage(), containsString("Task type must match the task type of the existing endpoint"));
    }

    public void testUpdateEndpointWithTaskTypeInURL() throws IOException {
        testUpdateEndpoint(false, true);
    }

    public void testUpdateEndpointWithTaskTypeInBody() throws IOException {
        testUpdateEndpoint(true, false);
    }

    public void testUpdateEndpointWithTaskTypeInBodyAndURL() throws IOException {
        testUpdateEndpoint(true, true);
    }

    @SuppressWarnings("unchecked")
    private void testUpdateEndpoint(boolean taskTypeInBody, boolean taskTypeInURL) throws IOException {
        String endpointId = "sparse_embedding_model";
        putModel(endpointId, mockSparseServiceModelConfig(), SPARSE_EMBEDDING);

        int temperature = randomIntBetween(1, 10);
        var expectedConfig = updateConfig(taskTypeInBody ? SPARSE_EMBEDDING : null, randomAlphaOfLength(1), temperature);
        Map<String, Object> updatedEndpoint;
        if (taskTypeInURL) {
            updatedEndpoint = updateEndpoint(endpointId, expectedConfig, SPARSE_EMBEDDING);
        } else {
            updatedEndpoint = updateEndpoint(endpointId, expectedConfig);
        }

        Map<String, Objects> updatedTaskSettings = (Map<String, Objects>) updatedEndpoint.get("task_settings");
        assertEquals(temperature, updatedTaskSettings.get("temperature"));
    }

    private static Iterator<String> expectedResultsIterator(List<String> input) {
        // The Locale needs to be ROOT to match what the test service is going to respond with
        return Stream.concat(input.stream().map(s -> s.toUpperCase(Locale.ROOT)).map(InferenceCrudIT::expectedResult), Stream.of("[DONE]"))
            .iterator();
    }

    private static String expectedResult(String input) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.field("id", "id");
            builder.startArray("choices");
            builder.startObject();
            builder.startObject("delta");
            builder.field("content", input);
            builder.endObject();
            builder.field("index", 0);
            builder.endObject();
            builder.endArray();
            builder.field("model", "gpt-4o-2024-08-06");
            builder.field("object", "chat.completion.chunk");
            builder.endObject();

            return Strings.toString(builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void testGetZeroModels() throws IOException {
        var models = getModels("_all", TaskType.COMPLETION);
        assertThat(models, empty());
    }
}
