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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;

public class InferenceCrudIT extends InferenceBaseRestTest {

    @SuppressWarnings("unchecked")
    public void testGet() throws IOException {
        for (int i = 0; i < 5; i++) {
            putModel("se_model_" + i, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            putModel("te_model_" + i, mockSparseServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        }

        var getAllModels = getAllModels();
        assertThat(getAllModels, hasSize(9));

        var getSparseModels = getModels("_all", TaskType.SPARSE_EMBEDDING);
        assertThat(getSparseModels, hasSize(5));
        for (var sparseModel : getSparseModels) {
            assertEquals("sparse_embedding", sparseModel.get("task_type"));
        }

        var getDenseModels = getModels("_all", TaskType.TEXT_EMBEDDING);
        assertThat(getDenseModels, hasSize(4));
        for (var denseModel : getDenseModels) {
            assertEquals("text_embedding", denseModel.get("task_type"));
        }

        var singleModel = getModels("se_model_1", TaskType.SPARSE_EMBEDDING);
        assertThat(singleModel, hasSize(1));
        assertEquals("se_model_1", singleModel.get(0).get("inference_id"));

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

        var inference = inferOnMockService(modelId, List.of(randomAlphaOfLength(10)));
        assertNonEmptyInferenceResults(inference, 1, TaskType.SPARSE_EMBEDDING);
        deleteModel(modelId);
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
        String endpointId = "endpoint_referenced_by_semantic_text";
        putModel(endpointId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        String indexName = randomAlphaOfLength(10).toLowerCase();
        putSemanticText(endpointId, indexName);
        {

            var errorString = new StringBuilder().append(" Inference endpoint ")
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
        }
        {
            var response = deleteModel(endpointId, "force=true");
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
            var events = streamInferOnMockService(modelId, TaskType.SPARSE_EMBEDDING, List.of(randomAlphaOfLength(10)));
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

        var input = IntStream.range(1, randomInt(10)).mapToObj(i -> randomAlphaOfLength(10)).toList();

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
}
