/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceMinimalSettings;
import org.junit.Before;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ModelRegistryEisIT extends ModelRegistryEisBaseIT {

    @Before
    public void setupTest() {
        initializeModels();
    }

    private static final String eisAuthorizedResponse = """
        {
            "models": [
                {
                    "model_name": "rainbow-sprinkles",
                    "task_types": ["chat"]
                },
                {
                    "model_name": "elser_model_2",
                    "task_types": ["embed/text/sparse"]
                },
                {
                    "model_name": "multilingual-embed-v1",
                    "task_types": ["embed/text/dense"]
                },
                {
                    "model_name": "rerank-v1",
                    "task_types": ["rerank/text/text-similarity"]
                }
            ]
        }
        """;

    private static final String eisUnauthorizedResponse = """
        {
            "models": [
            ]
        }
        """;

    public void testGetModelsByTaskType() {
        {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisAuthorizedResponse));
            PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
            modelRegistry.getModelsByTaskType(TaskType.COMPLETION, listener);

            assertThat(listener.actionGet(TIMEOUT), is(List.of()));
        }
        {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisAuthorizedResponse));
            PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
            modelRegistry.getModelsByTaskType(TaskType.SPARSE_EMBEDDING, listener);

            var results = listener.actionGet(TIMEOUT);
            var expected = Stream.of("sparse-1", "sparse-2", "sparse-3", ".elser-2-elastic").toArray(String[]::new);
            assertThat(results.size(), is(expected.length));
            assertThat(results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(), containsInAnyOrder(expected));
        }
        {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisAuthorizedResponse));
            PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
            modelRegistry.getModelsByTaskType(TaskType.TEXT_EMBEDDING, listener);

            var results = listener.actionGet(TIMEOUT);
            var expected = Stream.of("embedding-1", "embedding-2", ".multilingual-embed-v1-elastic").toArray(String[]::new);
            assertThat(results.size(), is(expected.length));
            assertThat(results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(), containsInAnyOrder(expected));
        }
        {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisAuthorizedResponse));
            PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
            modelRegistry.getModelsByTaskType(TaskType.CHAT_COMPLETION, listener);

            var results = listener.actionGet(TIMEOUT);
            assertThat(results.size(), is(1));
            assertThat(
                results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(),
                containsInAnyOrder(".rainbow-sprinkles-elastic")
            );
        }
        {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisAuthorizedResponse));
            PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
            modelRegistry.getModelsByTaskType(TaskType.RERANK, listener);

            var results = listener.actionGet(TIMEOUT);
            assertThat(results.size(), is(1));
            assertThat(results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(), containsInAnyOrder(".rerank-v1-elastic"));
        }
    }

    public void testGetAllModels() {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisAuthorizedResponse));
        PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
        modelRegistry.getAllModels(false, listener);

        var results = listener.actionGet(TIMEOUT);
        var expected = Stream.of(
            "sparse-1",
            "sparse-2",
            "sparse-3",
            "embedding-1",
            "embedding-2",
            ".elser-2-elastic",
            ".multilingual-embed-v1-elastic",
            ".rainbow-sprinkles-elastic",
            ".rerank-v1-elastic"
        ).toArray(String[]::new);
        assertThat(results.size(), is(expected.length));
        assertThat(results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(), containsInAnyOrder(expected));
    }

    public void testGetAllModelsNoEisResults() {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisUnauthorizedResponse));
        PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
        modelRegistry.getAllModels(false, listener);

        var results = listener.actionGet(TIMEOUT);
        var expected = Stream.of("sparse-1", "sparse-2", "sparse-3", "embedding-1", "embedding-2").toArray(String[]::new);
        assertThat(results.size(), is(expected.length));
        assertThat(results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(), containsInAnyOrder(expected));
    }

    public void testGetModel_WhenNotAuthorizedForEis() {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(eisUnauthorizedResponse));
        PlainActionFuture<UnparsedModel> listener = new PlainActionFuture<>();
        modelRegistry.getModel(ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_ENDPOINT_ID_V1, listener);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), containsString("Unable to retrieve the preconfigured inference endpoint"));
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "No Elastic Inference Service preconfigured endpoint found for inference ID [.rerank-v1-elastic]. "
                    + "Either it does not exist, or you are not authorized to access it."
            )
        );
    }

    public void testGetAllModelsEisReturnsFailureStatusCode() {
        webServer.enqueue(new MockResponse().setResponseCode(500).setBody("{}"));
        PlainActionFuture<List<UnparsedModel>> listener = new PlainActionFuture<>();
        modelRegistry.getAllModels(false, listener);

        var results = listener.actionGet(TIMEOUT);
        var expected = Stream.of("sparse-1", "sparse-2", "sparse-3", "embedding-1", "embedding-2").toArray(String[]::new);
        assertThat(results.size(), is(expected.length));
        assertThat(results.stream().map(UnparsedModel::inferenceEntityId).sorted().toList(), containsInAnyOrder(expected));
    }
}
