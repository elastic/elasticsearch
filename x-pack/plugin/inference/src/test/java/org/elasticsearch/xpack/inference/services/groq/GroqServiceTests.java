/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.groq.action.GroqActionCreatorTests.createModel;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class GroqServiceTests extends InferenceServiceTestCase {

    private static final String INFERENCE_ENTITY_ID_VALUE = "id";

    public void testUnifiedHandlerUsesOpenAiUnifiedChatCompletionHandler() {
        var handler = GroqService.UNIFIED_CHAT_COMPLETION_HANDLER;
        assertThat(handler, instanceOf(OpenAiUnifiedChatCompletionResponseHandler.class));
    }

    public void testParseRequestConfigCreatesModel() throws Exception {
        try (var service = createInferenceService()) {
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            service.parseRequestConfig(
                "groq-test",
                TaskType.CHAT_COMPLETION,
                createRequestConfig("llama-3.3-70b-versatile", "secret-key", "user-1", null),
                future
            );

            GroqChatCompletionModel model = (GroqChatCompletionModel) future.actionGet();
            assertThat(model.getServiceSettings().modelId(), equalTo("llama-3.3-70b-versatile"));
            assertTrue(model.getSecretSettings().apiKey().equals("secret-key"));
            assertThat(model.getTaskSettings().user(), equalTo("user-1"));
        }
    }

    public void testParseRequestConfigRequiresModelId() throws Exception {
        try (var service = createInferenceService()) {
            Map<String, Object> serviceSettings = new HashMap<>();
            serviceSettings.put(DefaultSecretSettings.API_KEY, "secret-key");
            PlainActionFuture<Model> future = new PlainActionFuture<>();

            service.parseRequestConfig("groq-test", TaskType.CHAT_COMPLETION, wrapRequestConfig(serviceSettings, Map.of()), future);

            expectThrows(ValidationException.class, future::actionGet);
        }
    }

    public void testParseRequestConfigRejectsUnsupportedTaskType() throws Exception {
        try (var service = createInferenceService()) {
            PlainActionFuture<Model> future = new PlainActionFuture<>();
            service.parseRequestConfig(
                "groq-test",
                TaskType.TEXT_EMBEDDING,
                createRequestConfig("llama-3.1", "secret-key", null, null),
                future
            );

            expectThrows(ElasticsearchStatusException.class, future::actionGet);
        }
    }

    public void testParsePersistedConfigWithSecretsUsesSecretSettings() throws Exception {
        try (var service = createInferenceService()) {
            Map<String, Object> config = new HashMap<>();
            config.put(ModelConfigurations.SERVICE_SETTINGS, new HashMap<>(Map.of(ServiceFields.MODEL_ID, "persisted-model")));

            Map<String, Object> secrets = new HashMap<>();
            secrets.put(ModelSecrets.SECRET_SETTINGS, new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, "persisted-secret")));

            GroqChatCompletionModel model = (GroqChatCompletionModel) service.parsePersistedConfig(
                new UnparsedModel("groq-test", TaskType.CHAT_COMPLETION, GroqService.NAME, config, secrets)
            );
            assertTrue(model.getSecretSettings().apiKey().equals("persisted-secret"));
            assertThat(model.getServiceSettings().modelId(), equalTo("persisted-model"));
        }
    }

    public void testChunkedInferNotSupported() throws Exception {
        try (var service = createInferenceService()) {
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            List<ChunkInferenceInput> inputs = List.of();
            service.chunkedInfer(createModel("some_url"), null, inputs, Map.of(), null, null, listener);

            var exception = expectThrows(UnsupportedOperationException.class, listener::actionGet);
            assertThat(exception.getMessage(), containsString("groq service does not support chunked inference"));
        }
    }

    private Map<String, Object> createRequestConfig(String modelId, String apiKey, String user, String org) {
        Map<String, Object> serviceSettings = new HashMap<>();
        serviceSettings.put(ServiceFields.MODEL_ID, modelId);
        serviceSettings.put(DefaultSecretSettings.API_KEY, apiKey);
        if (org != null) {
            serviceSettings.put(OpenAiServiceFields.ORGANIZATION, org);
        }
        return wrapRequestConfig(serviceSettings, user == null ? Map.of() : Map.of(OpenAiServiceFields.USER, user));
    }

    private Map<String, Object> wrapRequestConfig(Map<String, Object> serviceSettings, Map<String, Object> taskSettings) {
        Map<String, Object> config = new HashMap<>();
        config.put(ModelConfigurations.SERVICE_SETTINGS, new HashMap<>(serviceSettings));
        config.put(ModelConfigurations.TASK_SETTINGS, new HashMap<>(taskSettings));
        return config;
    }

    public void testBuildModelFromConfigAndSecrets_ChatCompletion() throws IOException {
        var model = createModel("some_url");
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            INFERENCE_ENTITY_ID_VALUE,
            TaskType.SPARSE_EMBEDDING,
            GroqService.NAME,
            mock(ServiceSettings.class)
        );
        try (var inferenceService = createInferenceService()) {
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> inferenceService.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                thrownException.getMessage(),
                is(Strings.format("The [%s] service does not support task type [%s]", GroqService.NAME, TaskType.SPARSE_EMBEDDING))
            );
        }
    }

    private void validateModelBuilding(Model model) throws IOException {
        try (var inferenceService = createInferenceService()) {
            var resultModel = inferenceService.buildModelFromConfigAndSecrets(model.getConfigurations(), model.getSecrets());
            assertThat(resultModel, is(model));
        }
    }

    @Override
    public InferenceService createInferenceService() {
        return new GroqService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mock(ClusterService.class)
        );
    }

    @Override
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.of(TaskType.CHAT_COMPLETION);
    }
}
