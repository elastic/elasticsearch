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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class GroqServiceTests extends ESTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testUnifiedHandlerUsesOpenAiUnifiedChatCompletionHandler() {
        var handler = GroqService.UNIFIED_CHAT_COMPLETION_HANDLER;
        assertThat(handler, instanceOf(OpenAiUnifiedChatCompletionResponseHandler.class));
    }

    public void testParseRequestConfigCreatesModel() throws Exception {
        try (GroqService service = createService()) {
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
        try (GroqService service = createService()) {
            Map<String, Object> serviceSettings = new HashMap<>();
            serviceSettings.put(DefaultSecretSettings.API_KEY, "secret-key");
            PlainActionFuture<Model> future = new PlainActionFuture<>();

            service.parseRequestConfig("groq-test", TaskType.CHAT_COMPLETION, wrapRequestConfig(serviceSettings, Map.of()), future);

            expectThrows(ValidationException.class, future::actionGet);
        }
    }

    public void testParseRequestConfigRejectsUnsupportedTaskType() throws Exception {
        try (GroqService service = createService()) {
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
        try (GroqService service = createService()) {
            Map<String, Object> config = new HashMap<>();
            config.put(ModelConfigurations.SERVICE_SETTINGS, new HashMap<>(Map.of(ServiceFields.MODEL_ID, "persisted-model")));

            Map<String, Object> secrets = new HashMap<>();
            secrets.put(ModelSecrets.SECRET_SETTINGS, new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, "persisted-secret")));

            GroqChatCompletionModel model = service.parsePersistedConfigWithSecrets("groq-test", TaskType.CHAT_COMPLETION, config, secrets);
            assertTrue(model.getSecretSettings().apiKey().equals("persisted-secret"));
            assertThat(model.getServiceSettings().modelId(), equalTo("persisted-model"));
        }
    }

    public void testDoInferRejectsNonGroqModel() throws Exception {
        try (GroqService service = createService()) {
            PlainActionFuture<InferenceServiceResults> future = new PlainActionFuture<>();
            service.doInfer(createNonGroqModel(), null, Map.of(), TimeValue.ZERO, future);

            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, future::actionGet);
            assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    public void testDoChunkedInferAlwaysFails() throws Exception {
        try (GroqService service = createService()) {
            PlainActionFuture<List<ChunkedInference>> future = new PlainActionFuture<>();
            List<ChunkInferenceInput> inputs = List.of();
            service.doChunkedInfer(null, inputs, Map.of(), null, TimeValue.ZERO, future);

            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, future::actionGet);
            assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(exception.getMessage(), containsString("Groq does not support chunked inference"));
        }
    }

    private GroqService createService() {
        return new GroqService(
            mock(HttpRequestSender.Factory.class),
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mock(ClusterService.class)
        );
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

    private Model createNonGroqModel() {
        var serviceSettings = new GroqChatCompletionServiceSettings("other-model", (URI) null, null, null);
        var configurations = new ModelConfigurations("non-groq", TaskType.CHAT_COMPLETION, "other-service", serviceSettings);
        return new Model(configurations);
    }
}
