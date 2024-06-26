/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.anthropic.AnthropicRequestUtils;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettingsTests;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.buildExpectationCompletions;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getModelListenerForException;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AnthropicServiceTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;

    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testParseRequestConfig_CreatesACompletionModel() throws IOException {
        var apiKey = "apiKey";
        var modelId = "model";

        try (var service = createServiceWithMockSender()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

                var completionModel = (AnthropicChatCompletionModel) model;
                assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
                assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                    new HashMap<>(Map.of(AnthropicServiceFields.MAX_TOKENS, 1)),
                    getSecretSettingsMap(apiKey)
                ),
                Set.of(),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [anthropic] service does not support task type [sparse_embedding]"
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model")),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap("secret")
                ),
                Set.of(),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model")),
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, null, null, null),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [anthropic] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            Map<String, Object> serviceSettings = new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model"));
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                serviceSettings,
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, null, null, null),
                getSecretSettingsMap("api_key")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [anthropic] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var taskSettingsMap = AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, null, null, null);
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model")),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [anthropic] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            Map<String, Object> secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model")),
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, null, null, null),
                secretSettings
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [anthropic] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesACompletionModel() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createServiceWithMockSender()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createServiceWithMockSender()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3),
                getSecretSettingsMap(apiKey)
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertThat(completionModel.getSecretSettings().apiKey(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createServiceWithMockSender()) {
            var secretSettingsMap = getSecretSettingsMap(apiKey);
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createServiceWithMockSender()) {
            Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId));
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createServiceWithMockSender()) {
            Map<String, Object> taskSettings = AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3);
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                taskSettings,
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfig_CreatesACompletionModel() throws IOException {
        var modelId = "model";

        try (var service = createServiceWithMockSender()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3)
            );

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var modelId = "model";

        try (var service = createServiceWithMockSender()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3)
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var modelId = "model";

        try (var service = createServiceWithMockSender()) {
            Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId));
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3)
            );

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var modelId = "model";

        try (var service = createServiceWithMockSender()) {
            Map<String, Object> taskSettings = AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 1.0, 2.1, 3);
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)), taskSettings);

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(AnthropicChatCompletionModel.class));

            var completionModel = (AnthropicChatCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(new AnthropicChatCompletionTaskSettings(1, 1.0, 2.1, 3)));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotAValidModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new AnthropicService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                List.of(""),
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            MatcherAssert.assertThat(
                thrownException.getMessage(),
                is("The internal model was invalid, please delete the service [service_name] with id [model_id] and add it again.")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_SendsCompletionRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AnthropicService(senderFactory, createWithEmptySettings(threadPool))) {
            String responseJson = """
                {
                    "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-3-opus-20240229",
                    "content": [
                        {
                            "type": "text",
                            "text": "result"
                        }
                    ],
                    "stop_reason": "end_turn",
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": 16,
                        "output_tokens": 326
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = AnthropicChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "secret", "model", 1);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("input"),
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletions(List.of("result"))));
            var request = webServer.requests().get(0);
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));
            assertThat(request.getHeader(AnthropicRequestUtils.X_API_KEY), Matchers.equalTo("secret"));
            assertThat(
                request.getHeader(AnthropicRequestUtils.VERSION),
                Matchers.equalTo(AnthropicRequestUtils.ANTHROPIC_VERSION_2023_06_01)
            );

            var requestMap = entityAsMap(request.getBody());
            assertThat(
                requestMap,
                is(Map.of("messages", List.of(Map.of("role", "user", "content", "input")), "model", "model", "max_tokens", 1))
            );
        }
    }

    private AnthropicService createServiceWithMockSender() {
        return new AnthropicService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }
}
