/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModelTests;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModelTests;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class GoogleAiStudioServiceTests extends ESTestCase {

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

    public void testParseRequestConfig_CreatesAGoogleAiStudioCompletionModel() throws IOException {
        var apiKey = "apiKey";
        var modelId = "model";

        try (var service = createGoogleAiStudioService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

                var completionModel = (GoogleAiStudioCompletionModel) model;
                assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
                assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(apiKey)
                ),
                Set.of(),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAGoogleAiStudioEmbeddingsModel() throws IOException {
        var apiKey = "apiKey";
        var modelId = "model";

        try (var service = createGoogleAiStudioService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

                var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(apiKey));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(apiKey)
                ),
                Set.of(),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [googleaistudio] service does not support task type [sparse_embedding]"
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
        try (var service = createGoogleAiStudioService()) {
            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model")),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> serviceSettings = new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model"));
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMapEmpty(), getSecretSettingsMap("api_key"));

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> taskSettingsMap = new HashMap<>();
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model")),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, "model")),
                getTaskSettingsMapEmpty(),
                secretSettings
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleAiStudioCompletionModel() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleAiStudioEmbeddingsModel() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

            var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(apiKey)
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createGoogleAiStudioService()) {
            var secretSettingsMap = getSecretSettingsMap(apiKey);
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
                getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId));
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMapEmpty(), getSecretSettingsMap(apiKey));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> taskSettings = getTaskSettingsMapEmpty();
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

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfig_CreatesAGoogleAiStudioCompletionModel() throws IOException {
        var modelId = "model";

        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)), getTaskSettingsMapEmpty());

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var modelId = "model";

        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)), getTaskSettingsMapEmpty());
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var modelId = "model";

        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId));
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMapEmpty());

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var modelId = "model";

        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> taskSettings = getTaskSettingsMapEmpty();
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)), taskSettings);

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(modelId));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotGoogleAiStudioModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new GoogleAiStudioService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                List.of(""),
                false,
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

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            String responseJson = """
                {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": "result"
                                    }
                                ],
                                "role": "model"
                            },
                            "finishReason": "STOP",
                            "index": 0,
                            "safetyRatings": [
                                {
                                    "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_HATE_SPEECH",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_HARASSMENT",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                                    "probability": "NEGLIGIBLE"
                                }
                            ]
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 4,
                        "candidatesTokenCount": 215,
                        "totalTokenCount": 219
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = GoogleAiStudioCompletionModelTests.createModel("model", getUrl(webServer), "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("input"),
                false,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletions(List.of("result"))));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getQuery(), is("key=secret"));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "contents",
                        List.of(Map.of("role", "user", "parts", List.of(Map.of("text", "input")))),
                        "generationConfig",
                        Map.of("candidateCount", 1)
                    )
                )
            );
        }
    }

    public void testInfer_SendsEmbeddingsRequest() throws IOException {
        var modelId = "model";
        var apiKey = "apiKey";
        var input = "input";

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            String responseJson = """
                {
                     "embeddings": [
                         {
                             "values": [
                                 0.0123,
                                 -0.0123
                             ]
                         }
                     ]
                 }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = GoogleAiStudioEmbeddingsModelTests.createModel(modelId, apiKey, getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of(input),
                false,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getQuery(), endsWith(apiKey));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap, aMapWithSize(1));
            assertThat(
                requestMap.get("requests"),
                Matchers.is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", modelId),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input)))
                        )
                    )
                )
            );
        }
    }

    public void testChunkedInfer_Batches() throws IOException {
        var modelId = "modelId";
        var apiKey = "apiKey";
        var input = List.of("foo", "bar");

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            String responseJson = """
                {
                     "embeddings": [
                         {
                             "values": [
                                 0.0123,
                                 -0.0123
                             ]
                         },
                         {
                             "values": [
                                 0.0456,
                                 -0.0456
                             ]
                         }
                     ]
                 }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = GoogleAiStudioEmbeddingsModelTests.createModel(modelId, apiKey, getUrl(webServer));
            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                input,
                new HashMap<>(),
                InputType.INGEST,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));

            // first result
            {
                assertThat(results.get(0), instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(input.get(0), floatResult.chunks().get(0).matchedText());
                assertTrue(Arrays.equals(new float[] { 0.0123f, -0.0123f }, floatResult.chunks().get(0).embedding()));
            }

            // second result
            {
                assertThat(results.get(1), instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(input.get(1), floatResult.chunks().get(0).matchedText());
                assertTrue(Arrays.equals(new float[] { 0.0456f, -0.0456f }, floatResult.chunks().get(0).embedding()));
            }

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getQuery(), endsWith(apiKey));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap, aMapWithSize(1));
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", modelId),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input.get(0))))
                        ),
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", modelId),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input.get(1))))
                        )
                    )
                )
            );
        }
    }

    public void testInfer_ResourceNotFound() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "error": {
                        "message": "error"
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

            var model = GoogleAiStudioCompletionModelTests.createModel("model", getUrl(webServer), "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(error.getMessage(), containsString("Resource not found at "));
            assertThat(error.getMessage(), containsString("Error message: [error]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testCheckModelConfig_UpdatesDimensions() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var similarityMeasure = SimilarityMeasure.DOT_PRODUCT;
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            String responseJson = """
                {
                     "embeddings": [
                         {
                             "values": [
                                 0.0123,
                                 -0.0123
                             ]
                         }
                     ]
                 }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = GoogleAiStudioEmbeddingsModelTests.createModel(getUrl(webServer), modelId, apiKey, 1, similarityMeasure);

            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            // Updates dimensions to two as two embeddings were returned instead of one as specified before
            assertThat(
                result,
                is(GoogleAiStudioEmbeddingsModelTests.createModel(getUrl(webServer), modelId, apiKey, 2, similarityMeasure))
            );
        }
    }

    public void testCheckModelConfig_UpdatesSimilarityToDotProduct_WhenItIsNull() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var oneDimension = 1;
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            String responseJson = """
                {
                     "embeddings": [
                         {
                             "values": [
                                 0.0123
                             ]
                         }
                     ]
                 }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = GoogleAiStudioEmbeddingsModelTests.createModel(getUrl(webServer), modelId, apiKey, oneDimension, null);

            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result,
                is(
                    GoogleAiStudioEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        modelId,
                        apiKey,
                        oneDimension,
                        SimilarityMeasure.DOT_PRODUCT
                    )
                )
            );
        }
    }

    public void testCheckModelConfig_DoesNotUpdateSimilarity_WhenItIsSpecifiedAsCosine() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var oneDimension = 1;
        var modelId = "model";
        var apiKey = "apiKey";

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            String responseJson = """
                {
                     "embeddings": [
                         {
                             "values": [
                                 0.0123
                             ]
                         }
                     ]
                 }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = GoogleAiStudioEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelId,
                apiKey,
                oneDimension,
                SimilarityMeasure.COSINE
            );

            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result,
                is(
                    GoogleAiStudioEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        modelId,
                        apiKey,
                        oneDimension,
                        SimilarityMeasure.COSINE
                    )
                )
            );
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = GoogleAiStudioCompletionModelTests.createModel(randomAlphaOfLength(10), randomAlphaOfLength(10));
            assertThrows(
                ElasticsearchStatusException.class,
                () -> { service.updateModelWithEmbeddingDetails(model, randomNonNegativeInt()); }
            );
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(null);
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(randomFrom(SimilarityMeasure.values()));
    }

    private void testUpdateModelWithEmbeddingDetails_Successful(SimilarityMeasure similarityMeasure) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            var embeddingSize = randomNonNegativeInt();
            var model = GoogleAiStudioEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomNonNegativeInt(),
                similarityMeasure
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null ? SimilarityMeasure.DOT_PRODUCT : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public static Map<String, Object> buildExpectationCompletions(List<String> completions) {
        return Map.of(
            ChatCompletionResults.COMPLETION,
            completions.stream().map(completion -> Map.of(ChatCompletionResults.Result.RESULT, completion)).collect(Collectors.toList())
        );
    }

    private static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.<Model>wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            assertThat(e.getMessage(), is(expectedMessage));
        });
    }

    private Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(
            Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)
        );
    }

    private GoogleAiStudioService createGoogleAiStudioService() {
        return new GoogleAiStudioService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }
}
