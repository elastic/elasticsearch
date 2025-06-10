/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModelTests;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getModelListenerForException;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceTests extends ESSingleNodeTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();

    private ModelRegistry modelRegistry;
    private ThreadPool threadPool;

    private HttpClientManager clientManager;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Before
    public void init() throws Exception {
        webServer.start();
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testParseRequestConfig_CreatesASparseEmbeddingsModel() throws IOException {
        try (var service = createServiceWithMockSender()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));

                var completionModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
                assertThat(completionModel.getServiceSettings().modelId(), is(ElserModels.ELSER_V2_MODEL));

            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), Map.of(), Map.of()),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var config = getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), Map.of(), Map.of());
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            Map<String, Object> serviceSettings = new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL));
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, Map.of(), Map.of());

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var taskSettings = Map.of("extra_key", (Object) "value");

            var config = getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), taskSettings, Map.of());

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var secretSettings = Map.of("extra_key", (Object) "value");

            var config = getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), Map.of(), secretSettings);

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesASparseEmbeddingModel() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL)),
                Map.of(),
                Map.of()
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.SPARSE_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));

            var sparseEmbeddingsModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
            assertThat(sparseEmbeddingsModel.getServiceSettings().modelId(), is(ElserModels.ELSER_V2_MODEL));
            assertThat(sparseEmbeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(sparseEmbeddingsModel.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL)),
                Map.of(),
                Map.of()
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.SPARSE_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));

            var completionModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(ElserModels.ELSER_V2_MODEL));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createServiceWithMockSender()) {
            Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL));
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, Map.of(), Map.of());

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.SPARSE_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));

            var completionModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(ElserModels.ELSER_V2_MODEL));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var taskSettings = Map.of("extra_key", (Object) "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL)),
                taskSettings,
                Map.of()
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.SPARSE_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));

            var completionModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(ElserModels.ELSER_V2_MODEL));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var secretSettingsMap = Map.of("extra_key", (Object) "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL)),
                Map.of(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.SPARSE_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));

            var completionModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(ElserModels.ELSER_V2_MODEL));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotAValidModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.SPARSE_EMBEDDING);

        try (var service = createService(factory)) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                null,
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

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.TEXT_EMBEDDING);

        try (var service = createService(factory)) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                null,
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
                is(
                    "Inference entity [model_id] does not support task type [text_embedding] "
                        + "for inference, the task type must be one of [sparse_embedding]."
                )
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid_ChatCompletion() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.CHAT_COMPLETION);

        try (var service = createService(factory)) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                null,
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
                is(
                    "Inference entity [model_id] does not support task type [chat_completion] "
                        + "for inference, the task type must be one of [sparse_embedding]. "
                        + "The task type for the inference entity is chat_completion, "
                        + "please use the _inference/chat_completion/model_id/_stream URL."
                )
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_SendsEmbeddingsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var elasticInferenceServiceURL = getUrl(webServer);

        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            String responseJson = """
                {
                    "data": [
                        {
                            "hello": 2.1259406,
                            "greet": 1.7073475
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(elasticInferenceServiceURL, "my-model-id");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("input text"),
                false,
                new HashMap<>(),
                InputType.SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result.asMap(),
                Matchers.is(
                    SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings(
                        List.of(
                            new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("hello", 2.1259406f, "greet", 1.7073475f), false)
                        )
                    )
                )
            );
            var request = webServer.requests().getFirst();
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, is(Map.of("input", List.of("input text"), "model", "my-model-id", "usage_context", "search")));
        }
    }

    public void testInfer_PropagatesProductUseCaseHeader() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var elasticInferenceServiceURL = getUrl(webServer);

        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            String responseJson = """
                {
                    "data": [
                        {
                            "hello": 2.1259406,
                            "greet": 1.7073475
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            // Set up the product use case in the thread context
            String productUseCase = "test-product-use-case";
            threadPool.getThreadContext().putHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, productUseCase);

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(elasticInferenceServiceURL, "my-model-id");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            try {
                service.infer(
                    model,
                    null,
                    null,
                    null,
                    List.of("input text"),
                    false,
                    new HashMap<>(),
                    InputType.SEARCH,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                );
                var result = listener.actionGet(TIMEOUT);

                // Verify the response was processed correctly
                assertThat(
                    result.asMap(),
                    Matchers.is(
                        SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings(
                            List.of(
                                new SparseEmbeddingResultsTests.EmbeddingExpectation(
                                    Map.of("hello", 2.1259406f, "greet", 1.7073475f),
                                    false
                                )
                            )
                        )
                    )
                );

                // Verify the header was sent in the request
                var request = webServer.requests().getFirst();
                assertNull(request.getUri().getQuery());
                assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

                // Check that the product use case header was set correctly
                assertThat(request.getHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER), is(productUseCase));

                // Verify request body
                var requestMap = entityAsMap(request.getBody());
                assertThat(requestMap, is(Map.of("input", List.of("input text"), "model", "my-model-id", "usage_context", "search")));
            } finally {
                // Clean up the thread context
                threadPool.getThreadContext().stashContext();
            }
        }
    }

    public void testChunkedInfer_PropagatesProductUseCaseHeader() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var elasticInferenceServiceURL = getUrl(webServer);

        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            String responseJson = """
                {
                    "data": [
                        {
                            "hello": 2.1259406,
                            "greet": 1.7073475
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            // Set up the product use case in the thread context
            String productUseCase = "test-product-use-case";
            threadPool.getThreadContext().putHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, productUseCase);

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(elasticInferenceServiceURL, "my-model-id");
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();

            try {
                service.chunkedInfer(
                    model,
                    null,
                    List.of(new ChunkInferenceInput("input text")),
                    new HashMap<>(),
                    InputType.INGEST,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                );

                var results = listener.actionGet(TIMEOUT);

                // Verify the response was processed correctly
                ChunkedInference inferenceResult = results.getFirst();
                assertThat(inferenceResult, instanceOf(ChunkedInferenceEmbedding.class));
                var sparseResult = (ChunkedInferenceEmbedding) inferenceResult;
                assertThat(
                    sparseResult.chunks(),
                    is(
                        List.of(
                            new EmbeddingResults.Chunk(
                                new SparseEmbeddingResults.Embedding(
                                    List.of(new WeightedToken("hello", 2.1259406f), new WeightedToken("greet", 1.7073475f)),
                                    false
                                ),
                                new ChunkedInference.TextOffset(0, "input text".length())
                            )
                        )
                    )
                );

                // Verify the request was sent and contains expected headers
                MatcherAssert.assertThat(webServer.requests(), hasSize(1));
                var request = webServer.requests().getFirst();
                assertNull(request.getUri().getQuery());
                MatcherAssert.assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

                // Check that the product use case header was set correctly
                assertThat(request.getHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER), is(productUseCase));

                // Verify request body
                var requestMap = entityAsMap(request.getBody());
                assertThat(requestMap, is(Map.of("input", List.of("input text"), "model", "my-model-id", "usage_context", "ingest")));
            } finally {
                // Clean up the thread context
                threadPool.getThreadContext().stashContext();
            }
        }
    }

    public void testUnifiedCompletionInfer_PropagatesProductUseCaseHeader() throws IOException {
        var elasticInferenceServiceURL = getUrl(webServer);
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            // Mock a successful streaming response
            String responseJson = """
                data: {"id":"1","object":"completion","created":1677858242,"model":"my-model-id",
                "choices":[{"finish_reason":null,"index":0,"delta":{"role":"assistant","content":"Hello"}}]}

                data: {"id":"2","object":"completion","created":1677858242,"model":"my-model-id",
                "choices":[{"finish_reason":"stop","index":0,"delta":{"content":" world!"}}]}

                data: [DONE]

                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            String productUseCase = "test-product-use-case";
            threadPool.getThreadContext().putHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, productUseCase);

            // Create completion model
            var model = new ElasticInferenceServiceCompletionModel(
                "id",
                TaskType.CHAT_COMPLETION,
                "elastic",
                new ElasticInferenceServiceCompletionServiceSettings("my-model-id", new RateLimitSettings(100)),
                EmptyTaskSettings.INSTANCE,
                EmptySecretSettings.INSTANCE,
                ElasticInferenceServiceComponents.of(elasticInferenceServiceURL)
            );

            var request = UnifiedCompletionRequest.of(
                List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("Hello"), "user", null, null))
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            try {
                service.unifiedCompletionInfer(model, request, InferenceAction.Request.DEFAULT_TIMEOUT, listener);

                // We don't need to check the actual response as we're only testing header propagation
                listener.actionGet(TIMEOUT);

                // Verify the request was sent
                assertThat(webServer.requests(), hasSize(1));
                var httpRequest = webServer.requests().getFirst();

                // Check that the product use case header was set correctly
                assertThat(httpRequest.getHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER), is(productUseCase));
            } finally {
                // Clean up the thread context
                threadPool.getThreadContext().stashContext();
            }
        }
    }

    public void testChunkedInfer_PassesThrough() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var elasticInferenceServiceURL = getUrl(webServer);

        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            String responseJson = """
                {
                    "data": [
                        {
                            "hello": 2.1259406,
                            "greet": 1.7073475
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(elasticInferenceServiceURL, "my-model-id");
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("input text")),
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results.get(0), instanceOf(ChunkedInferenceEmbedding.class));
            var sparseResult = (ChunkedInferenceEmbedding) results.get(0);
            assertThat(
                sparseResult.chunks(),
                is(
                    List.of(
                        new EmbeddingResults.Chunk(
                            new SparseEmbeddingResults.Embedding(
                                List.of(new WeightedToken("hello", 2.1259406f), new WeightedToken("greet", 1.7073475f)),
                                false
                            ),
                            new ChunkedInference.TextOffset(0, "input text".length())
                        )
                    )
                )
            );

            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap, is(Map.of("input", List.of("input text"), "model", "my-model-id", "usage_context", "ingest")));
        }
    }

    public void testHideFromConfigurationApi_ReturnsTrue_WithNoAvailableModels() throws Exception {
        try (var service = createServiceWithMockSender(ElasticInferenceServiceAuthorizationModel.newDisabledService())) {
            ensureAuthorizationCallFinished(service);

            assertTrue(service.hideFromConfigurationApi());
        }
    }

    public void testHideFromConfigurationApi_ReturnsTrue_WithModelTaskTypesThatAreNotImplemented() throws Exception {
        try (
            var service = createServiceWithMockSender(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING)
                            )
                        )
                    )
                )
            )
        ) {
            ensureAuthorizationCallFinished(service);

            assertTrue(service.hideFromConfigurationApi());
        }
    }

    public void testHideFromConfigurationApi_ReturnsFalse_WithAvailableModels() throws Exception {
        try (
            var service = createServiceWithMockSender(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.CHAT_COMPLETION)
                            )
                        )
                    )
                )
            )
        ) {
            ensureAuthorizationCallFinished(service);

            assertFalse(service.hideFromConfigurationApi());
        }
    }

    public void testGetConfiguration() throws Exception {
        try (
            var service = createServiceWithMockSender(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)
                            )
                        )
                    )
                )
            )
        ) {
            ensureAuthorizationCallFinished(service);

            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "elastic",
                       "name": "Elastic",
                       "task_types": ["sparse_embedding", "chat_completion"],
                       "configurations": {
                           "rate_limit.requests_per_minute": {
                               "description": "Minimize the number of rate limit errors.",
                               "label": "Rate Limit",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["sparse_embedding" , "chat_completion"]
                           },
                           "model_id": {
                               "description": "The name of the model to use for the inference task.",
                               "label": "Model ID",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["sparse_embedding" , "chat_completion"]
                           },
                           "max_input_tokens": {
                               "description": "Allows you to specify the maximum number of tokens per input.",
                               "label": "Maximum Input Tokens",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["sparse_embedding"]
                           }
                       }
                   }
                """);
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    public void testGetConfiguration_WithoutSupportedTaskTypes() throws Exception {
        try (var service = createServiceWithMockSender(ElasticInferenceServiceAuthorizationModel.newDisabledService())) {
            ensureAuthorizationCallFinished(service);

            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "elastic",
                       "name": "Elastic",
                       "task_types": [],
                       "configurations": {
                           "rate_limit.requests_per_minute": {
                               "description": "Minimize the number of rate limit errors.",
                               "label": "Rate Limit",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["sparse_embedding" , "chat_completion"]
                           },
                           "model_id": {
                               "description": "The name of the model to use for the inference task.",
                               "label": "Model ID",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["sparse_embedding" , "chat_completion"]
                           },
                           "max_input_tokens": {
                               "description": "Allows you to specify the maximum number of tokens per input.",
                               "label": "Maximum Input Tokens",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["sparse_embedding"]
                           }
                       }
                   }
                """);
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    public void testGetConfiguration_WithoutSupportedTaskTypes_WhenModelsReturnTaskOutsideOfImplementation() throws Exception {
        try (
            var service = createServiceWithMockSender(
                // this service doesn't yet support text embedding so we should still have no task types
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                "model-1",
                                EnumSet.of(TaskType.TEXT_EMBEDDING)
                            )
                        )
                    )
                )
            )
        ) {
            ensureAuthorizationCallFinished(service);

            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "elastic",
                       "name": "Elastic",
                       "task_types": [],
                       "configurations": {
                           "rate_limit.requests_per_minute": {
                               "description": "Minimize the number of rate limit errors.",
                               "label": "Rate Limit",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["sparse_embedding" , "chat_completion"]
                           },
                           "model_id": {
                               "description": "The name of the model to use for the inference task.",
                               "label": "Model ID",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["sparse_embedding" , "chat_completion"]
                           },
                           "max_input_tokens": {
                               "description": "Allows you to specify the maximum number of tokens per input.",
                               "label": "Maximum Input Tokens",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["sparse_embedding"]
                           }
                       }
                   }
                """);
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    public void testSupportedStreamingTasks_ReturnsChatCompletion_WhenAuthRespondsWithAValidModel() throws Exception {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "model-a",
                      "task_types": ["embed/text/sparse", "chat"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createServiceWithAuthHandler(senderFactory, getUrl(webServer))) {
            ensureAuthorizationCallFinished(service);

            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
            assertTrue(service.defaultConfigIds().isEmpty());

            PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
            service.defaultConfigs(listener);
            assertTrue(listener.actionGet(TIMEOUT).isEmpty());
        }
    }

    public void testSupportedTaskTypes_Returns_TheAuthorizedTaskTypes_IgnoresUnimplementedTaskTypes() throws Exception {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "model-a",
                      "task_types": ["embed/text/sparse"]
                    },
                    {
                      "model_name": "model-b",
                      "task_types": ["embed"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createServiceWithAuthHandler(senderFactory, getUrl(webServer))) {
            ensureAuthorizationCallFinished(service);

            assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING)));
        }
    }

    public void testSupportedTaskTypes_Returns_TheAuthorizedTaskTypes() throws Exception {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "model-a",
                      "task_types": ["embed/text/sparse"]
                    },
                    {
                      "model_name": "model-b",
                      "task_types": ["chat"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createServiceWithAuthHandler(senderFactory, getUrl(webServer))) {
            ensureAuthorizationCallFinished(service);

            assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)));
        }
    }

    public void testSupportedStreamingTasks_ReturnsEmpty_WhenAuthRespondsWithoutChatCompletion() throws Exception {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "model-a",
                      "task_types": ["embed/text/sparse"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createServiceWithAuthHandler(senderFactory, getUrl(webServer))) {
            ensureAuthorizationCallFinished(service);

            assertThat(service.supportedStreamingTasks(), is(EnumSet.noneOf(TaskType.class)));
            assertTrue(service.defaultConfigIds().isEmpty());
            assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING)));

            PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
            service.defaultConfigs(listener);
            assertTrue(listener.actionGet(TIMEOUT).isEmpty());
        }
    }

    public void testDefaultConfigs_Returns_DefaultChatCompletion_V1_WhenTaskTypeIsIncorrect() throws Exception {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "rainbow-sprinkles",
                      "task_types": ["embed/text/sparse"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createServiceWithAuthHandler(senderFactory, getUrl(webServer))) {
            ensureAuthorizationCallFinished(service);
            assertThat(service.supportedStreamingTasks(), is(EnumSet.noneOf(TaskType.class)));
            assertThat(
                service.defaultConfigIds(),
                is(
                    List.of(
                        new InferenceService.DefaultConfigId(
                            ".rainbow-sprinkles-elastic",
                            MinimalServiceSettings.chatCompletion(ElasticInferenceService.NAME),
                            service
                        )
                    )
                )
            );
            assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING)));

            PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
            service.defaultConfigs(listener);
            assertThat(listener.actionGet(TIMEOUT).get(0).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));
        }
    }

    public void testDefaultConfigs_Returns_DefaultEndpoints_WhenTaskTypeIsCorrect() throws Exception {
        String responseJson = """
            {
                "models": [
                    {
                      "model_name": "rainbow-sprinkles",
                      "task_types": ["chat"]
                    },
                    {
                      "model_name": "elser-v2",
                      "task_types": ["embed/text/sparse"]
                    }
                ]
            }
            """;

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createServiceWithAuthHandler(senderFactory, getUrl(webServer))) {
            ensureAuthorizationCallFinished(service);
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
            assertThat(
                service.defaultConfigIds(),
                is(
                    List.of(
                        new InferenceService.DefaultConfigId(
                            ".elser-v2-elastic",
                            MinimalServiceSettings.sparseEmbedding(ElasticInferenceService.NAME),
                            service
                        ),
                        new InferenceService.DefaultConfigId(
                            ".rainbow-sprinkles-elastic",
                            MinimalServiceSettings.chatCompletion(ElasticInferenceService.NAME),
                            service
                        )
                    )
                )
            );
            assertThat(service.supportedTaskTypes(), is(EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.SPARSE_EMBEDDING)));

            PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
            service.defaultConfigs(listener);
            var models = listener.actionGet(TIMEOUT);
            assertThat(models.size(), is(2));
            assertThat(models.get(0).getConfigurations().getInferenceEntityId(), is(".elser-v2-elastic"));
            assertThat(models.get(1).getConfigurations().getInferenceEntityId(), is(".rainbow-sprinkles-elastic"));
        }
    }

    public void testUnifiedCompletionError() {
        var e = assertThrows(UnifiedChatCompletionException.class, () -> testUnifiedStream(404, """
            {
                "error": "The model `rainbow-sprinkles` does not exist or you do not have access to it."
            }"""));
        assertThat(
            e.getMessage(),
            equalTo(
                "Received an unsuccessful status code for request from inference entity id [id] status "
                    + "[404]. Error message: [The model `rainbow-sprinkles` does not exist or you do not have access to it.]"
            )
        );
    }

    public void testUnifiedCompletionErrorMidStream() throws Exception {
        testUnifiedStreamError(200, """
            data: { "error": "some error" }

            """, """
            {\
            "error":{\
            "code":"stream_error",\
            "message":"Received an error response for request from inference entity id [id]. Error message: [some error]",\
            "type":"error"\
            }}""");
    }

    public void testUnifiedCompletionMalformedError() throws Exception {
        testUnifiedStreamError(200, """
            data: { i am not json }

            """, """
            {\
            "error":{\
            "code":"bad_request",\
            "message":"[1:3] Unexpected character ('i' (code 105)): was expecting double-quote to start field name\\n\
             at [Source: (String)\\"{ i am not json }\\"; line: 1, column: 3]",\
            "type":"x_content_parse_exception"\
            }}""");
    }

    private void testUnifiedStreamError(int responseCode, String responseJson, String expectedJson) throws Exception {
        testUnifiedStream(responseCode, responseJson).hasNoEvents().hasErrorMatching(e -> {
            e = unwrapCause(e);
            assertThat(e, isA(UnifiedChatCompletionException.class));
            try (var builder = XContentFactory.jsonBuilder()) {
                ((UnifiedChatCompletionException) e).toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                    try {
                        xContent.toXContent(builder, EMPTY_PARAMS);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                });
                var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());

                assertThat(json, is(expectedJson));
            }
        });
    }

    private InferenceEventsAssertion testUnifiedStream(int responseCode, String responseJson) throws Exception {
        var elasticInferenceServiceURL = getUrl(webServer);
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(responseJson));
            var model = new ElasticInferenceServiceCompletionModel(
                "id",
                TaskType.COMPLETION,
                "elastic",
                new ElasticInferenceServiceCompletionServiceSettings("model_id", new RateLimitSettings(100)),
                EmptyTaskSettings.INSTANCE,
                EmptySecretSettings.INSTANCE,
                ElasticInferenceServiceComponents.of(elasticInferenceServiceURL)
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            return InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream();
        }
    }

    private void ensureAuthorizationCallFinished(ElasticInferenceService service) {
        service.onNodeStarted();
        service.waitForFirstAuthorizationToComplete(TIMEOUT);
    }

    private ElasticInferenceService createServiceWithMockSender() {
        return createServiceWithMockSender(ElasticInferenceServiceAuthorizationModelTests.createEnabledAuth());
    }

    private ElasticInferenceService createServiceWithMockSender(ElasticInferenceServiceAuthorizationModel auth) {
        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(auth);
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);
        return new ElasticInferenceService(
            factory,
            createWithEmptySettings(threadPool),
            new ElasticInferenceServiceSettings(Settings.EMPTY),
            modelRegistry,
            mockAuthHandler
        );
    }

    private ElasticInferenceService createService(HttpRequestSender.Factory senderFactory) {
        return createService(senderFactory, ElasticInferenceServiceAuthorizationModelTests.createEnabledAuth(), null);
    }

    private ElasticInferenceService createService(HttpRequestSender.Factory senderFactory, String elasticInferenceServiceURL) {
        return createService(senderFactory, ElasticInferenceServiceAuthorizationModelTests.createEnabledAuth(), elasticInferenceServiceURL);
    }

    private ElasticInferenceService createService(
        HttpRequestSender.Factory senderFactory,
        ElasticInferenceServiceAuthorizationModel auth,
        String elasticInferenceServiceURL
    ) {
        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(auth);
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        return new ElasticInferenceService(
            senderFactory,
            createWithEmptySettings(threadPool),
            ElasticInferenceServiceSettingsTests.create(elasticInferenceServiceURL),
            modelRegistry,
            mockAuthHandler
        );
    }

    private ElasticInferenceService createServiceWithAuthHandler(
        HttpRequestSender.Factory senderFactory,
        String elasticInferenceServiceURL
    ) {
        return new ElasticInferenceService(
            senderFactory,
            createWithEmptySettings(threadPool),
            ElasticInferenceServiceSettingsTests.create(elasticInferenceServiceURL),
            modelRegistry,
            new ElasticInferenceServiceAuthorizationRequestHandler(elasticInferenceServiceURL, threadPool)
        );
    }
}
