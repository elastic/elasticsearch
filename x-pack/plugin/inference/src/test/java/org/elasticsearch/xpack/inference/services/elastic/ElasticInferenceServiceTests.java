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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
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
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.elser.ElserModels;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceTests extends ESTestCase {

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
                Set.of(),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [elastic] service does not support task type [completion]"
            );

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), Map.of(), Map.of()),
                Set.of(),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var config = getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), Map.of(), Map.of());
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            Map<String, Object> serviceSettings = new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL));
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, Map.of(), Map.of());

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var taskSettings = Map.of("extra_key", (Object) "value");

            var config = getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), taskSettings, Map.of());

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, Set.of(), failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var secretSettings = Map.of("extra_key", (Object) "value");

            var config = getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), Map.of(), secretSettings);

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [elastic] service"
            );
            service.parseRequestConfig("id", TaskType.SPARSE_EMBEDDING, config, Set.of(), failureListener);
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

    public void testCheckModelConfig_ReturnsNewModelReference() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (
            var service = new ElasticInferenceService(
                senderFactory,
                createWithEmptySettings(threadPool),
                new ElasticInferenceServiceComponents(getUrl(webServer))
            )
        ) {
            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(getUrl(webServer));
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var returnedModel = listener.actionGet(TIMEOUT);
            assertThat(returnedModel, is(ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(getUrl(webServer))));
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotAValidModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (
            var service = new ElasticInferenceService(
                factory,
                createWithEmptySettings(threadPool),
                new ElasticInferenceServiceComponents(null)
            )
        ) {
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

    public void testInfer_SendsEmbeddingsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);

        try (
            var service = new ElasticInferenceService(
                senderFactory,
                createWithEmptySettings(threadPool),
                new ElasticInferenceServiceComponents(eisGatewayUrl)
            )
        ) {
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

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(eisGatewayUrl);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("input text"),
                false,
                new HashMap<>(),
                InputType.INGEST,
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
            var request = webServer.requests().get(0);
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, is(Map.of("input", List.of("input text"))));
        }
    }

    public void testChunkedInfer_PassesThrough() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);

        try (
            var service = new ElasticInferenceService(
                senderFactory,
                createWithEmptySettings(threadPool),
                new ElasticInferenceServiceComponents(eisGatewayUrl)
            )
        ) {
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

            var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(eisGatewayUrl);
            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of("input text"),
                new HashMap<>(),
                InputType.INGEST,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            MatcherAssert.assertThat(
                results.get(0).asMap(),
                Matchers.is(
                    Map.of(
                        InferenceChunkedSparseEmbeddingResults.FIELD_NAME,
                        List.of(
                            Map.of(
                                ChunkedNlpInferenceResults.TEXT,
                                "input text",
                                ChunkedNlpInferenceResults.INFERENCE,
                                Map.of("hello", 2.1259406f, "greet", 1.7073475f)
                            )
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
            assertThat(requestMap, is(Map.of("input", List.of("input text"))));
        }
    }

    private ElasticInferenceService createServiceWithMockSender() {
        return new ElasticInferenceService(
            mock(HttpRequestSender.Factory.class),
            createWithEmptySettings(threadPool),
            new ElasticInferenceServiceComponents(null)
        );
    }
}
