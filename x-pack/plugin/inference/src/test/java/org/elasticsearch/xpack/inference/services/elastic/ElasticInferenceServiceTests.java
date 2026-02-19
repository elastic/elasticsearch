/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InferenceStringGroupTests;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.core.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelTests;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModel;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModelTests;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.InferenceString.DataFormat.BASE64;
import static org.elasticsearch.inference.InferenceString.DataType.IMAGE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getModelListenerForException;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.SenderServiceTests.createMockSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactoryTests.createNoopApplierFactory;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceTests extends ESTestCase {

    private static final String URL_VALUE = "http://eis-gateway.com";
    private static final String INFERENCE_ENTITY_ID = "id";
    private static final String MODEL_ID_VALUE = "some model id";

    private final MockWebServer webServer = new MockWebServer();

    private ThreadPool threadPool;

    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
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
            var modelListener = new TestPlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID,
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL), Map.of(), Map.of()),
                modelListener
            );

            var model = modelListener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));

            var sparseEmbeddingsModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
            assertThat(sparseEmbeddingsModel.getServiceSettings().modelId(), is(ElserModels.ELSER_V2_MODEL));
        }
    }

    public void testParseRequestConfig_CreatesADenseEmbeddingsModel_TextEmbedding() throws IOException {
        testParseRequestConfig_CreatesADenseEmbeddingsModel(TaskType.TEXT_EMBEDDING);
    }

    public void testParseRequestConfig_CreatesADenseEmbeddingsModel_Embedding() throws IOException {
        testParseRequestConfig_CreatesADenseEmbeddingsModel(TaskType.EMBEDDING);
    }

    private void testParseRequestConfig_CreatesADenseEmbeddingsModel(TaskType taskType) throws IOException {
        try (var service = createServiceWithMockSender()) {
            var modelId = "embedding_model";
            var dimensions = 1024;
            var similarity = "cosine";
            var maxInputTokens = 2048;
            var chunkingStrategy = "word";
            var maxChunkSize = 24;
            var overlap = maxChunkSize / 4;
            var modelListener = new TestPlainActionFuture<Model>();

            Map<String, Object> chunkingSettingsMap = new HashMap<>();
            chunkingSettingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), chunkingStrategy);
            chunkingSettingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
            chunkingSettingsMap.put(ChunkingSettingsOptions.OVERLAP.toString(), overlap);
            service.parseRequestConfig(
                "id",
                taskType,
                getRequestConfigMap(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        ServiceFields.DIMENSIONS,
                        dimensions,
                        ServiceFields.SIMILARITY,
                        similarity,
                        ServiceFields.MAX_INPUT_TOKENS,
                        maxInputTokens
                    ),
                    Map.of(),
                    chunkingSettingsMap,
                    Map.of()
                ),
                modelListener
            );

            var model = modelListener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(model, instanceOf(ElasticInferenceServiceDenseEmbeddingsModel.class));
            assertThat(model.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(model.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
            assertThat(model.getConfigurations().getTaskType(), is(taskType));
            assertThat(model.getConfigurations().getChunkingSettings(), is(new WordBoundaryChunkingSettings(maxChunkSize, overlap)));

            var serviceSettings = ((ElasticInferenceServiceDenseEmbeddingsModel) model).getServiceSettings();
            assertThat(serviceSettings.modelId(), is(modelId));
            assertThat(serviceSettings.dimensions(), is(dimensions));
            assertThat(serviceSettings.similarity(), is(SimilarityMeasure.fromString(similarity)));
            assertThat(serviceSettings.maxInputTokens(), is(maxInputTokens));
        }
    }

    public void testParseRequestConfig_CreatesARerankModel() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var modelListener = new TestPlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID,
                TaskType.RERANK,
                getRequestConfigMap(Map.of(ServiceFields.MODEL_ID, "my-rerank-model-id"), Map.of(), Map.of()),
                modelListener
            );

            var model = modelListener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(model, instanceOf(ElasticInferenceServiceRerankModel.class));
            ElasticInferenceServiceRerankModel rerankModel = (ElasticInferenceServiceRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is("my-rerank-model-id"));
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
            service.parseRequestConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, config, failureListener);
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
            service.parseRequestConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenRateLimitFieldExistsInServiceSettingsMap() throws IOException {
        try (var service = createServiceWithMockSender()) {
            Map<String, Object> serviceSettings = new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    ElserModels.ELSER_V2_MODEL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
                )
            );

            var config = getRequestConfigMap(serviceSettings, Map.of(), Map.of());

            var failureListener = getModelListenerForException(
                ValidationException.class,
                "Validation Failed: 1: [service_settings] rate limit settings are not permitted for "
                    + "service [elastic] and task type [sparse_embedding];"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, config, failureListener);
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
            service.parseRequestConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, config, failureListener);
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
            service.parseRequestConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, config, failureListener);
        }
    }

    public void testParseStoredConfig_CreatesASparseEmbeddingModel() throws IOException {
        try (var service = createServiceWithMockSender()) {
            {
                var mockedPersistedConfig = getBaseSparseEmbeddingConfig();

                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        new UnparsedModel(
                            INFERENCE_ENTITY_ID,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            mockedPersistedConfig.config(),
                            mockedPersistedConfig.secrets()
                        )
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getBaseSparseEmbeddingConfig();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        INFERENCE_ENTITY_ID,
                        TaskType.SPARSE_EMBEDDING,
                        mockedPersistedConfig.config(),
                        mockedPersistedConfig.secrets()
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getBaseSparseEmbeddingConfig();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, mockedPersistedConfig.config()),
                    ElserModels.ELSER_V2_MODEL
                );
            }
        }
    }

    public void testParseStoredConfig_CreatesADenseEmbeddingsModel_TextEmbedding() throws IOException {
        testParseStoredConfig_CreatesADenseEmbeddingsModel(TaskType.TEXT_EMBEDDING);
    }

    public void testParseStoredConfig_CreatesADenseEmbeddingsModel_Embedding() throws IOException {
        testParseStoredConfig_CreatesADenseEmbeddingsModel(TaskType.EMBEDDING);
    }

    private void testParseStoredConfig_CreatesADenseEmbeddingsModel(TaskType taskType) throws IOException {
        try (var service = createServiceWithMockSender()) {
            var modelId = "embedding_model";
            var dimensions = 1024;
            var similarity = "cosine";
            var maxInputTokens = 2048;
            var chunkingStrategy = "word";
            var maxChunkSize = 24;
            var overlap = maxChunkSize / 4;

            Map<String, Object> chunkingSettingsMap = new HashMap<>();
            chunkingSettingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), chunkingStrategy);
            chunkingSettingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
            chunkingSettingsMap.put(ChunkingSettingsOptions.OVERLAP.toString(), overlap);
            var config = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        ServiceFields.DIMENSIONS,
                        dimensions,
                        ServiceFields.SIMILARITY,
                        similarity,
                        ServiceFields.MAX_INPUT_TOKENS,
                        maxInputTokens
                    )
                ),
                Map.of(),
                chunkingSettingsMap,
                Map.of()
            );
            var model = service.parsePersistedConfigWithSecrets("id", taskType, config.config(), config.secrets());

            assertThat(model, instanceOf(ElasticInferenceServiceDenseEmbeddingsModel.class));
            assertThat(model.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(model.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
            assertThat(model.getConfigurations().getTaskType(), is(taskType));
            assertThat(model.getConfigurations().getChunkingSettings(), is(new WordBoundaryChunkingSettings(maxChunkSize, overlap)));

            var serviceSettings = ((ElasticInferenceServiceDenseEmbeddingsModel) model).getServiceSettings();
            assertThat(serviceSettings.modelId(), is(modelId));
            assertThat(serviceSettings.dimensions(), is(dimensions));
            assertThat(serviceSettings.similarity(), is(SimilarityMeasure.fromString(similarity)));
            assertThat(serviceSettings.maxInputTokens(), is(maxInputTokens));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createServiceWithMockSender()) {
            {
                var mockedPersistedConfig = getExtraKeyInConfig();

                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        new UnparsedModel(
                            INFERENCE_ENTITY_ID,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            mockedPersistedConfig.config(),
                            mockedPersistedConfig.secrets()
                        )
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInConfig();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        INFERENCE_ENTITY_ID,
                        TaskType.SPARSE_EMBEDDING,
                        mockedPersistedConfig.config(),
                        mockedPersistedConfig.secrets()
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInConfig();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, mockedPersistedConfig.config()),
                    ElserModels.ELSER_V2_MODEL
                );
            }
        }
    }

    private static Utils.PersistedConfig getBaseSparseEmbeddingConfig() {
        return getPersistedConfigMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL)), Map.of(), Map.of());
    }

    private static Utils.PersistedConfig getExtraKeyInConfig() {
        var persistedConfig = getBaseSparseEmbeddingConfig();
        persistedConfig.config().put("extra_key", "value");
        return persistedConfig;
    }

    public void testParseStoredConfig_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createServiceWithMockSender()) {
            {
                var mockedPersistedConfig = getExtraKeyInServiceSettings();

                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        new UnparsedModel(
                            INFERENCE_ENTITY_ID,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            mockedPersistedConfig.config(),
                            mockedPersistedConfig.secrets()
                        )
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInServiceSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        INFERENCE_ENTITY_ID,
                        TaskType.SPARSE_EMBEDDING,
                        mockedPersistedConfig.config(),
                        mockedPersistedConfig.secrets()
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInServiceSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, mockedPersistedConfig.config()),
                    ElserModels.ELSER_V2_MODEL
                );
            }
        }
    }

    private static Utils.PersistedConfig getExtraKeyInServiceSettings() {
        Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL));
        serviceSettingsMap.put("extra_key", "value");
        return getPersistedConfigMap(serviceSettingsMap, Map.of(), Map.of());
    }

    public void testParseStoredConfig_DoesNotThrowWhenRateLimitFieldExistsInServiceSettings() throws IOException {
        try (var service = createServiceWithMockSender()) {
            {
                var mockedPersistedConfig = getRateLimitInServiceSettings();

                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        new UnparsedModel(
                            INFERENCE_ENTITY_ID,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            mockedPersistedConfig.config(),
                            mockedPersistedConfig.secrets()
                        )
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getRateLimitInServiceSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        INFERENCE_ENTITY_ID,
                        TaskType.SPARSE_EMBEDDING,
                        mockedPersistedConfig.config(),
                        mockedPersistedConfig.secrets()
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getRateLimitInServiceSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, mockedPersistedConfig.config()),
                    ElserModels.ELSER_V2_MODEL
                );
            }
        }
    }

    private static Utils.PersistedConfig getRateLimitInServiceSettings() {
        Map<String, Object> serviceSettingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                ElserModels.ELSER_V2_MODEL,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );
        return getPersistedConfigMap(serviceSettingsMap, Map.of(), Map.of());
    }

    public void testParseStoredConfig_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createServiceWithMockSender()) {
            {
                var mockedPersistedConfig = getExtraKeyInTaskSettings();

                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        new UnparsedModel(
                            INFERENCE_ENTITY_ID,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            mockedPersistedConfig.config(),
                            mockedPersistedConfig.secrets()
                        )
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInTaskSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        INFERENCE_ENTITY_ID,
                        TaskType.SPARSE_EMBEDDING,
                        mockedPersistedConfig.config(),
                        mockedPersistedConfig.secrets()
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInTaskSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, mockedPersistedConfig.config()),
                    ElserModels.ELSER_V2_MODEL
                );
            }
        }
    }

    private static Utils.PersistedConfig getExtraKeyInTaskSettings() {
        var taskSettings = Map.of("extra_key", (Object) "value");
        return getPersistedConfigMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL)), taskSettings, Map.of());
    }

    public void testParseStoredConfig_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createServiceWithMockSender()) {
            {
                var mockedPersistedConfig = getExtraKeyInSecretsSettings();

                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        new UnparsedModel(
                            INFERENCE_ENTITY_ID,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            mockedPersistedConfig.config(),
                            mockedPersistedConfig.secrets()
                        )
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInSecretsSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfigWithSecrets(
                        INFERENCE_ENTITY_ID,
                        TaskType.SPARSE_EMBEDDING,
                        mockedPersistedConfig.config(),
                        mockedPersistedConfig.secrets()
                    ),
                    ElserModels.ELSER_V2_MODEL
                );
            }

            {
                var mockedPersistedConfig = getExtraKeyInSecretsSettings();
                assertSparseEmbeddingModelFromPersistedConfig(
                    service.parsePersistedConfig(INFERENCE_ENTITY_ID, TaskType.SPARSE_EMBEDDING, mockedPersistedConfig.config()),
                    ElserModels.ELSER_V2_MODEL
                );
            }
        }
    }

    private static Utils.PersistedConfig getExtraKeyInSecretsSettings() {
        var secretSettingsMap = Map.of("extra_key", (Object) "value");
        return getPersistedConfigMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, ElserModels.ELSER_V2_MODEL)),
            Map.of(),
            secretSettingsMap
        );
    }

    private static void assertSparseEmbeddingModelFromPersistedConfig(Model model, String expectedModelId) {
        assertThat(model, instanceOf(ElasticInferenceServiceSparseEmbeddingsModel.class));
        var sparseEmbeddingsModel = (ElasticInferenceServiceSparseEmbeddingsModel) model;
        assertThat(sparseEmbeddingsModel.getServiceSettings().modelId(), is(expectedModelId));
        assertThat(sparseEmbeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
        assertThat(sparseEmbeddingsModel.getSecretSettings(), is(EmptySecretSettings.INSTANCE));
    }

    public void testInfer_ThrowsErrorWhenModelIsNotAValidModel() throws IOException {
        var sender = createMockSender();

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

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            MatcherAssert.assertThat(
                thrownException.getMessage(),
                is("The internal model was invalid, please delete the service [service_name] with id [model_id] and add it again.")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).startAsynchronously(any());
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsValidationErrorForInvalidRerankParams() throws IOException {
        try (var service = createServiceWithMockSender()) {
            var model = ElasticInferenceServiceRerankModelTests.createModel(getUrl(webServer), "my-rerank-model-id");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            service.infer(
                model,
                "search query",
                Boolean.TRUE,
                10,
                List.of("doc1", "doc2", "doc3"),
                false,
                new HashMap<>(),
                InputType.SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ValidationException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is("Validation Failed: 1: Invalid return_documents [true]. The return_documents option is not supported by this service;")
            );
        }
    }

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid_ChatCompletion() throws IOException {
        var sender = createMockSender();

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

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            MatcherAssert.assertThat(
                thrownException.getMessage(),
                is(
                    "Inference entity [model_id] does not support task type [chat_completion] "
                        + "for inference, the task type must be one of [text_embedding, sparse_embedding, rerank, completion]. "
                        + "The task type for the inference entity is chat_completion, "
                        + "please use the _inference/chat_completion/model_id/_stream URL."
                )
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).startAsynchronously(any());
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_SendsSparseEmbeddingsRequest() throws IOException {
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
            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

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

    public void testInfer_SendsTextEmbeddingsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var elasticInferenceServiceURL = getUrl(webServer);

        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            float[] embeddings = { 0.123f, 0.456f };
            String responseJson = Strings.format("""
                {
                    "data": [%s]
                }
                """, Arrays.toString(embeddings));

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var modelId = "my-model-id";
            var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(
                TaskType.TEXT_EMBEDDING,
                elasticInferenceServiceURL,
                modelId
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var input = "input text";
            var inputType = "search";
            service.infer(
                model,
                null,
                null,
                null,
                List.of(input),
                false,
                new HashMap<>(),
                InputType.fromString(inputType),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result, instanceOf(DenseEmbeddingFloatResults.class));
            assertThat(result.asMap(), Matchers.is(DenseEmbeddingFloatResultsTests.buildExpectationFloat(List.of(embeddings))));

            var request = webServer.requests().getFirst();
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, is(Map.of("input", List.of(input), "model", modelId, "usage_context", inputType)));
        }
    }

    @SuppressWarnings("unchecked")
    public void testRerank_SendsRerankRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var elasticInferenceServiceURL = getUrl(webServer);

        try (var service = createService(senderFactory, elasticInferenceServiceURL)) {
            var modelId = "my-model-id";
            var topN = 2;
            String responseJson = """
                {
                    "results": [
                        {"index": 0, "relevance_score": 0.95},
                        {"index": 1, "relevance_score": 0.85},
                        {"index": 2, "relevance_score": 0.75}
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = ElasticInferenceServiceRerankModelTests.createModel(elasticInferenceServiceURL, modelId);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            service.infer(
                model,
                "search query",
                null,
                topN,
                List.of("doc1", "doc2", "doc3"),
                false,
                new HashMap<>(),
                InputType.SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            var resultMap = result.asMap();
            var rerankResults = (List<Map<String, Object>>) resultMap.get("rerank");
            assertThat(rerankResults.size(), Matchers.is(3));

            Map<String, Object> rankedDocOne = (Map<String, Object>) rerankResults.getFirst().get("ranked_doc");
            Map<String, Object> rankedDocTwo = (Map<String, Object>) rerankResults.get(1).get("ranked_doc");
            Map<String, Object> rankedDocThree = (Map<String, Object>) rerankResults.get(2).get("ranked_doc");

            assertThat(rankedDocOne.get("index"), equalTo(0));
            assertThat(rankedDocTwo.get("index"), equalTo(1));
            assertThat(rankedDocThree.get("index"), equalTo(2));

            // Verify the outgoing HTTP request
            var request = webServer.requests().getFirst();
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            // Verify the outgoing request body
            Map<String, Object> requestMap = entityAsMap(request.getBody());
            Map<String, Object> expectedRequestMap = Map.of(
                "query",
                "search query",
                "model",
                modelId,
                "top_n",
                topN,
                "documents",
                List.of("doc1", "doc2", "doc3")
            );
            assertThat(requestMap, is(expectedRequestMap));
        }
    }

    public void testInfer_PropagatesProductUseCaseHeader() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var elasticInferenceServiceURL = getUrl(webServer);

        try (
            var service = createService(senderFactory, elasticInferenceServiceURL);
            var ignored = threadPool.getThreadContext().stashContext()
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

            // Set up the product use case in the thread context
            String productUseCase = "test-product-use-case";
            threadPool.getThreadContext().putHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, productUseCase);

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
            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            // Verify the response was processed correctly
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

            // Verify the header was sent in the request
            var request = webServer.requests().getFirst();
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            // Check that the product use case header was set correctly
            var productUseCaseHeaders = request.getHeaders().get(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);
            assertThat(productUseCaseHeaders, contains("internal_search", productUseCase));

            // Verify request body
            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, is(Map.of("input", List.of("input text"), "model", "my-model-id", "usage_context", "search")));
        }
    }

    public void testUnifiedCompletionInfer_PropagatesProductUseCaseHeader() throws IOException {
        var elasticInferenceServiceURL = getUrl(webServer);
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (
            var service = createService(senderFactory, elasticInferenceServiceURL);
            var ignored = threadPool.getThreadContext().stashContext()
        ) {
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
                INFERENCE_ENTITY_ID,
                TaskType.CHAT_COMPLETION,
                new ElasticInferenceServiceCompletionServiceSettings("my-model-id"),
                ElasticInferenceServiceComponents.of(elasticInferenceServiceURL)
            );

            var request = UnifiedCompletionRequest.of(
                List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("Hello"), "user", null, null))
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            service.unifiedCompletionInfer(model, request, InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            // We don't need to check the actual response as we're only testing header propagation
            listener.actionGet(TEST_REQUEST_TIMEOUT);

            // Verify the request was sent
            assertThat(webServer.requests(), hasSize(1));
            var httpRequest = webServer.requests().getFirst();

            // Check that the product use case header was set correctly
            assertThat(httpRequest.getHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER), is(productUseCase));
        }
    }

    public void testChunkedInfer_PropagatesProductUseCaseHeader() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer)); var ignored = threadPool.getThreadContext().stashContext()) {

            // Batching will call the service with 2 inputs
            String responseJson = """
                {
                    "data": [
                        [
                            0.123,
                            -0.456,
                            0.789
                        ],
                        [
                            0.987,
                            -0.654,
                            0.321
                        ]
                    ],
                    "meta": {
                        "usage": {
                            "total_tokens": 10
                        }
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createTextEmbeddingModel(getUrl(webServer), "my-dense-model-id");

            String productUseCase = "test-product-use-case";
            threadPool.getThreadContext().putHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, productUseCase);

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            // 2 inputs
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("hello world"), new ChunkInferenceInput("dense embedding")),
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, hasSize(2));

            // Verify the response was processed correctly
            ChunkedInference inferenceResult = results.getFirst();
            assertThat(inferenceResult, instanceOf(ChunkedInferenceEmbedding.class));

            // Verify the request was sent and contains expected headers
            assertThat(webServer.requests(), hasSize(1));
            var request = webServer.requests().getFirst();
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            // Check that the product use case header was set correctly
            var productUseCaseHeaders = request.getHeaders().get(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);
            assertThat(productUseCaseHeaders, contains("internal_ingest", productUseCase));

        }
    }

    public void testEmbeddingInfer_PropagatesProductUseCaseHeader() throws IOException {
        var modelId = "my-dense-model-id";
        var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createEmbeddingModel(getUrl(webServer), modelId);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer)); var ignored = threadPool.getThreadContext().stashContext()) {
            String responseJson = """
                {
                    "data": [
                        [
                            0.123,
                            0.456
                        ]
                    ],
                    "meta": {
                        "usage": {
                            "total_tokens": 10
                        }
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            String productUseCase = "test-product-use-case";
            threadPool.getThreadContext().putHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, productUseCase);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(List.of(new InferenceStringGroup("first_input")), InputType.INGEST, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(webServer.requests(), hasSize(1));

            // Check that the product use case header was set correctly
            var productUseCaseHeaders = webServer.requests()
                .getFirst()
                .getHeaders()
                .get(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);
            assertThat(productUseCaseHeaders, contains("internal_ingest", productUseCase));
        }

    }

    public void testChunkedInfer_BatchesCalls_DefaultChunkingSettings_TextEmbedding() throws IOException {
        testChunkedInfer_BatchesCalls_DefaultChunkingSettings(TaskType.TEXT_EMBEDDING);
    }

    public void testChunkedInfer_BatchesCalls_DefaultChunkingSettings_Embedding() throws IOException {
        testChunkedInfer_BatchesCalls_DefaultChunkingSettings(TaskType.EMBEDDING);
    }

    private void testChunkedInfer_BatchesCalls_DefaultChunkingSettings(TaskType taskType) throws IOException {
        var modelId = "my-dense-model-id";
        var url = getUrl(webServer);
        var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(taskType, url, modelId);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, url)) {

            // Batching will call the service with 2 inputs
            float[] firstEmbedding = { 0.123f, -0.456f, 0.789f };
            float[] secondEmbedding = { 0.987f, -0.654f, 0.321f };
            String responseJson = Strings.format("""
                {
                    "data": [
                        %s,
                        %s
                    ],
                    "meta": {
                        "usage": {
                            "total_tokens": 10
                        }
                    }
                }
                """, Arrays.toString(firstEmbedding), Arrays.toString(secondEmbedding));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            // 2 inputs
            var firstInput = "hello world";
            var secondInput = "dense embedding";
            var inputType = "ingest";
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput(firstInput), new ChunkInferenceInput(secondInput)),
                new HashMap<>(),
                InputType.fromString(inputType),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, hasSize(2));

            // First result
            {
                assertThat(results.getFirst(), instanceOf(ChunkedInferenceEmbedding.class));
                var denseResult = (ChunkedInferenceEmbedding) results.getFirst();
                assertThat(denseResult.chunks(), hasSize(1));
                assertThat(denseResult.chunks().getFirst().offset(), is(new ChunkedInference.TextOffset(0, firstInput.length())));
                if (taskType == TaskType.TEXT_EMBEDDING) {
                    assertThat(denseResult.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                } else if (taskType == TaskType.EMBEDDING) {
                    assertThat(denseResult.chunks().getFirst().embedding(), instanceOf(GenericDenseEmbeddingFloatResults.Embedding.class));
                }

                var embedding = (EmbeddingFloatResults.Embedding) denseResult.chunks().getFirst().embedding();
                assertThat(embedding.values(), is(firstEmbedding));
            }

            // Second result
            {
                assertThat(results.get(1), instanceOf(ChunkedInferenceEmbedding.class));
                var denseResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(denseResult.chunks(), hasSize(1));
                assertThat(denseResult.chunks().getFirst().offset(), is(new ChunkedInference.TextOffset(0, secondInput.length())));
                if (taskType == TaskType.TEXT_EMBEDDING) {
                    assertThat(denseResult.chunks().getFirst().embedding(), instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                } else if (taskType == TaskType.EMBEDDING) {
                    assertThat(denseResult.chunks().getFirst().embedding(), instanceOf(GenericDenseEmbeddingFloatResults.Embedding.class));
                }

                var embedding = (EmbeddingFloatResults.Embedding) denseResult.chunks().getFirst().embedding();
                assertThat(embedding.values(), is(secondEmbedding));
            }

            assertThat(webServer.requests(), hasSize(1));
            var request = webServer.requests().getFirst();
            if (taskType == TaskType.TEXT_EMBEDDING) {
                assertThat(request.getUri().getPath(), endsWith("/embed/text/dense"));
            } else {
                assertThat(request.getUri().getPath(), endsWith("/embed/dense"));
            }
            assertThat(request.getUri().getQuery(), nullValue());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            List<Object> expectedInputs;
            if (taskType == TaskType.TEXT_EMBEDDING) {
                expectedInputs = List.of(firstInput, secondInput);
            } else {
                expectedInputs = List.of(
                    InferenceStringGroupTests.toRequestMap(new InferenceStringGroup(firstInput)),
                    InferenceStringGroupTests.toRequestMap(new InferenceStringGroup(secondInput))
                );
            }

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, is(Map.of("input", expectedInputs, "model", modelId, "usage_context", inputType)));
        }
    }

    public void testChunkedInfer_noInputs() throws IOException {
        var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createTextEmbeddingModel(getUrl(webServer), "my-dense-model-id");

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer))) {
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(),
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    public void testEmbeddingInfer() throws IOException {
        var modelId = "my-dense-model-id";
        var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createEmbeddingModel(getUrl(webServer), modelId);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer))) {
            float[] firstEmbedding = { 0.123f, -0.456f, 0.789f };
            float[] secondEmbedding = { 0.987f, -0.654f, 0.321f };
            String responseJson = Strings.format("""
                {
                    "data": [
                        %s,
                        %s
                    ],
                    "meta": {
                        "usage": {
                            "total_tokens": 10
                        }
                    }
                }
                """, Arrays.toString(firstEmbedding), Arrays.toString(secondEmbedding));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var inputs = List.of(
                new InferenceStringGroup("first_input"),
                new InferenceStringGroup(new InferenceString(IMAGE, BASE64, "second_input"))
            );

            var inputType = randomFrom(InputType.INGEST, InputType.SEARCH);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(inputs, inputType, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(
                results.asMap(),
                is(GenericDenseEmbeddingFloatResultsTests.buildExpectationFloat(List.of(firstEmbedding, secondEmbedding)))
            );

            assertThat(webServer.requests(), hasSize(1));
            var request = webServer.requests().getFirst();
            assertThat(request.getUri().getPath(), endsWith("/embed/dense"));
            assertThat(request.getUri().getQuery(), nullValue());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            var expectedInputs = inputs.stream().map(InferenceStringGroupTests::toRequestMap).toList();
            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, is(Map.of("input", expectedInputs, "model", modelId, "usage_context", inputType.toString())));
        }
    }

    public void testEmbeddingInfer_WithNonEmbeddingModel_Throws() throws IOException {
        var modelId = "my-dense-model-id";
        var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createTextEmbeddingModel(getUrl(webServer), modelId);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer))) {
            var inputs = List.of(new InferenceStringGroup("first_input"));

            var inputType = randomFrom(InputType.INGEST, InputType.SEARCH);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(inputs, inputType, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                exception.getMessage(),
                is(
                    "Inference entity [id] does not support task type [text_embedding] for inference,"
                        + " the task type must be one of [embedding]."
                )
            );
        }

    }

    public void testBatching_GivenSparseAndMultipleChunksFittingInSingleBatch() throws IOException {
        var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "my-sparse-model-id",
            null,
            10,
            new WordBoundaryChunkingSettings(1, 0)
        );

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer))) {
            EmbeddingRequestChunker<?> embeddingRequestChunker = service.createEmbeddingRequestChunker(
                model,
                List.of(new ChunkInferenceInput("hello world plus"))
            );
            List<EmbeddingRequestChunker.BatchRequestAndListener> batches = embeddingRequestChunker.batchRequestsWithListeners(null);
            assertThat(batches, hasSize(1));
            assertThatBatchContains(batches.getFirst(), List.of(List.of("hello"), List.of(" world"), List.of(" plus")));
        }
    }

    public void testBatching_GivenSparseAndBatchSizeOfOne() throws IOException {
        var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "my-sparse-model-id",
            null,
            1,
            new WordBoundaryChunkingSettings(1, 0)
        );

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer))) {
            EmbeddingRequestChunker<?> embeddingRequestChunker = service.createEmbeddingRequestChunker(
                model,
                List.of(new ChunkInferenceInput("hello world"))
            );
            List<EmbeddingRequestChunker.BatchRequestAndListener> batches = embeddingRequestChunker.batchRequestsWithListeners(null);
            assertThat(batches, hasSize(2));
            assertThatBatchContains(batches.getFirst(), List.of(List.of("hello")));
            assertThatBatchContains(batches.get(1), List.of(List.of(" world")));
        }
    }

    public void testBatching_GivenSparseAndBatchSizeOfTwo() throws IOException {
        var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "my-sparse-model-id",
            null,
            2,
            new WordBoundaryChunkingSettings(1, 0)
        );

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = createService(senderFactory, getUrl(webServer))) {
            EmbeddingRequestChunker<?> embeddingRequestChunker = service.createEmbeddingRequestChunker(
                model,
                List.of(new ChunkInferenceInput("hello world plus"))
            );
            List<EmbeddingRequestChunker.BatchRequestAndListener> batches = embeddingRequestChunker.batchRequestsWithListeners(null);
            assertThat(batches, hasSize(2));
            assertThatBatchContains(batches.getFirst(), List.of(List.of("hello"), List.of(" world")));
            assertThatBatchContains(batches.get(1), List.of(List.of(" plus")));
        }
    }

    private static void assertThatBatchContains(EmbeddingRequestChunker.BatchRequestAndListener batch, List<List<String>> expectedChunks) {
        List<InferenceStringGroup> inferenceStringGroups = batch.batch().inputs().get();
        assertThat(inferenceStringGroups, hasSize(expectedChunks.size()));
        for (int i = 0; i < expectedChunks.size(); i++) {
            assertThat(
                inferenceStringGroups.get(i).inferenceStrings().stream().map(InferenceString::value).toList(),
                equalTo(expectedChunks.get(i))
            );
        }
    }

    public void testHideFromConfigurationApi_ThrowsUnsupported() throws Exception {
        try (var service = createServiceWithMockSender()) {
            expectThrows(UnsupportedOperationException.class, service::hideFromConfigurationApi);
        }
    }

    public void testCreateConfiguration() throws Exception {
        String content = XContentHelper.stripWhitespace("""
            {
                   "service": "elastic",
                   "name": "Elastic",
                   "task_types": ["sparse_embedding", "chat_completion", "text_embedding", "embedding"],
                   "configurations": {
                       "model_id": {
                           "description": "The name of the model to use for the inference task.",
                           "label": "Model ID",
                           "required": true,
                           "sensitive": false,
                           "updatable": false,
                           "type": "str",
                           "supported_task_types": ["text_embedding", "sparse_embedding" , "rerank", "chat_completion", "embedding"]
                       },
                       "max_input_tokens": {
                           "description": "Allows you to specify the maximum number of tokens per input.",
                           "label": "Maximum Input Tokens",
                           "required": false,
                           "sensitive": false,
                           "updatable": false,
                           "type": "int",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "embedding"]
                       },
                       "max_batch_size": {
                           "description": "Allows you to specify the maximum number of chunks per batch.",
                           "label": "Maximum Batch Size",
                           "required": false,
                           "sensitive": false,
                           "updatable": true,
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
        InferenceServiceConfiguration serviceConfiguration = ElasticInferenceService.createConfiguration(
            EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION, TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)
        );
        assertToXContentEquivalent(originalBytes, toXContent(serviceConfiguration, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testGetConfiguration_WithoutSupportedTaskTypes() throws Exception {
        String content = XContentHelper.stripWhitespace("""
            {
                   "service": "elastic",
                   "name": "Elastic",
                   "task_types": [],
                   "configurations": {
                       "model_id": {
                           "description": "The name of the model to use for the inference task.",
                           "label": "Model ID",
                           "required": true,
                           "sensitive": false,
                           "updatable": false,
                           "type": "str",
                           "supported_task_types": ["text_embedding", "sparse_embedding" , "rerank", "chat_completion", "embedding"]
                       },
                       "max_input_tokens": {
                           "description": "Allows you to specify the maximum number of tokens per input.",
                           "label": "Maximum Input Tokens",
                           "required": false,
                           "sensitive": false,
                           "updatable": false,
                           "type": "int",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "embedding"]
                       },
                       "max_batch_size": {
                           "description": "Allows you to specify the maximum number of chunks per batch.",
                           "label": "Maximum Batch Size",
                           "required": false,
                           "sensitive": false,
                           "updatable": true,
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
        var humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        InferenceServiceConfiguration serviceConfiguration = ElasticInferenceService.createConfiguration(EnumSet.noneOf(TaskType.class));
        assertToXContentEquivalent(originalBytes, toXContent(serviceConfiguration, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testGetConfiguration_ThrowsUnsupported() throws Exception {
        try (var service = createServiceWithMockSender()) {
            expectThrows(UnsupportedOperationException.class, service::getConfiguration);
        }
    }

    public void testSupportedStreamingTasks_ReturnsChatCompletion() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createService(senderFactory)) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.CHAT_COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
            assertTrue(service.defaultConfigIds().isEmpty());

            PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
            service.defaultConfigs(listener);
            assertTrue(listener.actionGet(TEST_REQUEST_TIMEOUT).isEmpty());
        }
    }

    public void testDefaultConfigs_ReturnsEmptyLists() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createService(senderFactory)) {
            assertTrue(service.defaultConfigIds().isEmpty());

            PlainActionFuture<List<Model>> listener = new PlainActionFuture<>();
            service.defaultConfigs(listener);
            assertTrue(listener.actionGet(TEST_REQUEST_TIMEOUT).isEmpty());
        }
    }

    public void testSupportedTaskTypes_Returns_Unsupported() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = createService(senderFactory)) {
            expectThrows(UnsupportedOperationException.class, service::supportedTaskTypes);
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
                INFERENCE_ENTITY_ID,
                TaskType.CHAT_COMPLETION,
                new ElasticInferenceServiceCompletionServiceSettings(MODEL_ID_VALUE),
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

            return InferenceEventsAssertion.assertThat(listener.actionGet(TEST_REQUEST_TIMEOUT)).hasFinishedStream();
        }
    }

    private ElasticInferenceService createServiceWithMockSender() {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);
        var service = new ElasticInferenceService(
            factory,
            createWithEmptySettings(threadPool),
            new ElasticInferenceServiceSettings(Settings.EMPTY),
            mockClusterServiceEmpty(),
            createNoopApplierFactory()
        );
        service.init();
        return service;
    }

    private ElasticInferenceService createService(HttpRequestSender.Factory senderFactory) {
        return createService(senderFactory, URL_VALUE);
    }

    private ElasticInferenceService createService(HttpRequestSender.Factory senderFactory, String elasticInferenceServiceURL) {
        var service = new ElasticInferenceService(
            senderFactory,
            createWithEmptySettings(threadPool),
            ElasticInferenceServiceSettingsTests.create(elasticInferenceServiceURL),
            mockClusterServiceEmpty(),
            createNoopApplierFactory()
        );
        service.init();
        return service;
    }

    public void testBuildModelFromConfigAndSecrets_TextEmbedding() throws IOException {
        var model = createTestModel(TaskType.TEXT_EMBEDDING);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_SparseEmbedding() throws IOException {
        var model = createTestModel(TaskType.SPARSE_EMBEDDING);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_Completion() throws IOException {
        var model = createTestModel(TaskType.COMPLETION);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_ChatCompletion() throws IOException {
        var model = createTestModel(TaskType.CHAT_COMPLETION);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_Rerank() throws IOException {
        var model = createTestModel(TaskType.RERANK);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            INFERENCE_ENTITY_ID,
            TaskType.ANY,
            ElasticInferenceService.NAME,
            mock(ServiceSettings.class)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var inferenceService = createService(senderFactory)) {
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> inferenceService.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                thrownException.getMessage(),
                is(
                    Strings.format(
                        """
                            Failed to parse stored model [%s] for [%s] service, error: [The [%s] service does not support task type [%s]]. \
                            Please delete and add the service again""",
                        INFERENCE_ENTITY_ID,
                        ElasticInferenceService.NAME,
                        ElasticInferenceService.NAME,
                        TaskType.ANY
                    )
                )

            );
        }
    }

    private Model createTestModel(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> ElasticInferenceServiceDenseEmbeddingsModelTests.createTextEmbeddingModel(URL_VALUE, MODEL_ID_VALUE);
            case EMBEDDING -> ElasticInferenceServiceDenseEmbeddingsModelTests.createEmbeddingModel(URL_VALUE, MODEL_ID_VALUE);
            case SPARSE_EMBEDDING -> ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(URL_VALUE, MODEL_ID_VALUE);
            case COMPLETION -> ElasticInferenceServiceCompletionModelTests.createModel(URL_VALUE, MODEL_ID_VALUE, TaskType.COMPLETION);
            case CHAT_COMPLETION -> ElasticInferenceServiceCompletionModelTests.createModel(
                URL_VALUE,
                MODEL_ID_VALUE,
                TaskType.CHAT_COMPLETION
            );
            case RERANK -> ElasticInferenceServiceRerankModelTests.createModel(URL_VALUE, MODEL_ID_VALUE);
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }

    private void validateModelBuilding(Model model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var inferenceService = createService(senderFactory)) {
            var resultModel = inferenceService.buildModelFromConfigAndSecrets(model.getConfigurations(), model.getSecrets());
            assertThat(resultModel, is(model));
        }
    }
}
