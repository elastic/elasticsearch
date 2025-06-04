/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.action.AlibabaCloudSearchActionVisitor;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class AlibabaCloudSearchServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));

                var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                    AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));

                var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                    AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));

                var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                    AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            var model = service.parsePersistedConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getPersistedConfigMap(
                    AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                    AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                    createRandomChunkingSettingsMap()
                ).config()
            );

            assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));
            var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            var model = service.parsePersistedConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getPersistedConfigMap(
                    AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                    AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null)
                ).config()
            );

            assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));
            var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            var persistedConfig = getPersistedConfigMap(
                AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );
            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));
            var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            var persistedConfig = getPersistedConfigMap(
                AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );
            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));
            var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = OpenAiChatCompletionModelTests.createCompletionModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10)
            );
            assertThrows(
                ElasticsearchStatusException.class,
                () -> { service.updateModelWithEmbeddingDetails(model, randomNonNegativeInt()); }
            );
        }
    }

    public void testUpdateModelWithEmbeddingDetails_UpdatesEmbeddingSizeAndSimilarity() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            var embeddingSize = randomNonNegativeInt();
            var model = AlibabaCloudSearchEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomFrom(TaskType.values()),
                AlibabaCloudSearchEmbeddingsServiceSettingsTests.createRandom(),
                AlibabaCloudSearchEmbeddingsTaskSettingsTests.createRandom(),
                null
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.DOT_PRODUCT, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testInfer_ThrowsValidationErrorForInvalidInputType_TextEmbedding() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        Map<String, Object> serviceSettingsMap = new HashMap<>();
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
        serviceSettingsMap.put(ServiceFields.DIMENSIONS, 1536);

        Map<String, Object> taskSettingsMap = new HashMap<>();

        Map<String, Object> secretSettingsMap = new HashMap<>();
        secretSettingsMap.put("api_key", "secret");

        var model = AlibabaCloudSearchEmbeddingsModelTests.createModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap
        );
        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var thrownException = expectThrows(
                ValidationException.class,
                () -> service.infer(
                    model,
                    null,
                    null,
                    null,
                    List.of(""),
                    false,
                    new HashMap<>(),
                    InputType.CLASSIFICATION,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                )
            );
            assertThat(
                thrownException.getMessage(),
                is("Validation Failed: 1: Input type [classification] is not supported for [AlibabaCloud AI Search];")
            );
        }
    }

    public void testInfer_ThrowsValidationExceptionForInvalidInputType_SparseEmbedding() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        Map<String, Object> serviceSettingsMap = new HashMap<>();
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
        serviceSettingsMap.put(ServiceFields.DIMENSIONS, 1536);

        Map<String, Object> taskSettingsMap = new HashMap<>();

        Map<String, Object> secretSettingsMap = new HashMap<>();
        secretSettingsMap.put("api_key", "secret");

        var model = AlibabaCloudSearchEmbeddingsModelTests.createModel(
            "service",
            TaskType.SPARSE_EMBEDDING,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap
        );
        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var thrownException = expectThrows(
                ValidationException.class,
                () -> service.infer(
                    model,
                    null,
                    null,
                    null,
                    List.of(""),
                    false,
                    new HashMap<>(),
                    InputType.CLASSIFICATION,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                )
            );
            assertThat(
                thrownException.getMessage(),
                is("Validation Failed: 1: Input type [classification] is not supported for [AlibabaCloud AI Search];")
            );
        }
    }

    public void testInfer_ThrowsValidationErrorForInvalidRerankParams() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        Map<String, Object> serviceSettingsMap = new HashMap<>();
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
        serviceSettingsMap.put(ServiceFields.DIMENSIONS, 1536);

        Map<String, Object> taskSettingsMap = new HashMap<>();

        Map<String, Object> secretSettingsMap = new HashMap<>();
        secretSettingsMap.put("api_key", "secret");

        var model = AlibabaCloudSearchEmbeddingsModelTests.createModel(
            "service",
            TaskType.RERANK,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap
        );
        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var thrownException = expectThrows(
                ValidationException.class,
                () -> service.infer(
                    model,
                    "hi",
                    Boolean.TRUE,
                    10,
                    List.of("a"),
                    false,
                    new HashMap<>(),
                    null,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                )
            );
            assertThat(
                thrownException.getMessage(),
                is(
                    "Validation Failed: 1: Invalid return_documents [true]. The return_documents option is not supported by this "
                        + "service;2: Invalid top_n [10]. The top_n option is not supported by this service;"
                )
            );
        }
    }

    public void testChunkedInfer_TextEmbeddingChunkingSettingsSet() throws IOException {
        testChunkedInfer(TaskType.TEXT_EMBEDDING, ChunkingSettingsTests.createRandomChunkingSettings());
    }

    public void testChunkedInfer_TextEmbeddingChunkingSettingsNotSet() throws IOException {
        testChunkedInfer(TaskType.TEXT_EMBEDDING, null);
    }

    public void testChunkedInfer_SparseEmbeddingChunkingSettingsSet() throws IOException {
        testChunkedInfer(TaskType.SPARSE_EMBEDDING, ChunkingSettingsTests.createRandomChunkingSettings());
    }

    public void testChunkedInfer_SparseEmbeddingChunkingSettingsNotSet() throws IOException {
        testChunkedInfer(TaskType.SPARSE_EMBEDDING, null);
    }

    private void testChunkedInfer(TaskType taskType, ChunkingSettings chunkingSettings) throws IOException {
        var input = List.of(new ChunkInferenceInput("foo"), new ChunkInferenceInput("bar"));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = createModelForTaskType(taskType, chunkingSettings);

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                input,
                new HashMap<>(),
                InputTypeTests.randomWithIngestAndSearch(),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, instanceOf(List.class));
            assertThat(results, hasSize(2));
            var firstResult = results.getFirst();
            assertThat(firstResult, instanceOf(ChunkedInferenceEmbedding.class));
            Class<?> expectedClass = switch (taskType) {
                case TEXT_EMBEDDING -> TextEmbeddingFloatResults.Chunk.class;
                case SPARSE_EMBEDDING -> SparseEmbeddingResults.Chunk.class;
                default -> null;
            };
            assertThat(((ChunkedInferenceEmbedding) firstResult).chunks().getFirst(), instanceOf(expectedClass));
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                       "service": "alibabacloud-ai-search",
                       "name": "AlibabaCloud AI Search",
                       "task_types": ["text_embedding", "sparse_embedding", "rerank", "completion"],
                       "configurations": {
                         "workspace": {
                           "description": "The name of the workspace used for the {infer} task.",
                           "label": "Workspace",
                           "required": true,
                           "sensitive": false,
                           "updatable": false,
                           "type": "str",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "rerank", "completion"]
                         },
                         "api_key": {
                           "description": "A valid API key for the AlibabaCloud AI Search API.",
                           "label": "API Key",
                           "required": true,
                           "sensitive": true,
                           "updatable": true,
                           "type": "str",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "rerank", "completion"]
                         },
                         "service_id": {
                           "description": "The name of the model service to use for the {infer} task.",
                           "label": "Project ID",
                           "required": true,
                           "sensitive": false,
                           "updatable": false,
                           "type": "str",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "rerank", "completion"]
                         },
                         "host": {
                           "description": "The name of the host address used for the {infer} task. You can find the host address at https://opensearch.console.aliyun.com/cn-shanghai/rag/api-key[ the API keys section] of the documentation.",
                           "label": "Host",
                           "required": true,
                           "sensitive": false,
                           "updatable": false,
                           "type": "str",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "rerank", "completion"]
                         },
                         "rate_limit.requests_per_minute": {
                           "description": "Minimize the number of rate limit errors.",
                           "label": "Rate Limit",
                           "required": false,
                           "sensitive": false,
                           "updatable": false,
                           "type": "int",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "rerank", "completion"]
                         },
                         "http_schema": {
                           "description": "",
                           "label": "HTTP Schema",
                           "required": false,
                           "sensitive": false,
                           "updatable": false,
                           "type": "str",
                           "supported_task_types": ["text_embedding", "sparse_embedding", "rerank", "completion"]
                         }
                       }
                    }
                    """
            );
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

    private AlibabaCloudSearchModel createModelForTaskType(TaskType taskType, ChunkingSettings chunkingSettings) {
        Map<String, Object> serviceSettingsMap = new HashMap<>();
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
        serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
        serviceSettingsMap.put(ServiceFields.DIMENSIONS, 1536);

        Map<String, Object> taskSettingsMap = new HashMap<>();

        Map<String, Object> secretSettingsMap = new HashMap<>();

        secretSettingsMap.put("api_key", "secret");
        return switch (taskType) {
            case TEXT_EMBEDDING -> createEmbeddingsModel(serviceSettingsMap, taskSettingsMap, chunkingSettings, secretSettingsMap);
            case SPARSE_EMBEDDING -> createSparseEmbeddingsModel(serviceSettingsMap, taskSettingsMap, chunkingSettings, secretSettingsMap);
            default -> throw new IllegalArgumentException("Unsupported task type for chunking: " + taskType);
        };
    }

    private AlibabaCloudSearchModel createEmbeddingsModel(
        Map<String, Object> serviceSettingsMap,
        Map<String, Object> taskSettingsMap,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettingsMap
    ) {
        return new AlibabaCloudSearchEmbeddingsModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            null
        ) {
            public ExecutableAction accept(AlibabaCloudSearchActionVisitor visitor, Map<String, Object> taskSettings) {
                return (inferenceInputs, timeout, listener) -> {
                    TextEmbeddingFloatResults results = new TextEmbeddingFloatResults(
                        List.of(
                            new TextEmbeddingFloatResults.Embedding(new float[] { 0.0123f, -0.0123f }),
                            new TextEmbeddingFloatResults.Embedding(new float[] { 0.0456f, -0.0456f })
                        )
                    );

                    listener.onResponse(results);
                };
            }
        };
    }

    private AlibabaCloudSearchModel createSparseEmbeddingsModel(
        Map<String, Object> serviceSettingsMap,
        Map<String, Object> taskSettingsMap,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettingsMap
    ) {
        return new AlibabaCloudSearchSparseModel(
            "service",
            TaskType.SPARSE_EMBEDDING,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            null
        ) {
            public ExecutableAction accept(AlibabaCloudSearchActionVisitor visitor, Map<String, Object> taskSettings) {
                return (inferenceInputs, timeout, listener) -> {
                    listener.onResponse(SparseEmbeddingResultsTests.createRandomResults(2, 1));
                };
            }
        };
    }

    public Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        var requestConfigMap = getRequestConfigMap(serviceSettings, taskSettings, secretSettings);
        requestConfigMap.put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return requestConfigMap;
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
}
