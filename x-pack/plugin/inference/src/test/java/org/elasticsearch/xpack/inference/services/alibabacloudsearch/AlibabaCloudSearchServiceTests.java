/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.ChunkingSettingsFeatureFlag;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.alibabacloudsearch.AlibabaCloudSearchActionVisitor;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
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

    public void testParseRequestConfig_ThrowsElasticsearchStatusExceptionWhenChunkingSettingsProvidedAndFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), containsString("Model configuration contains settings"));
                }
            );

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

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWithoutChunkingSettingsWhenFeatureFlagDisabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
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
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWithoutChunkingSettingsWhenFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
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
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testCheckModelConfig() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool)) {
            @Override
            public void doInfer(
                Model model,
                InferenceInputs inputs,
                Map<String, Object> taskSettings,
                InputType inputType,
                TimeValue timeout,
                ActionListener<InferenceServiceResults> listener
            ) {
                InferenceTextEmbeddingFloatResults results = new InferenceTextEmbeddingFloatResults(
                    List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { -0.028680f, 0.022033f }))
                );

                listener.onResponse(results);
            }
        }) {
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
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            Map<String, Object> expectedServiceSettingsMap = new HashMap<>();
            expectedServiceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
            expectedServiceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
            expectedServiceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
            expectedServiceSettingsMap.put(ServiceFields.SIMILARITY, "DOT_PRODUCT");
            expectedServiceSettingsMap.put(ServiceFields.DIMENSIONS, 2);

            Map<String, Object> expectedTaskSettingsMap = new HashMap<>();

            Map<String, Object> expectedSecretSettingsMap = new HashMap<>();
            expectedSecretSettingsMap.put("api_key", "secret");

            var expectedModel = AlibabaCloudSearchEmbeddingsModelTests.createModel(
                "service",
                TaskType.TEXT_EMBEDDING,
                expectedServiceSettingsMap,
                expectedTaskSettingsMap,
                expectedSecretSettingsMap
            );

            MatcherAssert.assertThat(result, is(expectedModel));
        }
    }

    public void testChunkedInfer_TextEmbeddingBatches() throws IOException {
        testChunkedInfer(TaskType.TEXT_EMBEDDING, null);
    }

    public void testChunkedInfer_TextEmbeddingChunkingSettingsSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        testChunkedInfer(TaskType.TEXT_EMBEDDING, ChunkingSettingsTests.createRandomChunkingSettings());
    }

    public void testChunkedInfer_TextEmbeddingChunkingSettingsNotSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        testChunkedInfer(TaskType.TEXT_EMBEDDING, null);
    }

    public void testChunkedInfer_SparseEmbeddingBatches() throws IOException {
        testChunkedInfer(TaskType.SPARSE_EMBEDDING, null);
    }

    public void testChunkedInfer_SparseEmbeddingChunkingSettingsSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        testChunkedInfer(TaskType.SPARSE_EMBEDDING, ChunkingSettingsTests.createRandomChunkingSettings());
    }

    public void testChunkedInfer_SparseEmbeddingChunkingSettingsNotSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        testChunkedInfer(TaskType.SPARSE_EMBEDDING, null);
    }

    public void testChunkedInfer_InvalidTaskType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = AlibabaCloudSearchCompletionModelTests.createModel(
                randomAlphaOfLength(10),
                TaskType.COMPLETION,
                AlibabaCloudSearchCompletionServiceSettingsTests.createRandom(),
                AlibabaCloudSearchCompletionTaskSettingsTests.createRandom(),
                null
            );

            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            try {
                service.chunkedInfer(
                    model,
                    null,
                    List.of("foo", "bar"),
                    new HashMap<>(),
                    InputType.INGEST,
                    new ChunkingOptions(null, null),
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                );
            } catch (Exception e) {
                assertThat(e, instanceOf(IllegalArgumentException.class));
            }
        }
    }

    private void testChunkedInfer(TaskType taskType, ChunkingSettings chunkingSettings) throws IOException {
        var input = List.of("foo", "bar");

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = createModelForTaskType(taskType, chunkingSettings);

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
            assertThat(results, instanceOf(List.class));
            assertThat(results, hasSize(2));
            var firstResult = results.get(0);
            if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
                assertThat(firstResult, instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
            } else if (TaskType.SPARSE_EMBEDDING.equals(taskType)) {
                assertThat(firstResult, instanceOf(InferenceChunkedSparseEmbeddingResults.class));
            }
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
            public ExecutableAction accept(AlibabaCloudSearchActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
                return (inferenceInputs, timeout, listener) -> {
                    InferenceTextEmbeddingFloatResults results = new InferenceTextEmbeddingFloatResults(
                        List.of(
                            new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.0123f, -0.0123f }),
                            new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.0456f, -0.0456f })
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
            public ExecutableAction accept(AlibabaCloudSearchActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
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
