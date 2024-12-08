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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
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
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests;
import org.hamcrest.MatcherAssert;
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

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = OpenAiChatCompletionModelTests.createChatCompletionModel(
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
            service.chunkedInfer(model, null, input, new HashMap<>(), InputType.INGEST, InferenceAction.Request.DEFAULT_TIMEOUT, listener);

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

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                       "provider": "alibabacloud-ai-search",
                       "task_types": [
                             {
                                 "task_type": "text_embedding",
                                 "configuration": {
                                     "input_type": {
                                         "default_value": null,
                                         "depends_on": [],
                                         "display": "dropdown",
                                         "label": "Input Type",
                                         "options": [
                                             {
                                                 "label": "ingest",
                                                 "value": "ingest"
                                             },
                                             {
                                                 "label": "search",
                                                 "value": "search"
                                             }
                                         ],
                                         "order": 1,
                                         "required": false,
                                         "sensitive": false,
                                         "tooltip": "Specifies the type of input passed to the model.",
                                         "type": "str",
                                         "ui_restrictions": [],
                                         "validations": [],
                                         "value": ""
                                     }
                                 }
                             },
                             {
                                 "task_type": "sparse_embedding",
                                 "configuration": {
                                     "return_token": {
                                         "default_value": null,
                                         "depends_on": [],
                                         "display": "toggle",
                                         "label": "Return Token",
                                         "order": 2,
                                         "required": false,
                                         "sensitive": false,
                                         "tooltip": "If `true`, the token name will be returned in the response. Defaults to `false` which means only the token ID will be returned in the response.",
                                         "type": "bool",
                                         "ui_restrictions": [],
                                         "validations": [],
                                         "value": true
                                     },
                                     "input_type": {
                                         "default_value": null,
                                         "depends_on": [],
                                         "display": "dropdown",
                                         "label": "Input Type",
                                         "options": [
                                             {
                                                 "label": "ingest",
                                                 "value": "ingest"
                                             },
                                             {
                                                 "label": "search",
                                                 "value": "search"
                                             }
                                         ],
                                         "order": 1,
                                         "required": false,
                                         "sensitive": false,
                                         "tooltip": "Specifies the type of input passed to the model.",
                                         "type": "str",
                                         "ui_restrictions": [],
                                         "validations": [],
                                         "value": ""
                                     }
                                 }
                             },
                             {
                                 "task_type": "rerank",
                                 "configuration": {}
                             },
                             {
                                 "task_type": "completion",
                                 "configuration": {}
                             }
                       ],
                       "configuration": {
                         "workspace": {
                           "default_value": null,
                           "depends_on": [],
                           "display": "textbox",
                           "label": "Workspace",
                           "order": 5,
                           "required": true,
                           "sensitive": false,
                           "tooltip": "The name of the workspace used for the {infer} task.",
                           "type": "str",
                           "ui_restrictions": [],
                           "validations": [],
                           "value": null
                         },
                         "api_key": {
                           "default_value": null,
                           "depends_on": [],
                           "display": "textbox",
                           "label": "API Key",
                           "order": 1,
                           "required": true,
                           "sensitive": true,
                           "tooltip": "A valid API key for the AlibabaCloud AI Search API.",
                           "type": "str",
                           "ui_restrictions": [],
                           "validations": [],
                           "value": null
                         },
                         "service_id": {
                           "default_value": null,
                           "depends_on": [],
                           "display": "dropdown",
                           "label": "Project ID",
                           "options": [
                             {
                               "label": "ops-text-embedding-001",
                               "value": "ops-text-embedding-001"
                             },
                             {
                               "label": "ops-text-embedding-zh-001",
                               "value": "ops-text-embedding-zh-001"
                             },
                             {
                               "label": "ops-text-embedding-en-001",
                               "value": "ops-text-embedding-en-001"
                             },
                             {
                               "label": "ops-text-embedding-002",
                               "value": "ops-text-embedding-002"
                             },
                             {
                               "label": "ops-text-sparse-embedding-001",
                               "value": "ops-text-sparse-embedding-001"
                             },
                             {
                               "label": "ops-bge-reranker-larger",
                               "value": "ops-bge-reranker-larger"
                             }
                           ],
                           "order": 2,
                           "required": true,
                           "sensitive": false,
                           "tooltip": "The name of the model service to use for the {infer} task.",
                           "type": "str",
                           "ui_restrictions": [],
                           "validations": [],
                           "value": null
                         },
                         "host": {
                           "default_value": null,
                           "depends_on": [],
                           "display": "textbox",
                           "label": "Host",
                           "order": 3,
                           "required": true,
                           "sensitive": false,
                           "tooltip": "The name of the host address used for the {infer} task. You can find the host address at https://opensearch.console.aliyun.com/cn-shanghai/rag/api-key[ the API keys section] of the documentation.",
                           "type": "str",
                           "ui_restrictions": [],
                           "validations": [],
                           "value": null
                         },
                         "rate_limit.requests_per_minute": {
                           "default_value": null,
                           "depends_on": [],
                           "display": "numeric",
                           "label": "Rate Limit",
                           "order": 6,
                           "required": false,
                           "sensitive": false,
                           "tooltip": "Minimize the number of rate limit errors.",
                           "type": "int",
                           "ui_restrictions": [],
                           "validations": [],
                           "value": null
                         },
                         "http_schema": {
                           "default_value": null,
                           "depends_on": [],
                           "display": "dropdown",
                           "label": "HTTP Schema",
                           "options": [
                             {
                               "label": "https",
                               "value": "https"
                             },
                             {
                               "label": "http",
                               "value": "http"
                             }
                           ],
                           "order": 4,
                           "required": true,
                           "sensitive": false,
                           "tooltip": "",
                           "type": "str",
                           "ui_restrictions": [],
                           "validations": [],
                           "value": null
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
