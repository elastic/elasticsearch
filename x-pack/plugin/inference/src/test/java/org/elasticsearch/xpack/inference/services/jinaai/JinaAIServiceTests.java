/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.InferenceString.DataFormat.BASE64;
import static org.elasticsearch.inference.InferenceString.DataType.IMAGE;
import static org.elasticsearch.inference.InferenceString.DataType.TEXT;
import static org.elasticsearch.inference.ModelConfigurations.SERVICE_SETTINGS;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder.DEFAULT_SETTINGS;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder.OLD_DEFAULT_SETTINGS;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.TaskTypeTests.randomEmbeddingTaskType;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.SenderServiceTests.createMockSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettingsTests.getMapOfCommonEmbeddingSettings;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettingsTests.getMapOfMinimalEmbeddingSettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("resource")
public class JinaAIServiceTests extends InferenceServiceTestCase {
    private static final String DEFAULT_EMBEDDING_URL = "https://api.jina.ai/v1/embeddings";
    private static final String DEFAULT_RERANK_URL = "https://api.jina.ai/v1/rerank";
    private static final String MODEL_NAME_VALUE = "modelName";
    private static final String API_KEY_VALUE = "apiKey";
    private static final String INFERENCE_ENTITY_ID_VALUE = "id";
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

    public void testParseRequestConfig_createsEmbeddingsModel_textEmbeddingTask() throws IOException {
        testParseRequestConfig_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParseRequestConfig_createsEmbeddingsModel_embeddingTask() throws IOException {
        testParseRequestConfig_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParseRequestConfig_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var similarity = randomSimilarityMeasure();
            var dimensions = randomNonNegativeInt();
            var maxInputTokens = randomNonNegativeInt();
            var embeddingType = randomFrom(JinaAIEmbeddingType.values());
            var multimodalModel = taskType == TaskType.EMBEDDING && randomBoolean();
            var inputType = InputTypeTests.randomRequestType();
            var lateChunking = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);
            var chunkingSettings = createRandomChunkingSettings();

            var serviceSettingsMap = getMapOfCommonEmbeddingSettings(
                modelName,
                similarity,
                dimensions,
                null,
                maxInputTokens,
                embeddingType,
                requestsPerMinute
            );

            if (taskType == TaskType.EMBEDDING) {
                serviceSettingsMap.put(MULTIMODAL_MODEL, multimodalModel);
            }

            var modelListener = new PlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                taskType,
                getRequestConfigMap(
                    serviceSettingsMap,
                    JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(inputType, lateChunking),
                    chunkingSettings.asMap(),
                    getSecretSettingsMap(apiKey)
                ),
                modelListener
            );

            assertEmbeddingModelSettings(
                modelListener.actionGet(),
                modelName,
                new RateLimitSettings(requestsPerMinute),
                similarity,
                dimensions,
                true,
                maxInputTokens,
                embeddingType,
                multimodalModel,
                new JinaAIEmbeddingsTaskSettings(inputType, lateChunking),
                chunkingSettings,
                apiKey
            );
        }
    }

    public void testParseRequestConfig_createsRerankModel() throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new PlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.RERANK,
                getRequestConfigMap(
                    JinaAIRerankServiceSettingsTests.getServiceSettingsMap(modelName, requestsPerMinute),
                    JinaAIRerankTaskSettingsTests.getTaskSettingsMap(topN, returnDocuments),
                    getSecretSettingsMap(apiKey)
                ),
                modelListener
            );

            assertRerankModelSettings(
                modelListener.actionGet(),
                modelName,
                new RateLimitSettings(requestsPerMinute),
                apiKey,
                new JinaAIRerankTaskSettings(topN, returnDocuments)
            );
        }
    }

    public void testParseRequestConfig_onlyRequiredSettings_createsEmbeddingModel_textEmbedding() throws IOException {
        testParseRequestConfig_onlyRequiredSettings_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParseRequestConfig_onlyRequiredSettings_createsEmbeddingModel_embedding() throws IOException {
        testParseRequestConfig_onlyRequiredSettings_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParseRequestConfig_onlyRequiredSettings_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new PlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                taskType,
                getRequestConfigMap(
                    getMapOfCommonEmbeddingSettings(modelName, null, null, null, null, null, null),
                    getSecretSettingsMap(apiKey)
                ),
                modelListener
            );

            assertEmbeddingModelSettings(
                modelListener.actionGet(),
                modelName,
                DEFAULT_RATE_LIMIT_SETTINGS,
                null,
                null,
                false,
                null,
                JinaAIEmbeddingType.FLOAT,
                taskType == TaskType.EMBEDDING,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                DEFAULT_SETTINGS,
                apiKey
            );
        }
    }

    public void testParseRequestConfig_onlyRequiredSettings_createsRerankModel() throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new PlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.RERANK,
                getRequestConfigMap(JinaAIRerankServiceSettingsTests.getServiceSettingsMap(modelName), getSecretSettingsMap(apiKey)),
                modelListener
            );

            assertRerankModelSettings(
                modelListener.actionGet(),
                modelName,
                DEFAULT_RATE_LIMIT_SETTINGS,
                apiKey,
                JinaAIRerankTaskSettings.EMPTY_SETTINGS
            );

        }
    }

    public void testParseRequestConfig_ThrowsErrorWithUnsupportedTaskType() throws IOException {
        try (var service = createJinaAIService()) {
            var unsupportedTaskType = randomValueOtherThanMany(
                t -> service.supportedTaskTypes().contains(t),
                () -> randomFrom(TaskType.values())
            );
            var failureListener = getModelListenerForStatusException(
                Strings.format("The [jinaai] service does not support task type [%s]", unsupportedTaskType)
            );

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                unsupportedTaskType,
                getRequestConfigMap(getMapOfMinimalEmbeddingSettings("model"), getSecretSettingsMap("secret")),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            var config = getRequestConfigMap(getMapOfMinimalEmbeddingSettings("model"), getSecretSettingsMap("secret"));
            config.put("extra_key", "value");

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettings = getMapOfMinimalEmbeddingSettings("model");
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getSecretSettingsMap("secret"));

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParseRequestConfig_textEmbedding_throwsWhenMultimodalModelKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettings = getMapOfMinimalEmbeddingSettings("model");
            serviceSettings.put(MULTIMODAL_MODEL, true);

            var config = getRequestConfigMap(serviceSettings, getSecretSettingsMap("secret"));

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{multimodal_model=true}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_embedding_doesNotThrowWhenMultimodalModelKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = "model";
            var serviceSettings = getMapOfMinimalEmbeddingSettings(modelName);
            var multimodalModel = randomBoolean();
            serviceSettings.put(MULTIMODAL_MODEL, multimodalModel);

            String apiKey = "secret";
            var config = getRequestConfigMap(serviceSettings, getSecretSettingsMap(apiKey));

            var modelListener = new PlainActionFuture<Model>();
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.EMBEDDING, config, modelListener);

            assertEmbeddingModelSettings(
                modelListener.actionGet(),
                modelName,
                DEFAULT_RATE_LIMIT_SETTINGS,
                null,
                null,
                false,
                null,
                JinaAIEmbeddingType.FLOAT,
                multimodalModel,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                DEFAULT_SETTINGS,
                apiKey
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {

            var config = getRequestConfigMap(
                getMapOfMinimalEmbeddingSettings("model"),
                new HashMap<>(Map.of("extra_key", "value")),
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(getMapOfMinimalEmbeddingSettings("model"), secretSettingsMap);

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_createsEmbeddingsModel_textEmbedding() throws IOException {
        testParsePersistedConfigWithSecrets_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParsePersistedConfigWithSecrets_createsEmbeddingsModel_embedding() throws IOException {
        testParsePersistedConfigWithSecrets_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParsePersistedConfigWithSecrets_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var similarity = randomSimilarityMeasure();
            var dimensions = randomNonNegativeInt();
            var dimensionsSetByUser = randomBoolean();
            var maxInputTokens = randomNonNegativeInt();
            var embeddingType = randomFrom(JinaAIEmbeddingType.values());
            var multimodalModel = taskType == TaskType.EMBEDDING && randomBoolean();
            var inputType = InputTypeTests.randomRequestType();
            var lateChunking = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);
            var chunkingSettings = createRandomChunkingSettings();

            var serviceSettingsMap = getMapOfCommonEmbeddingSettings(
                modelName,
                similarity,
                dimensions,
                dimensionsSetByUser,
                maxInputTokens,
                embeddingType,
                requestsPerMinute
            );

            if (taskType == TaskType.EMBEDDING) {
                serviceSettingsMap.put(MULTIMODAL_MODEL, multimodalModel);
            }

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(inputType, lateChunking),
                chunkingSettings.asMap(),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                taskType,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertEmbeddingModelSettings(
                model,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                similarity,
                dimensions,
                dimensionsSetByUser,
                maxInputTokens,
                embeddingType,
                multimodalModel,
                new JinaAIEmbeddingsTaskSettings(inputType, lateChunking),
                chunkingSettings,
                apiKey
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_createsRerankModel() throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);

            var persistedConfig = getPersistedConfigMap(
                JinaAIRerankServiceSettingsTests.getServiceSettingsMap(modelName, requestsPerMinute),
                JinaAIRerankTaskSettingsTests.getTaskSettingsMap(topN, returnDocuments),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.RERANK,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertRerankModelSettings(
                model,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                apiKey,
                new JinaAIRerankTaskSettings(topN, returnDocuments)
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsEmbeddingsModel_textEmbedding() throws IOException {
        testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsEmbeddingsModel_embedding() throws IOException {
        testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);
            Map<String, Object> chunkingSettingsMap = randomBoolean() ? Map.of() : null;

            var persistedConfig = getPersistedConfigMap(
                getMapOfMinimalEmbeddingSettings(modelName),
                Map.of(),
                chunkingSettingsMap,
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                taskType,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertEmbeddingModelSettings(
                model,
                modelName,
                DEFAULT_RATE_LIMIT_SETTINGS,
                null,
                null,
                false,
                null,
                JinaAIEmbeddingType.FLOAT,
                taskType == TaskType.EMBEDDING,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                chunkingSettingsMap == null ? OLD_DEFAULT_SETTINGS : DEFAULT_SETTINGS,
                apiKey
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsRerankModel() throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap(modelName, null), Map.of(), getSecretSettingsMap(apiKey));

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.RERANK,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertRerankModelSettings(model, modelName, DEFAULT_RATE_LIMIT_SETTINGS, apiKey, JinaAIRerankTaskSettings.EMPTY_SETTINGS);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorWithUnsupportedTaskType() throws IOException {
        try (var service = createJinaAIService()) {
            var unsupportedTaskType = randomValueOtherThanMany(
                t -> service.supportedTaskTypes().contains(t),
                () -> randomFrom(TaskType.values())
            );
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("oldmodel", null), Map.of(), getSecretSettingsMap("secret"));

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfigWithSecrets(
                    INFERENCE_ENTITY_ID_VALUE,
                    unsupportedTaskType,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(thrownException.getMessage(), containsString("Failed to parse stored model [id] for [jinaai] service"));
            assertThat(
                thrownException.getMessage(),
                containsString(Strings.format("The [jinaai] service does not support task type [%s]", unsupportedTaskType))
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            String apiKey = "secret";
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(modelName, null),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH),
                getSecretSettingsMap(apiKey)
            );
            persistedConfig.config().put("extra_key", "value");

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig, modelName, apiKey);
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            String apiKey = "secret";
            var secretSettingsMap = getSecretSettingsMap(apiKey);
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap(modelName, null), Map.of(), secretSettingsMap);

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig, modelName, apiKey);
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            String apiKey = "secret";
            var serviceSettingsMap = getServiceSettingsMap(modelName, null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, Map.of(), getSecretSettingsMap("secret"));

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig, modelName, apiKey);
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            String apiKey = "secret";

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(modelName, null),
                new HashMap<>(Map.of("extra_key", "value")),
                getSecretSettingsMap(apiKey)
            );

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig, modelName, apiKey);
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInChunkingSettings() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            String apiKey = "secret";

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(modelName, null),
                Map.of(),
                Map.of(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.NONE.toString(), "extra_key", "value"),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                randomEmbeddingTaskType(),
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model.getServiceSettings().modelId(), is(modelName));
            assertThat(model.apiKey().toString(), is(apiKey));
        }
    }

    public void testParsePersistedConfig_createsEmbeddingsModel_textEmbedding() throws IOException {
        testParsePersistedConfig_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParsePersistedConfig_createsEmbeddingsModel_embedding() throws IOException {
        testParsePersistedConfig_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParsePersistedConfig_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var similarity = randomSimilarityMeasure();
            var dimensions = randomNonNegativeInt();
            var dimensionsSetByUser = randomBoolean();
            var maxInputTokens = randomNonNegativeInt();
            var embeddingType = randomFrom(JinaAIEmbeddingType.values());
            var multimodalModel = taskType == TaskType.EMBEDDING && randomBoolean();
            var inputType = InputTypeTests.randomRequestType();
            var lateChunking = randomBoolean();
            var chunkingSettings = createRandomChunkingSettings();

            var serviceSettingsMap = getMapOfCommonEmbeddingSettings(
                modelName,
                similarity,
                dimensions,
                dimensionsSetByUser,
                maxInputTokens,
                embeddingType,
                requestsPerMinute
            );

            if (taskType == TaskType.EMBEDDING) {
                serviceSettingsMap.put(MULTIMODAL_MODEL, multimodalModel);
            }

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(inputType, lateChunking),
                chunkingSettings.asMap(),
                null
            );

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, taskType, persistedConfig.config());

            assertEmbeddingModelSettings(
                model,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                similarity,
                dimensions,
                dimensionsSetByUser,
                maxInputTokens,
                embeddingType,
                multimodalModel,
                new JinaAIEmbeddingsTaskSettings(inputType, lateChunking),
                chunkingSettings,
                ""
            );
        }
    }

    public void testParsePersistedConfig_createsRerankModel() throws IOException {
        try (var service = createJinaAIService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();

            var persistedConfig = getPersistedConfigMap(
                JinaAIRerankServiceSettingsTests.getServiceSettingsMap(modelName, requestsPerMinute),
                JinaAIRerankTaskSettingsTests.getTaskSettingsMap(topN, returnDocuments),
                null
            );

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.RERANK, persistedConfig.config());

            assertRerankModelSettings(
                model,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                "",
                new JinaAIRerankTaskSettings(topN, returnDocuments)
            );
        }
    }

    public void testParsePersistedConfig_ThrowsErrorWithUnsupportedTaskType() throws IOException {
        try (var service = createJinaAIService()) {
            var unsupportedTaskType = randomValueOtherThanMany(
                t -> service.supportedTaskTypes().contains(t),
                () -> randomFrom(TaskType.values())
            );
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("model_old", null));

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, unsupportedTaskType, persistedConfig.config())
            );

            assertThat(thrownException.getMessage(), containsString("Failed to parse stored model [id] for [jinaai] service"));
            assertThat(
                thrownException.getMessage(),
                containsString(Strings.format("The [jinaai] service does not support task type [%s]", unsupportedTaskType))
            );
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap(modelName, null));
            persistedConfig.config().put("extra_key", "value");

            assertParsePersistedConfigMinimalSettings(service, persistedConfig, modelName);
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            var serviceSettingsMap = getServiceSettingsMap(modelName, null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap);

            assertParsePersistedConfigMinimalSettings(service, persistedConfig, modelName);
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;
            var taskSettingsMap = new HashMap<String, Object>(Map.of("extra_key", "value"));

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap(modelName, null), taskSettingsMap);

            assertParsePersistedConfigMinimalSettings(service, persistedConfig, modelName);
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInChunkingSettings() throws IOException {
        try (var service = createJinaAIService()) {
            String modelName = MODEL_NAME_VALUE;

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(modelName, null),
                Map.of(),
                Map.of(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.NONE.toString(), "extra_key", "value"),
                null
            );

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, randomEmbeddingTaskType(), persistedConfig.config());

            assertThat(model.getServiceSettings().modelId(), is(modelName));
            assertThat(model.apiKey().toString(), is(""));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(null, 128);
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(randomFrom(SimilarityMeasure.values()), 128);
    }

    public void testUpdateModelWithEmbeddingDetails_NullDimensionsInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(randomFrom(SimilarityMeasure.values()), null);
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityAndDimensionsInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(null, null);
    }

    private void testUpdateModelWithEmbeddingDetails_Successful(SimilarityMeasure similarityMeasure, Integer dimensions)
        throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var embeddingSize = randomNonNegativeInt();
            var embeddingType = randomFrom(JinaAIEmbeddingType.values());
            var model = JinaAIEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                RateLimitSettingsTests.createRandom(),
                similarityMeasure,
                dimensions,
                randomNonNegativeInt(),
                embeddingType,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                null,
                randomAlphaOfLength(10),
                false,
                randomEmbeddingTaskType(),
                randomBoolean()
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null
                ? JinaAIService.defaultSimilarity(embeddingType)
                : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testUpdateModelWithEmbeddingDetails_returnsExistingModelIfSettingsUnchanged() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var similarityMeasure = randomSimilarityMeasure();
            var dimensions = randomNonNegativeInt();
            var model = JinaAIEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                RateLimitSettingsTests.createRandom(),
                similarityMeasure,
                dimensions,
                randomNonNegativeInt(),
                randomFrom(JinaAIEmbeddingType.values()),
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                null,
                randomAlphaOfLength(10),
                false,
                randomEmbeddingTaskType(),
                randomBoolean()
            );

            assertThat(service.updateModelWithEmbeddingDetails(model, dimensions), sameInstance(model));
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotJinaAIModel() throws IOException {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new JinaAIService(factory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
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
            assertThat(
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

    public void testInfer_TextEmbedding_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = JinaAIEmbeddingsModelTests.createTextEmbeddingModel(getUrl(webServer), "model", "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_Rerank_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), "model", 1024, false);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                "query",
                null,
                null,
                List.of("candidate1", "candidate2"),
                false,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_TextEmbedding_Get_Response_Ingest() throws IOException {
        testInfer_TextEmbedding_Get_Response(randomFrom(InputType.INGEST, InputType.INTERNAL_INGEST), "retrieval.passage");
    }

    public void testInfer_TextEmbedding_Get_Response_Search() throws IOException {
        testInfer_TextEmbedding_Get_Response(randomFrom(InputType.SEARCH, InputType.INTERNAL_SEARCH), "retrieval.query");
    }

    public void testInfer_TextEmbedding_Get_Response_clustering() throws IOException {
        testInfer_TextEmbedding_Get_Response(InputType.CLUSTERING, "separation");
    }

    public void testInfer_TextEmbedding_Get_Response_classification() throws IOException {
        testInfer_TextEmbedding_Get_Response(InputType.CLASSIFICATION, "classification");
    }

    public void testInfer_TextEmbedding_Get_Response_unspecified() throws IOException {
        testInfer_TextEmbedding_Get_Response(InputType.UNSPECIFIED, null);
    }

    public void testInfer_TextEmbedding_Get_Response_NullInputType() throws IOException {
        testInfer_TextEmbedding_Get_Response(null, null);
    }

    private void testInfer_TextEmbedding_Get_Response(InputType inputType, String expectedJinaTask) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            var responseJson = """
                {
                    "model": "jina-embeddings-v3",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5,
                        "prompt_tokens": 5
                    },
                    "data": [
                        {
                            "object": "embedding",
                            "index": 0,
                            "embedding": [
                                0.123,
                                -0.123
                            ]
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            String modelName = "jina-embeddings-v3";
            int dimensions = 1024;
            String apiKey = API_KEY_VALUE;
            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                apiKey,
                dimensions,
                TEXT_EMBEDDING,
                false
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            List<String> input = List.of("abc");
            service.infer(
                model,
                null,
                null,
                null,
                input,
                false,
                new HashMap<>(),
                inputType,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + apiKey));

            Map<String, Object> expectedRequestMap = new HashMap<>(
                Map.of("input", input, "model", modelName, "embedding_type", "float", "dimensions", dimensions)
            );
            if (expectedJinaTask != null) {
                expectedRequestMap.put("task", expectedJinaTask);
            }
            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap, is(expectedRequestMap));
        }
    }

    public void testInfer_Rerank_Get_Response_NoReturnDocuments_NoTopN() throws IOException {
        String responseJson = """
            {
                "model": "model",
                "results": [
                    {
                        "index": 2,
                        "relevance_score": 0.98005307
                    },
                    {
                        "index": 1,
                        "relevance_score": 0.27904198
                    },
                    {
                        "index": 0,
                        "relevance_score": 0.10194652
                    }
                ],
                "usage": {
                    "total_tokens": 15
                }
            }
            """;
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), "secret", "model", null, false);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                "query",
                null,
                null,
                List.of("candidate1", "candidate2", "candidate3"),
                false,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            var resultAsMap = result.asMap();
            assertThat(
                resultAsMap,
                is(
                    Map.of(
                        "rerank",
                        List.of(
                            Map.of("ranked_doc", Map.of("index", 2, "relevance_score", 0.98005307F)),
                            Map.of("ranked_doc", Map.of("index", 1, "relevance_score", 0.27904198F)),
                            Map.of("ranked_doc", Map.of("index", 0, "relevance_score", 0.10194652F))
                        )
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "query",
                        "query",
                        "documents",
                        List.of("candidate1", "candidate2", "candidate3"),
                        "model",
                        "model",
                        "return_documents",
                        false
                    )
                )
            );

        }
    }

    public void testInfer_Rerank_Get_Response_NoReturnDocuments_TopN() throws IOException {
        String responseJson = """
            {
                "model": "model",
                "results": [
                    {
                        "index": 2,
                        "relevance_score": 0.98005307
                    },
                    {
                        "index": 1,
                        "relevance_score": 0.27904198
                    },
                    {
                        "index": 0,
                        "relevance_score": 0.10194652
                    }
                ],
                "usage": {
                    "total_tokens": 15
                }
            }
            """;
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), "secret", "model", 3, false);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                "query",
                null,
                null,
                List.of("candidate1", "candidate2", "candidate3", "candidate4"),
                false,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            var resultAsMap = result.asMap();
            assertThat(
                resultAsMap,
                is(
                    Map.of(
                        "rerank",
                        List.of(
                            Map.of("ranked_doc", Map.of("index", 2, "relevance_score", 0.98005307F)),
                            Map.of("ranked_doc", Map.of("index", 1, "relevance_score", 0.27904198F)),
                            Map.of("ranked_doc", Map.of("index", 0, "relevance_score", 0.10194652F))
                        )
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "query",
                        "query",
                        "documents",
                        List.of("candidate1", "candidate2", "candidate3", "candidate4"),
                        "model",
                        "model",
                        "return_documents",
                        false,
                        "top_n",
                        3
                    )
                )
            );

        }

    }

    public void testInfer_Rerank_Get_Response_ReturnDocumentsNull_NoTopN() throws IOException {
        String responseJson = """
            {
                "model": "model",
                "results": [
                    {
                        "index": 2,
                        "relevance_score": 0.98005307,
                        "document": {
                            "text": "candidate3"
                        }
                    },
                    {
                        "index": 1,
                        "relevance_score": 0.27904198,
                        "document": {
                            "text": "candidate2"
                        }
                    },
                    {
                        "index": 0,
                        "relevance_score": 0.10194652,
                        "document": {
                            "text": "candidate1"
                        }
                    }
                ],
                "usage": {
                    "total_tokens": 15
                }
            }
            """;
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), "secret", "model", null, null);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                "query",
                null,
                null,
                List.of("candidate1", "candidate2", "candidate3"),
                false,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            var resultAsMap = result.asMap();
            assertThat(
                resultAsMap,
                is(
                    Map.of(
                        "rerank",
                        List.of(
                            Map.of("ranked_doc", Map.of("text", "candidate3", "index", 2, "relevance_score", 0.98005307F)),
                            Map.of("ranked_doc", Map.of("text", "candidate2", "index", 1, "relevance_score", 0.27904198F)),
                            Map.of("ranked_doc", Map.of("text", "candidate1", "index", 0, "relevance_score", 0.10194652F))
                        )
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(Map.of("query", "query", "documents", List.of("candidate1", "candidate2", "candidate3"), "model", "model"))
            );

        }

    }

    public void testInfer_Rerank_Get_Response_ReturnDocuments_TopN() throws IOException {
        String responseJson = """
            {
                "model": "model",
                "results": [
                    {
                        "index": 2,
                        "relevance_score": 0.98005307,
                        "document": {
                            "text": "candidate3"
                        }
                    },
                    {
                        "index": 1,
                        "relevance_score": 0.27904198,
                        "document": {
                            "text": "candidate2"
                        }
                    },
                    {
                        "index": 0,
                        "relevance_score": 0.10194652,
                        "document": {
                            "text": "candidate1"
                        }
                    }
                ],
                "usage": {
                    "total_tokens": 15
                }
            }
            """;
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), "secret", "model", 3, true);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                "query",
                null,
                null,
                List.of("candidate1", "candidate2", "candidate3", "candidate4"),
                false,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            var resultAsMap = result.asMap();
            assertThat(
                resultAsMap,
                is(
                    Map.of(
                        "rerank",
                        List.of(
                            Map.of("ranked_doc", Map.of("text", "candidate3", "index", 2, "relevance_score", 0.98005307F)),
                            Map.of("ranked_doc", Map.of("text", "candidate2", "index", 1, "relevance_score", 0.27904198F)),
                            Map.of("ranked_doc", Map.of("text", "candidate1", "index", 0, "relevance_score", 0.10194652F))
                        )
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "query",
                        "query",
                        "documents",
                        List.of("candidate1", "candidate2", "candidate3", "candidate4"),
                        "model",
                        "model",
                        "return_documents",
                        true,
                        "top_n",
                        3
                    )
                )
            );

        }
    }

    public void test_TextEmbeddingModel_ChunkedInfer_BatchesCallsChunkingSettingsSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            createRandomChunkingSettings(),
            "secret",
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_TextEmbeddingModel_ChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            "secret",
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_TextEmbeddingModel_ChunkedInfer_LateChunkingEnabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            "secret",
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_TextEmbeddingModel_ChunkedInfer_LateChunkingDisabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, false),
            "secret",
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_embeddingModel_chunkedInfer_batchesCallsWhenLateChunkingEnabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            "secret",
            TaskType.EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_embeddingModel_chunkedInfer_batchesCallsWhenLateChunkingEnabled_inputContainsNonTextInput() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            "secret",
            TaskType.EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, false, true);
    }

    public void test_embeddingModel_chunkedInfer_batchesCallsWhenLateChunkingDisabled_inputContainsNonTextInput() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, false),
            "secret",
            TaskType.EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, false, true);
    }

    private void test_embedding_chunkedInfer_batchesCalls(
        JinaAIEmbeddingsModel model,
        Boolean expectMultipleResponses,
        boolean nonTextInput
    ) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            // 2 inputs
            String firstInput = "first_input";
            float[] firstEmbedding = { 0.123f, -0.123f };
            String secondInput = "second_input";
            float[] secondEmbedding = { 0.223f, -0.223f };

            List<Tuple<String, float[]>> inputsAndEmbeddings = List.of(
                Tuple.tuple(firstInput, firstEmbedding),
                Tuple.tuple(secondInput, secondEmbedding)
            );
            queueResponsesForChunkedInfer(expectMultipleResponses, inputsAndEmbeddings);

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            List<ChunkInferenceInput> inputs = new ArrayList<>();
            for (int i = 0; i < inputsAndEmbeddings.size(); ++i) {
                var anInput = new ChunkInferenceInput(inputsAndEmbeddings.get(i).v1());
                if (nonTextInput && i % 2 == 0) {
                    // Replace every other input with non-text if we're using non-text inputs
                    anInput = new ChunkInferenceInput(
                        new InferenceStringGroup(new InferenceString(IMAGE, BASE64, inputsAndEmbeddings.get(i).v1())),
                        null
                    );
                }
                inputs.add(anInput);
            }

            service.chunkedInfer(
                model,
                null,
                inputs,
                new HashMap<>(),
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, hasSize(inputsAndEmbeddings.size()));

            for (int i = 0; i < inputsAndEmbeddings.size(); ++i) {
                assertThat(results.get(i), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(i);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(
                    new ChunkedInference.TextOffset(0, inputsAndEmbeddings.get(i).v1().length()),
                    floatResult.chunks().getFirst().offset()
                );
                assertThat(floatResult.chunks().getFirst().embedding(), Matchers.instanceOf(EmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    inputsAndEmbeddings.get(i).v2(),
                    ((EmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values(),
                    0.0f
                );
            }
        }
    }

    private void queueResponsesForChunkedInfer(Boolean expectMultipleResponses, List<Tuple<String, float[]>> inputsAndEmbeddings) {
        if (Boolean.TRUE.equals(expectMultipleResponses)) {
            Function<MockRequest, String> bodyGenerator = (MockRequest r) -> {
                for (Tuple<String, float[]> inputAndEmbedding : inputsAndEmbeddings) {
                    if (r.getBody().contains(inputAndEmbedding.v1())) {
                        return Strings.format("""
                            {
                                "model": "jina-embeddings-v3",
                                "object": "list",
                                "usage": {
                                    "total_tokens": 5,
                                    "prompt_tokens": 5
                                },
                                "data": [
                                    {
                                        "object": "embedding",
                                        "index": 0,
                                        "embedding": %s
                                    }
                                ]
                            }
                            """, Arrays.toString(inputAndEmbedding.v2()));
                    }
                }
                throw new IllegalStateException("No matching inputs found for body generator");
            };
            // Queue a response for each request
            for (int i = 0; i < inputsAndEmbeddings.size(); ++i) {
                webServer.enqueue(new MockResponse().setResponseCode(200).setBody(bodyGenerator));
            }
        } else {
            // Queue a single response with multiple embeddings in it
            List<String> embeddingList = new ArrayList<>();
            int index = 0;
            for (Tuple<String, float[]> inputAndEmbedding : inputsAndEmbeddings) {
                embeddingList.add(Strings.format("""
                    {
                        "object": "embedding",
                        "index": %d,
                        "embedding": %s
                    }
                    """, index, Arrays.toString(inputAndEmbedding.v2())));
            }
            var responseJson = Strings.format("""
                {
                    "model": "jina-embeddings-v3",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5,
                        "prompt_tokens": 5
                    },
                    "data": %s
                }
                """, embeddingList);
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        }
    }

    public void test_ChunkedInfer_noInputs() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            "secret",
            randomFrom(TEXT_EMBEDDING, TaskType.EMBEDDING)
        );

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(),
                new HashMap<>(),
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    public void testEmbeddingInfer_returnsError_withNonJinaModel() throws IOException {
        String modelName = "model_id";
        String serviceName = "service_name";
        var mockModel = getInvalidModel(modelName, serviceName);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.embeddingInfer(
                mockModel,
                new EmbeddingRequest(List.of(), InputType.UNSPECIFIED, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(
                    Strings.format(
                        "The internal model was invalid, please delete the service [%s] with id [%s] and add it again.",
                        serviceName,
                        modelName
                    )
                )
            );
            assertThat(thrownException.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    public void testEmbeddingInfer_returnsError_withRerankModel() throws IOException {
        var model = JinaAIRerankModelTests.createModel(MODEL_NAME_VALUE);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(List.of(), InputType.UNSPECIFIED, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("The internal model was invalid, please delete the service [jinaai] with id [id] and add it again.")
            );
            assertThat(thrownException.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    public void testEmbeddingInfer_returnsError_nonMultimodalModel_withNonTextInput() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            MODEL_NAME_VALUE,
            JinaAIEmbeddingType.FLOAT,
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            API_KEY_VALUE,
            128,
            TaskType.EMBEDDING,
            false
        );

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputs = List.of(
                new InferenceStringGroup("first_input"),
                new InferenceStringGroup(new InferenceString(IMAGE, BASE64, "second_input"))
            );
            service.embeddingInfer(
                model,
                new EmbeddingRequest(inputs, InputType.UNSPECIFIED, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(thrownException.getMessage(), is("Non-text input provided for text-only model"));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testEmbeddingInfer_returnsError_multipleItemsInContentObject() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createEmbeddingModel(getUrl(webServer), MODEL_NAME_VALUE, API_KEY_VALUE);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            var listSize = randomIntBetween(1, 10);
            var inputs = new ArrayList<InferenceStringGroup>(listSize);
            for (int i = 0; i < listSize; ++i) {
                inputs.add(new InferenceStringGroup("a_string"));
            }

            // Add an InferenceStringGroup with multiple InferenceStrings at a random point in the input list
            var indexToAdd = randomIntBetween(0, inputs.size() - 1);
            var multipleInferenceStrings = new InferenceStringGroup(
                List.of(
                    new InferenceString(TEXT, InferenceString.DataFormat.TEXT, "first_input"),
                    new InferenceString(IMAGE, BASE64, "second_input")
                )
            );
            inputs.add(indexToAdd, multipleInferenceStrings);
            service.embeddingInfer(
                model,
                new EmbeddingRequest(inputs, InputType.UNSPECIFIED, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(
                    Strings.format(
                        "Field [content] must contain a single item for [jinaai] service. "
                            + "[content] object with multiple items found at $.input.content[%d]",
                        indexToAdd
                    )
                )
            );
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testEmbeddingInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = JinaAIEmbeddingsModelTests.createEmbeddingModel(getUrl(webServer), "model", "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(List.of(), InputType.UNSPECIFIED, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testEmbeddingInfer_Ingest() throws IOException {
        testEmbeddingInfer(randomFrom(InputType.INGEST, InputType.INTERNAL_INGEST), "retrieval.passage");
    }

    public void testEmbeddingInfer_Search() throws IOException {
        testEmbeddingInfer(randomFrom(InputType.SEARCH, InputType.INTERNAL_SEARCH), "retrieval.query");
    }

    public void testEmbeddingInfer_clustering() throws IOException {
        testEmbeddingInfer(InputType.CLUSTERING, "separation");
    }

    public void testEmbeddingInfer_classification() throws IOException {
        testEmbeddingInfer(InputType.CLASSIFICATION, "classification");
    }

    public void testEmbeddingInfer_nullInputType() throws IOException {
        testEmbeddingInfer(null, null);
    }

    public void testEmbeddingInfer_unspecifiedInputType() throws IOException {
        testEmbeddingInfer(InputType.UNSPECIFIED, null);
    }

    private void testEmbeddingInfer(InputType inputType, String expectedJinaTask) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            var responseJson = """
                {
                    "model": "jina-embeddings-v3",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5,
                        "prompt_tokens": 5
                    },
                    "data": [
                        {
                            "object": "embedding",
                            "index": 0,
                            "embedding": [
                                0.123,
                                -0.123
                            ]
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            String modelName = "jina-embeddings-v3";
            int dimensions = 1024;
            String apiKey = API_KEY_VALUE;
            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                apiKey,
                dimensions,
                TaskType.EMBEDDING,
                true
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var inputs = List.of(
                new InferenceStringGroup("first_input"),
                new InferenceStringGroup(new InferenceString(IMAGE, BASE64, "second_input"))
            );
            service.embeddingInfer(
                model,
                new EmbeddingRequest(inputs, inputType, Map.of()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertEquals(
                GenericDenseEmbeddingFloatResultsTests.buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })),
                result.asMap()
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + apiKey));

            Map<String, Object> expectedRequestMap = new HashMap<>(
                Map.of(
                    "input",
                    List.of(Map.of("text", "first_input"), Map.of("image", "second_input")),
                    "model",
                    modelName,
                    "embedding_type",
                    "float",
                    "dimensions",
                    dimensions
                )
            );
            if (expectedJinaTask != null) {
                expectedRequestMap.put("task", expectedJinaTask);
            }
            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap, is(expectedRequestMap));
        }
    }

    public void testDefaultSimilarity_BinaryEmbedding() {
        assertEquals(SimilarityMeasure.L2_NORM, JinaAIService.defaultSimilarity(JinaAIEmbeddingType.BINARY));
        assertEquals(SimilarityMeasure.L2_NORM, JinaAIService.defaultSimilarity(JinaAIEmbeddingType.BIT));
    }

    public void testDefaultSimilarity_NotBinaryEmbedding() {
        assertEquals(SimilarityMeasure.DOT_PRODUCT, JinaAIService.defaultSimilarity(JinaAIEmbeddingType.FLOAT));
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createJinaAIService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                            "service": "jinaai",
                            "name": "Jina AI",
                            "task_types": ["text_embedding", "rerank", "embedding"],
                            "configurations": {
                                "api_key": {
                                    "description": "API Key for the provider you're connecting to.",
                                    "label": "API Key",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "embedding"]
                                },
                                "dimensions": {
                                    "description": "The number of dimensions the resulting embeddings should have. For more information refer to https://api.jina.ai/docs#tag/embeddings/operation/create_embedding_v1_embeddings_post.",
                                    "label": "Dimensions",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding", "embedding"]
                                },
                                "embedding_type": {
                                    "description": "The type of embedding to return. One of [float, bit, binary]. bit and binary are equivalent and are encoded as bytes with signed int8 precision.",
                                    "label": "Embedding type",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "default_value": "float",
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "embedding"]
                                },
                                "similarity": {
                                    "description": "The similarity measure. One of [cosine, dot_product, l2_norm]. For float embeddings, the default similarity is dot_product. For bit and binary embeddings, the default similarity is l2_norm.",
                                    "label": "Similarity",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "embedding"]
                                },
                                "model_id": {
                                    "description": "The name of the model to use for the inference task.",
                                    "label": "Model ID",
                                    "required": true,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank", "embedding"]
                                },
                                "rate_limit.requests_per_minute": {
                                    "description": "Minimize the number of rate limit errors.",
                                    "label": "Rate Limit",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding", "rerank", "embedding"]
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

    public void testDoesNotSupportsStreaming() throws IOException {
        try (var service = new JinaAIService(mock(), createWithEmptySettings(mock()), mockClusterServiceEmpty())) {
            assertFalse(service.canStream(TaskType.COMPLETION));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }

    private Map<String, Object> getRequestConfigMap(
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

        return new HashMap<>(Map.of(SERVICE_SETTINGS, builtServiceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings));
    }

    private Map<String, Object> getRequestConfigMap(Map<String, Object> serviceSettings, Map<String, Object> secretSettings) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings));
    }

    private JinaAIService createJinaAIService() {
        return new JinaAIService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    @Override
    public InferenceService createInferenceService() {
        return createJinaAIService();
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), is(5500));
    }

    private static void assertEmbeddingModelSettings(
        Model model,
        String modelName,
        RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        JinaAIEmbeddingType embeddingType,
        boolean multimodalModel,
        JinaAIEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        String apiKey
    ) {
        assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

        var embeddingsModel = (JinaAIEmbeddingsModel) model;
        assertCommonModelSettings(
            embeddingsModel,
            DEFAULT_EMBEDDING_URL,
            modelName,
            rateLimitSettings,
            similarity,
            dimensions,
            dimensionsSetByUser,
            chunkingSettings,
            apiKey
        );

        assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(maxInputTokens));
        assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(embeddingType));
        assertThat(embeddingsModel.getServiceSettings().isMultimodal(), is(multimodalModel));

        assertThat(embeddingsModel.getTaskSettings(), is(taskSettings));
    }

    private static void assertRerankModelSettings(
        Model model,
        String modelName,
        RateLimitSettings rateLimitSettings,
        String apiKey,
        JinaAIRerankTaskSettings taskSettings
    ) {
        assertThat(model, instanceOf(JinaAIRerankModel.class));

        var rerankModel = (JinaAIRerankModel) model;
        assertCommonModelSettings(rerankModel, DEFAULT_RERANK_URL, modelName, rateLimitSettings, null, null, null, null, apiKey);

        assertThat(rerankModel.getTaskSettings(), is(taskSettings));
    }

    private static <T extends JinaAIModel> void assertCommonModelSettings(
        T model,
        String url,
        String modelName,
        RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable ChunkingSettings chunkingSettings,
        String apiKey
    ) {
        assertThat(model.uri().toString(), is(url));
        assertThat(model.getServiceSettings().modelId(), is(modelName));
        assertThat(model.rateLimitServiceSettings().rateLimitSettings(), is(rateLimitSettings));
        assertThat(model.getServiceSettings().similarity(), is(similarity));
        assertThat(model.getServiceSettings().dimensions(), is(dimensions));
        assertThat(model.getServiceSettings().dimensionsSetByUser(), is(dimensionsSetByUser));

        assertThat(model.getConfigurations().getChunkingSettings(), is(chunkingSettings));

        assertThat(model.apiKey().toString(), is(apiKey));
    }

    private static ActionListener<Model> getModelListenerForStatusException(String expectedMessage) {
        return ActionListener.wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, instanceOf(ElasticsearchStatusException.class));
            assertThat(e.getMessage(), is(expectedMessage));
        });
    }

    private static void assertParsePersistedConfigWithSecretsMinimalSettings(
        JinaAIService service,
        Utils.PersistedConfig persistedConfig,
        String modelName,
        String apiKey
    ) {
        var model = service.parsePersistedConfigWithSecrets(
            INFERENCE_ENTITY_ID_VALUE,
            randomFrom(service.supportedTaskTypes()),
            persistedConfig.config(),
            persistedConfig.secrets()
        );

        assertThat(model.getServiceSettings().modelId(), is(modelName));
        assertThat(model.apiKey().toString(), is(apiKey));
    }

    private static void assertParsePersistedConfigMinimalSettings(
        JinaAIService service,
        Utils.PersistedConfig persistedConfig,
        String modelName
    ) {
        var model = service.parsePersistedConfig(
            INFERENCE_ENTITY_ID_VALUE,
            randomFrom(service.supportedTaskTypes()),
            persistedConfig.config()
        );

        assertThat(model.getServiceSettings().modelId(), is(modelName));
        assertThat(model.apiKey().toString(), is(""));
    }

    public void testBuildModelFromConfigAndSecrets_TextEmbedding() throws IOException {
        var model = createTestModel(TaskType.TEXT_EMBEDDING);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_Embedding() throws IOException {
        var model = createTestModel(TaskType.EMBEDDING);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_Rerank() throws IOException {
        var model = createTestModel(TaskType.RERANK);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            INFERENCE_ENTITY_ID_VALUE,
            TaskType.CHAT_COMPLETION,
            JinaAIService.NAME,
            mock(ServiceSettings.class)
        );
        try (var inferenceService = createInferenceService()) {
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
                        INFERENCE_ENTITY_ID_VALUE,
                        JinaAIService.NAME,
                        JinaAIService.NAME,
                        TaskType.CHAT_COMPLETION
                    )
                )

            );
        }
    }

    private Model createTestModel(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> JinaAIEmbeddingsModelTests.createTextEmbeddingModel(
                DEFAULT_EMBEDDING_URL,
                MODEL_NAME_VALUE,
                API_KEY_VALUE
            );
            case EMBEDDING -> JinaAIEmbeddingsModelTests.createEmbeddingModel(DEFAULT_EMBEDDING_URL, MODEL_NAME_VALUE, API_KEY_VALUE);
            case RERANK -> JinaAIRerankModelTests.createModel(MODEL_NAME_VALUE);
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }

    private void validateModelBuilding(Model model) throws IOException {
        try (var inferenceService = createInferenceService()) {
            var resultModel = inferenceService.buildModelFromConfigAndSecrets(model.getConfigurations(), model.getSecrets());
            assertThat(resultModel, is(model));
        }
    }
}
