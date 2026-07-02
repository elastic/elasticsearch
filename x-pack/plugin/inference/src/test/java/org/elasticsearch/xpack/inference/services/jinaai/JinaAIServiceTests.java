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
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InferenceStringTests;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
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
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.DataFormat.BASE64;
import static org.elasticsearch.inference.DataType.IMAGE;
import static org.elasticsearch.inference.DataType.PDF;
import static org.elasticsearch.inference.InferenceString.fromStringList;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.inference.InferenceStringTests.randomDataTypeSupportingBase64;
import static org.elasticsearch.inference.InferenceStringTests.randomDataURI;
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
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MULTIMODAL_MODEL;
import static org.elasticsearch.xpack.inference.services.jinaai.AbstractJinaAIServiceSettingsTests.buildServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettingsTests.getMapOfCommonEmbeddingSettings;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.BaseJinaAIEmbeddingsServiceSettingsTests.getMapOfMinimalEmbeddingSettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class JinaAIServiceTests extends InferenceServiceTestCase {
    private static final String DEFAULT_EMBEDDING_URL = "https://api.jina.ai/v1/embeddings";
    private static final String DEFAULT_RERANK_URL = "https://api.jina.ai/v1/rerank";
    private static final String MODEL_NAME_VALUE = "some-model-name";
    private static final String API_KEY_VALUE = "some-api-key";
    private static final String INFERENCE_ENTITY_ID_VALUE = "id";

    public void testParseRequestConfig_createsEmbeddingsModel_textEmbeddingTask() throws IOException {
        testParseRequestConfig_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParseRequestConfig_createsEmbeddingsModel_embeddingTask() throws IOException {
        testParseRequestConfig_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParseRequestConfig_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createInferenceService()) {
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

            var modelListener = new TestPlainActionFuture<Model>();

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
        try (var service = createInferenceService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new TestPlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.RERANK,
                getRequestConfigMap(
                    JinaAIRerankServiceSettingsTests.buildServiceSettingsMap(modelName, requestsPerMinute),
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
        try (var service = createInferenceService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new TestPlainActionFuture<Model>();

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
        try (var service = createInferenceService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new TestPlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.RERANK,
                getRequestConfigMap(
                    JinaAIRerankServiceSettingsTests.buildServiceSettingsMap(modelName, null),
                    getSecretSettingsMap(apiKey)
                ),
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
        try (var service = createInferenceService()) {
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
                getRequestConfigMap(getMapOfMinimalEmbeddingSettings(MODEL_NAME_VALUE), getSecretSettingsMap(API_KEY_VALUE)),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createInferenceService()) {
            var config = getRequestConfigMap(getMapOfMinimalEmbeddingSettings(MODEL_NAME_VALUE), getSecretSettingsMap(API_KEY_VALUE));
            config.put("extra_key", "value");

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettings = getMapOfMinimalEmbeddingSettings(MODEL_NAME_VALUE);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getSecretSettingsMap(API_KEY_VALUE));

            // Service settings are parsed by a strict ObjectParser, which rejects unknown fields itself.
            var failureListener = getModelListenerForException(
                XContentParseException.class,
                "[service_settings] unknown field [extra_key]"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParseRequestConfig_textEmbedding_throwsWhenMultimodalModelKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettings = getMapOfMinimalEmbeddingSettings(MODEL_NAME_VALUE);
            serviceSettings.put(MULTIMODAL_MODEL, true);

            var config = getRequestConfigMap(serviceSettings, getSecretSettingsMap(API_KEY_VALUE));

            // text_embedding is non-multimodal, so multimodal_model is not a valid request field and the strict parser rejects it.
            var failureListener = getModelListenerForException(
                XContentParseException.class,
                "[service_settings] unknown field [multimodal_model]"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_embedding_doesNotThrowWhenMultimodalModelKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettings = getMapOfMinimalEmbeddingSettings(MODEL_NAME_VALUE);
            var multimodalModel = randomBoolean();
            serviceSettings.put(MULTIMODAL_MODEL, multimodalModel);

            var config = getRequestConfigMap(serviceSettings, getSecretSettingsMap(API_KEY_VALUE));

            var modelListener = new TestPlainActionFuture<Model>();
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.EMBEDDING, config, modelListener);

            assertEmbeddingModelSettings(
                modelListener.actionGet(),
                MODEL_NAME_VALUE,
                DEFAULT_RATE_LIMIT_SETTINGS,
                null,
                null,
                false,
                null,
                JinaAIEmbeddingType.FLOAT,
                multimodalModel,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                DEFAULT_SETTINGS,
                API_KEY_VALUE
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createInferenceService()) {

            var config = getRequestConfigMap(
                getMapOfMinimalEmbeddingSettings(MODEL_NAME_VALUE),
                new HashMap<>(Map.of("extra_key", "value")),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createInferenceService()) {
            var secretSettingsMap = getSecretSettingsMap(API_KEY_VALUE);
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(getMapOfMinimalEmbeddingSettings(MODEL_NAME_VALUE), secretSettingsMap);

            // Secrets are colocated with service settings in a request, so the strict service-settings parser rejects the unknown key.
            var failureListener = getModelListenerForException(
                XContentParseException.class,
                "[service_settings] unknown field [extra_key]"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, randomFrom(service.supportedTaskTypes()), config, failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_createsEmbeddingsModel_textEmbedding() throws IOException {
        testParsePersistedConfig_WithSecrets_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParsePersistedConfigWithSecrets_createsEmbeddingsModel_embedding() throws IOException {
        testParsePersistedConfig_WithSecrets_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParsePersistedConfig_WithSecrets_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createInferenceService()) {
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

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    taskType,
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
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

    public void testParsePersistedConfig_WithSecrets_createsRerankModel() throws IOException {
        try (var service = createInferenceService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);

            var persistedConfig = getPersistedConfigMap(
                JinaAIRerankServiceSettingsTests.buildServiceSettingsMap(modelName, requestsPerMinute),
                JinaAIRerankTaskSettingsTests.getTaskSettingsMap(topN, returnDocuments),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.RERANK,
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
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
        testParsePersistedConfig_WithSecrets_onlyRequiredSettings_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsEmbeddingsModel_embedding() throws IOException {
        testParsePersistedConfig_WithSecrets_onlyRequiredSettings_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParsePersistedConfig_WithSecrets_onlyRequiredSettings_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createInferenceService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);
            Map<String, Object> chunkingSettingsMap = randomBoolean() ? Map.of() : null;

            var persistedConfig = getPersistedConfigMap(
                getMapOfMinimalEmbeddingSettings(modelName),
                Map.of(),
                chunkingSettingsMap,
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    taskType,
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
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

    public void testParsePersistedConfig_WithSecrets_onlyRequiredSettings_createsRerankModel() throws IOException {
        try (var service = createInferenceService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var persistedConfig = getPersistedConfigMap(buildServiceSettingsMap(modelName, null), Map.of(), getSecretSettingsMap(apiKey));

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.RERANK,
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertRerankModelSettings(model, modelName, DEFAULT_RATE_LIMIT_SETTINGS, apiKey, JinaAIRerankTaskSettings.EMPTY_SETTINGS);
        }
    }

    public void testParsePersistedConfig_WithSecrets_ThrowsErrorWithUnsupportedTaskType() throws IOException {
        try (var service = createInferenceService()) {
            var unsupportedTaskType = randomValueOtherThanMany(
                t -> service.supportedTaskTypes().contains(t),
                () -> randomFrom(TaskType.values())
            );
            var persistedConfig = getPersistedConfigMap(
                buildServiceSettingsMap("oldmodel", null),
                Map.of(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig(
                    new UnparsedModel(
                        INFERENCE_ENTITY_ID_VALUE,
                        unsupportedTaskType,
                        JinaAIService.NAME,
                        persistedConfig.config(),
                        persistedConfig.secrets()
                    )
                )
            );

            assertThat(
                thrownException.getMessage(),
                containsString(Strings.format("Failed to parse stored model [%s] for [jinaai] service", INFERENCE_ENTITY_ID_VALUE))
            );
            assertThat(
                thrownException.getMessage(),
                containsString(Strings.format("The [jinaai] service does not support task type [%s]", unsupportedTaskType))
            );
        }
    }

    public void testParsePersistedConfig_WithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                buildServiceSettingsMap(MODEL_NAME_VALUE, null),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH),
                getSecretSettingsMap(API_KEY_VALUE)
            );
            persistedConfig.config().put("extra_key", "value");

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig);
        }
    }

    public void testParsePersistedConfig_WithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createInferenceService()) {
            var secretSettingsMap = getSecretSettingsMap(API_KEY_VALUE);
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(buildServiceSettingsMap(MODEL_NAME_VALUE, null), Map.of(), secretSettingsMap);

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig);
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettingsMap = buildServiceSettingsMap(MODEL_NAME_VALUE, null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, Map.of(), getSecretSettingsMap(API_KEY_VALUE));

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig);
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createInferenceService()) {

            var persistedConfig = getPersistedConfigMap(
                buildServiceSettingsMap(MODEL_NAME_VALUE, null),
                new HashMap<>(Map.of("extra_key", "value")),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            assertParsePersistedConfigWithSecretsMinimalSettings(service, persistedConfig);
        }
    }

    public void testParsePersistedConfig_WithSecrets_NotThrowWhenAnExtraKeyExistsInChunkingSettings() throws IOException {
        try (var service = createInferenceService()) {

            var persistedConfig = getPersistedConfigMap(
                buildServiceSettingsMap(MODEL_NAME_VALUE, null),
                Map.of(),
                Map.of(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.NONE.toString(), "extra_key", "value"),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = (JinaAIModel) service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    randomEmbeddingTaskType(),
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertApiKey(model, API_KEY_VALUE);
        }
    }

    public void testParsePersistedConfig_createsEmbeddingsModel_textEmbedding() throws IOException {
        testParsePersistedConfig_createsEmbeddingModel(TEXT_EMBEDDING);
    }

    public void testParsePersistedConfig_createsEmbeddingsModel_embedding() throws IOException {
        testParsePersistedConfig_createsEmbeddingModel(TaskType.EMBEDDING);
    }

    private void testParsePersistedConfig_createsEmbeddingModel(TaskType taskType) throws IOException {
        try (var service = createInferenceService()) {
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

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    taskType,
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
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
                null
            );
        }
    }

    public void testParsePersistedConfig_createsRerankModel() throws IOException {
        try (var service = createInferenceService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();

            var persistedConfig = getPersistedConfigMap(
                JinaAIRerankServiceSettingsTests.buildServiceSettingsMap(modelName, requestsPerMinute),
                JinaAIRerankTaskSettingsTests.getTaskSettingsMap(topN, returnDocuments),
                null
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.RERANK,
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertRerankModelSettings(
                model,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                null,
                new JinaAIRerankTaskSettings(topN, returnDocuments)
            );
        }
    }

    public void testParsePersistedConfig_ThrowsErrorWithUnsupportedTaskType() throws IOException {
        try (var service = createInferenceService()) {
            var unsupportedTaskType = randomValueOtherThanMany(
                t -> service.supportedTaskTypes().contains(t),
                () -> randomFrom(TaskType.values())
            );
            var persistedConfig = getPersistedConfigMap(buildServiceSettingsMap("model_old", null));

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig(
                    new UnparsedModel(
                        INFERENCE_ENTITY_ID_VALUE,
                        unsupportedTaskType,
                        JinaAIService.NAME,
                        persistedConfig.config(),
                        persistedConfig.secrets()
                    )
                )
            );

            assertThat(thrownException.getMessage(), containsString("Failed to parse stored model [id] for [jinaai] service"));
            assertThat(
                thrownException.getMessage(),
                containsString(Strings.format("The [jinaai] service does not support task type [%s]", unsupportedTaskType))
            );
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(buildServiceSettingsMap(MODEL_NAME_VALUE, null));
            persistedConfig.config().put("extra_key", "value");

            assertParsePersistedConfigMinimalSettings(service, persistedConfig);
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettingsMap = buildServiceSettingsMap(MODEL_NAME_VALUE, null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap);

            assertParsePersistedConfigMinimalSettings(service, persistedConfig);
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createInferenceService()) {
            var taskSettingsMap = new HashMap<String, Object>(Map.of("extra_key", "value"));

            var persistedConfig = getPersistedConfigMap(buildServiceSettingsMap(MODEL_NAME_VALUE, null), taskSettingsMap);

            assertParsePersistedConfigMinimalSettings(service, persistedConfig);
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInChunkingSettings() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                buildServiceSettingsMap(MODEL_NAME_VALUE, null),
                Map.of(),
                Map.of(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.NONE.toString(), "extra_key", "value"),
                null
            );

            var model = (JinaAIModel) service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    randomEmbeddingTaskType(),
                    JinaAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertApiKey(model, null);
        }
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

            var model = JinaAIEmbeddingsModelTests.createTextEmbeddingModel(getUrl(webServer), MODEL_NAME_VALUE, API_KEY_VALUE);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(model, List.of("abc"), false, new HashMap<>(), InputType.INGEST, null, listener);

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testRerankInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), MODEL_NAME_VALUE, 1024, false);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.rerankInfer(
                model,
                new RerankRequest(fromStringList(List.of("candidate1", "candidate2")), InferenceString.ofText("query"), null, null, null),
                null,
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
            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                API_KEY_VALUE,
                dimensions,
                TEXT_EMBEDDING,
                false
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            List<String> input = List.of("abc");
            service.infer(model, input, false, new HashMap<>(), inputType, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getUri().getQuery(), is(nullValue()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + API_KEY_VALUE));

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

    public void testRerankInfer_Get_Response_NoReturnDocuments_NoTopN() throws IOException {
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
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, MODEL_NAME_VALUE, null, false);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            var request = new RerankRequest(
                fromStringList(List.of("candidate1", "candidate2", "candidate3")),
                InferenceString.ofText("query"),
                null,
                null,
                null
            );
            service.rerankInfer(model, request, null, listener);

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
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                is(Strings.format("Bearer %s", API_KEY_VALUE))
            );

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
                        MODEL_NAME_VALUE,
                        "return_documents",
                        false
                    )
                )
            );

        }
    }

    public void testRerankInfer_Get_Response_NoReturnDocuments_TopN() throws IOException {
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
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, MODEL_NAME_VALUE, 3, false);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            var request = new RerankRequest(
                fromStringList(List.of("candidate1", "candidate2", "candidate3", "candidate4")),
                InferenceString.ofText("query"),
                null,
                null,
                null
            );
            service.rerankInfer(model, request, null, listener);

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
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                is(Strings.format("Bearer %s", API_KEY_VALUE))
            );

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
                        MODEL_NAME_VALUE,
                        "return_documents",
                        false,
                        "top_n",
                        3
                    )
                )
            );

        }

    }

    public void testRerankInfer_Get_Response_ReturnDocumentsNull_NoTopN() throws IOException {
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
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, MODEL_NAME_VALUE, null, null);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            var request = new RerankRequest(
                fromStringList(List.of("candidate1", "candidate2", "candidate3")),
                InferenceString.ofText("query"),
                null,
                null,
                null
            );
            service.rerankInfer(model, request, null, listener);

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
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                is(Strings.format("Bearer %s", API_KEY_VALUE))
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(Map.of("query", "query", "documents", List.of("candidate1", "candidate2", "candidate3"), "model", MODEL_NAME_VALUE))
            );

        }

    }

    public void testRerankInfer_Get_Response_ReturnDocuments_TopN() throws IOException {
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
            var model = JinaAIRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, MODEL_NAME_VALUE, 3, true);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            var request = new RerankRequest(
                fromStringList(List.of("candidate1", "candidate2", "candidate3", "candidate4")),
                InferenceString.ofText("query"),
                null,
                null,
                null
            );
            service.rerankInfer(model, request, null, listener);

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
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                is(Strings.format("Bearer %s", API_KEY_VALUE))
            );

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
                        MODEL_NAME_VALUE,
                        "return_documents",
                        true,
                        "top_n",
                        3
                    )
                )
            );

        }
    }

    public void testRerankInfer_ThrowsError_WithNonTextQuery() throws IOException {
        var textInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.of(DataType.TEXT)));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(textInputs, nonTextQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputs() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var textQuery = createRandomUsingDataTypes(EnumSet.of(DataType.TEXT));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, textQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputsAndQuery() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, nonTextQuery);
    }

    private void testRerankInfer_ThrowsError_WithNonTextInputOrQuery(List<InferenceString> inputs, InferenceString query)
        throws IOException {

        var model = mock(JinaAIRerankModel.class);

        try (var service = createInferenceService()) {
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            service.rerankInfer(model, new RerankRequest(inputs, query, null, null, new HashMap<>()), null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
            assertThat(thrownException.getMessage(), is("The jinaai service does not support rerank with non-text inputs or queries"));
        }
    }

    public void test_TextEmbeddingModel_ChunkedInfer_BatchesCallsChunkingSettingsSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            createRandomChunkingSettings(),
            API_KEY_VALUE,
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_TextEmbeddingModel_ChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            API_KEY_VALUE,
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_TextEmbeddingModel_ChunkedInfer_LateChunkingEnabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            API_KEY_VALUE,
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_TextEmbeddingModel_ChunkedInfer_LateChunkingDisabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, false),
            API_KEY_VALUE,
            TEXT_EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_embeddingModel_chunkedInfer_batchesCallsWhenLateChunkingEnabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            API_KEY_VALUE,
            TaskType.EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, model.getTaskSettings().getLateChunking(), false);
    }

    public void test_embeddingModel_chunkedInfer_batchesCallsWhenLateChunkingEnabled_inputContainsNonTextInput() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            API_KEY_VALUE,
            TaskType.EMBEDDING
        );

        test_embedding_chunkedInfer_batchesCalls(model, false, true);
    }

    public void test_embeddingModel_chunkedInfer_batchesCallsWhenLateChunkingDisabled_inputContainsNonTextInput() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-embeddings-v3",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, false),
            API_KEY_VALUE,
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
            String firstInput = nonTextInput ? randomDataURI() : "first_input";
            float[] firstEmbedding = { 0.123f, -0.123f };
            String secondInput = nonTextInput ? randomDataURI() : "second_input";
            float[] secondEmbedding = { 0.223f, -0.223f };

            List<Tuple<String, float[]>> inputsAndEmbeddings = List.of(
                Tuple.tuple(firstInput, firstEmbedding),
                Tuple.tuple(secondInput, secondEmbedding)
            );
            queueResponsesForChunkedInfer(expectMultipleResponses, inputsAndEmbeddings);

            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
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

            service.chunkedInfer(model, inputs, new HashMap<>(), InputType.UNSPECIFIED, null, listener);

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, hasSize(inputsAndEmbeddings.size()));

            for (int i = 0; i < inputsAndEmbeddings.size(); ++i) {
                assertThat(results.get(i), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(i);
                assertThat(floatResult.chunks(), hasSize(1));
                assertThat(
                    floatResult.chunks().getFirst().offset(),
                    is(new ChunkedInference.TextOffset(0, inputsAndEmbeddings.get(i).v1().length()))
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
            API_KEY_VALUE,
            randomFrom(TEXT_EMBEDDING, TaskType.EMBEDDING)
        );

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            service.chunkedInfer(model, List.of(), new HashMap<>(), InputType.UNSPECIFIED, null, listener);

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
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.embeddingInfer(
                mockModel,
                new EmbeddingRequest(List.of(new InferenceStringGroup("text input")), InputType.UNSPECIFIED, Map.of()),
                null,
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
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(List.of(new InferenceStringGroup("text input")), InputType.UNSPECIFIED, Map.of()),
                null,
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
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            var inputs = List.of(
                new InferenceStringGroup("first_input"),
                new InferenceStringGroup(new InferenceString(randomDataTypeSupportingBase64(), BASE64, TEST_DATA_URI))
            );
            service.embeddingInfer(model, new EmbeddingRequest(inputs, InputType.UNSPECIFIED, Map.of()), null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(thrownException.getMessage(), is("Non-text input provided for text-only model"));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testEmbeddingInfer_ReturnsError_MoreThanOneInputIncludingPdf_SingleInputPerGroup() throws IOException {
        testEmbeddingInfer_ReturnsError_MoreThanOneInputIncludingPdf(
            List.of(
                new InferenceStringGroup(new InferenceString(PDF, BASE64, TEST_DATA_URI)),
                new InferenceStringGroup(InferenceStringTests.createRandom())
            )
        );
    }

    public void testEmbeddingInfer_ReturnsError_MoreThanOneInputIncludingPdf_MultipleInputsInGroup() throws IOException {
        testEmbeddingInfer_ReturnsError_MoreThanOneInputIncludingPdf(
            List.of(new InferenceStringGroup(List.of(new InferenceString(PDF, BASE64, TEST_DATA_URI), InferenceStringTests.createRandom())))
        );
    }

    private void testEmbeddingInfer_ReturnsError_MoreThanOneInputIncludingPdf(List<InferenceStringGroup> inputs) throws IOException {
        var model = JinaAIEmbeddingsModelTests.createEmbeddingModel(getUrl(webServer), MODEL_NAME_VALUE, API_KEY_VALUE);

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            service.embeddingInfer(model, new EmbeddingRequest(inputs, InputType.UNSPECIFIED, Map.of()), null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("[jinaai] service does not support specifying more than one input if any inputs are of type [pdf]")
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

            var model = JinaAIEmbeddingsModelTests.createEmbeddingModel(getUrl(webServer), MODEL_NAME_VALUE, API_KEY_VALUE);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.embeddingInfer(
                model,
                new EmbeddingRequest(List.of(new InferenceStringGroup("text input")), InputType.UNSPECIFIED, Map.of()),
                null,
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

            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            var inputs = List.of(
                new InferenceStringGroup("first_input"),
                new InferenceStringGroup(new InferenceString(IMAGE, BASE64, TEST_DATA_URI))
            );
            service.embeddingInfer(model, new EmbeddingRequest(inputs, inputType, Map.of()), null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertEquals(
                GenericDenseEmbeddingFloatResultsTests.buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })),
                result.asMap()
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), is("Bearer " + apiKey));

            Map<String, Object> expectedRequestMap = new HashMap<>(
                Map.of(
                    "input",
                    List.of(Map.of("text", "first_input"), Map.of("image", TEST_DATA_URI)),
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
        assertThat(JinaAIService.defaultSimilarity(JinaAIEmbeddingType.BINARY), is(SimilarityMeasure.L2_NORM));
        assertThat(JinaAIService.defaultSimilarity(JinaAIEmbeddingType.BIT), is(SimilarityMeasure.L2_NORM));
    }

    public void testDefaultSimilarity_NotBinaryEmbedding() {
        assertThat(JinaAIService.defaultSimilarity(JinaAIEmbeddingType.FLOAT), is(SimilarityMeasure.DOT_PRODUCT));
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createInferenceService()) {
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

    @Override
    public InferenceService createInferenceService() {
        return new JinaAIService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        return JinaAIEmbeddingsModelTests.createModel(
            null,
            randomAlphaOfLength(8),
            null,
            similarity,
            null,
            null,
            null,
            JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            randomAlphaOfLength(8),
            false,
            TEXT_EMBEDDING,
            false
        );
    }

    @Override
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.noneOf(TaskType.class);
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), is(7000));
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
        @Nullable String apiKey
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
        @Nullable String apiKey,
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
        @Nullable String apiKey
    ) {
        assertThat(model.uri().toString(), is(url));
        assertThat(model.getServiceSettings().modelId(), is(modelName));
        assertThat(model.getServiceSettings().rateLimitSettings(), is(rateLimitSettings));
        assertThat(model.getServiceSettings().similarity(), is(similarity));
        assertThat(model.getServiceSettings().dimensions(), is(dimensions));
        assertThat(model.getServiceSettings().dimensionsSetByUser(), is(dimensionsSetByUser));

        assertThat(model.getConfigurations().getChunkingSettings(), is(chunkingSettings));

        assertApiKey(model, apiKey);
    }

    /**
     * Asserts the model's secret settings. A {@code null} {@code expectedApiKey} means no secrets are expected (e.g. a model parsed
     * from a persisted config without secrets), so the model must have no secret settings at all; otherwise the secret settings must
     * equal those for the expected api key.
     */
    private static void assertApiKey(JinaAIModel model, @Nullable String expectedApiKey) {
        var expectedSecretSettings = expectedApiKey == null
            ? null
            : new DefaultSecretSettings(new SecureString(expectedApiKey.toCharArray()));
        assertThat(model.getSecretSettings(), is(expectedSecretSettings));
    }

    private static ActionListener<Model> getModelListenerForStatusException(String expectedMessage) {
        return ActionListener.wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, instanceOf(ElasticsearchStatusException.class));
            assertThat(e.getMessage(), is(expectedMessage));
        });
    }

    private static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, instanceOf(exceptionClass));
            assertThat(e.getMessage(), containsString(expectedMessage));
        });
    }

    private static void assertParsePersistedConfigWithSecretsMinimalSettings(
        InferenceService service,
        Utils.PersistedConfig persistedConfig
    ) {
        var model = (JinaAIModel) service.parsePersistedConfig(
            new UnparsedModel(
                INFERENCE_ENTITY_ID_VALUE,
                randomFrom(service.supportedTaskTypes()),
                JinaAIService.NAME,
                persistedConfig.config(),
                persistedConfig.secrets()
            )
        );

        assertThat(model.getServiceSettings().modelId(), is(JinaAIServiceTests.MODEL_NAME_VALUE));
        assertApiKey(model, JinaAIServiceTests.API_KEY_VALUE);
    }

    private static void assertParsePersistedConfigMinimalSettings(InferenceService service, Utils.PersistedConfig persistedConfig) {
        var model = (JinaAIModel) service.parsePersistedConfig(
            new UnparsedModel(
                INFERENCE_ENTITY_ID_VALUE,
                randomFrom(service.supportedTaskTypes()),
                JinaAIService.NAME,
                persistedConfig.config(),
                persistedConfig.secrets()
            )
        );

        assertThat(model.getServiceSettings().modelId(), is(JinaAIServiceTests.MODEL_NAME_VALUE));
        assertApiKey(model, null);
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
                is(Strings.format("The [%s] service does not support task type [%s]", JinaAIService.NAME, TaskType.CHAT_COMPLETION))

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
