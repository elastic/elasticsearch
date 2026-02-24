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
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.SenderServiceTests.createMockSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class GoogleAiStudioServiceTests extends InferenceServiceTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String INFERENCE_ENTITY_ID_VALUE = "id";
    private static final String MODEL_ID_VALUE = "model";
    private static final String API_KEY_VALUE = "secret";
    private static final String URL_VALUE = "url";
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

    public void testParseRequestConfig_CreatesAGoogleAiStudioCompletionModel() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

                var completionModel = (GoogleAiStudioCompletionModel) model;
                assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
                assertThat(completionModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.COMPLETION,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAGoogleAiStudioEmbeddingsModel() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

                var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAGoogleAiStudioEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

                var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                    new HashMap<>(Map.of()),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAGoogleAiStudioEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

                var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
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
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> serviceSettings = new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE));
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMapEmpty(), getSecretSettingsMap("api_key"));

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> taskSettingsMap = new HashMap<>();
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                taskSettingsMap,
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> secretSettings = getSecretSettingsMap(API_KEY_VALUE);
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                secretSettings
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [googleaistudio] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, config, failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleAiStudioCompletionModel() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleAiStudioEmbeddingsModel() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

            var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleAiStudioEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

            var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), Matchers.instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

            var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), Matchers.instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var secretSettingsMap = getSecretSettingsMap(API_KEY_VALUE);
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE));
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMapEmpty(), getSecretSettingsMap(API_KEY_VALUE));

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> taskSettings = getTaskSettingsMapEmpty();
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                taskSettings,
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(completionModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_CreatesAGoogleAiStudioCompletionModel() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty()
            );

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAGoogleAiStudioEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty(),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

            var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAGoogleAiStudioEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty()
            );

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioEmbeddingsModel.class));

            var embeddingsModel = (GoogleAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(embeddingsModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)),
                getTaskSettingsMapEmpty()
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> serviceSettingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE));
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMapEmpty());

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createGoogleAiStudioService()) {
            Map<String, Object> taskSettings = getTaskSettingsMapEmpty();
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE)), taskSettings);

            var model = service.parsePersistedConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(GoogleAiStudioCompletionModel.class));

            var completionModel = (GoogleAiStudioCompletionModel) model;
            assertThat(completionModel.getServiceSettings().modelId(), is(MODEL_ID_VALUE));
            assertThat(completionModel.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
            assertNull(completionModel.getSecretSettings());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotGoogleAiStudioModel() throws IOException {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel(INFERENCE_ENTITY_ID_VALUE, "service_name");

        try (var service = new GoogleAiStudioService(factory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
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
                is("The internal model was invalid, please delete the service [service_name] with id [id] and add it again.")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).startAsynchronously(any());
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsValidationErrorWhenInputTypeIsSpecifiedForModelThatDoesNotAcceptTaskType() throws IOException {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var model = GoogleAiStudioEmbeddingsModelTests.createModel(MODEL_ID_VALUE, API_KEY_VALUE, getUrl(webServer));

        try (var service = new GoogleAiStudioService(factory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            service.infer(
                model,
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

            var thrownException = expectThrows(ValidationException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Validation Failed: 1: Invalid value [ingest] received. [input_type] is not allowed for model [model];")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).startAsynchronously(any());
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_SendsCompletionRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
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

            var model = GoogleAiStudioCompletionModelTests.createModel(MODEL_ID_VALUE, getUrl(webServer), API_KEY_VALUE);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
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
        var input = "input";

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
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

            var model = GoogleAiStudioEmbeddingsModelTests.createModel(MODEL_ID_VALUE, API_KEY_VALUE, getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of(input),
                false,
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getQuery(), endsWith(API_KEY_VALUE));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap, aMapWithSize(1));
            assertThat(
                requestMap.get("requests"),
                Matchers.is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", MODEL_ID_VALUE),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input))),
                            "taskType",
                            "RETRIEVAL_DOCUMENT"
                        )
                    )
                )
            );
        }
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = GoogleAiStudioEmbeddingsModelTests.createModel(MODEL_ID_VALUE, null, API_KEY_VALUE, getUrl(webServer));

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = GoogleAiStudioEmbeddingsModelTests.createModel(
            MODEL_ID_VALUE,
            createRandomChunkingSettings(),
            API_KEY_VALUE,
            getUrl(webServer)
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_noInputs() throws IOException {
        var model = GoogleAiStudioEmbeddingsModelTests.createModel(
            MODEL_ID_VALUE,
            createRandomChunkingSettings(),
            API_KEY_VALUE,
            getUrl(webServer)
        );

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    private void testChunkedInfer(GoogleAiStudioEmbeddingsModel model) throws IOException {

        var input = List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb"));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
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

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                input,
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));

            // first result
            {
                assertThat(results.get(0), instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, input.get(0).inputText().length()), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.0123f, -0.0123f },
                        ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }

            // second result
            {
                assertThat(results.get(1), instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, input.get(1).inputText().length()), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.0456f, -0.0456f },
                        ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getQuery(), endsWith(API_KEY_VALUE));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), Matchers.equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap, aMapWithSize(1));
            assertThat(
                requestMap.get("requests"),
                is(
                    List.of(
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", MODEL_ID_VALUE),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input.get(0).inputText()))),
                            "taskType",
                            "RETRIEVAL_DOCUMENT"
                        ),
                        Map.of(
                            "model",
                            Strings.format("%s/%s", "models", MODEL_ID_VALUE),
                            "content",
                            Map.of("parts", List.of(Map.of("text", input.get(1).inputText()))),
                            "taskType",
                            "RETRIEVAL_DOCUMENT"
                        )
                    )
                )
            );
        }
    }

    public void testInfer_ResourceNotFound() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "error": {
                        "message": "error"
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

            var model = GoogleAiStudioCompletionModelTests.createModel(MODEL_ID_VALUE, getUrl(webServer), API_KEY_VALUE);
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

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(error.getMessage(), containsString("Resource not found at "));
            assertThat(error.getMessage(), containsString("Error message: [error]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
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
        try (var service = new GoogleAiStudioService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
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

    public void testGetConfiguration() throws Exception {
        try (var service = createGoogleAiStudioService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "googleaistudio",
                       "name": "Google AI Studio",
                       "task_types": ["text_embedding", "completion"],
                       "configurations": {
                           "api_key": {
                               "description": "API Key for the provider you're connecting to.",
                               "label": "API Key",
                               "required": true,
                               "sensitive": true,
                               "updatable": true,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "completion"]
                           },
                           "rate_limit.requests_per_minute": {
                               "description": "Minimize the number of rate limit errors.",
                               "label": "Rate Limit",
                               "required": false,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["text_embedding", "completion"]
                           },
                           "model_id": {
                               "description": "ID of the LLM you're using.",
                               "label": "Model ID",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "completion"]
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

    public void testSupportsStreaming() throws IOException {
        try (var service = new GoogleAiStudioService(mock(), createWithEmptySettings(mock()), mockClusterServiceEmpty())) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }

    public static Map<String, Object> buildExpectationCompletions(List<String> completions) {
        return Map.of(
            ChatCompletionResults.COMPLETION,
            completions.stream().map(completion -> Map.of(ChatCompletionResults.Result.RESULT, completion)).collect(Collectors.toList())
        );
    }

    private static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.<Model>wrap(model -> fail("Model parsing should have failed"), e -> {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            assertThat(e.getMessage(), is(expectedMessage));
        });
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

        return new HashMap<>(
            Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)
        );
    }

    private GoogleAiStudioService createGoogleAiStudioService() {
        return new GoogleAiStudioService(
            mock(HttpRequestSender.Factory.class),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public InferenceService createInferenceService() {
        return createGoogleAiStudioService();
    }

    public void testBuildModelFromConfigAndSecrets_TextEmbedding() throws IOException {
        var model = createTestModel(TaskType.TEXT_EMBEDDING);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_Completion() throws IOException {
        var model = createTestModel(TaskType.COMPLETION);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            INFERENCE_ENTITY_ID_VALUE,
            TaskType.CHAT_COMPLETION,
            GoogleAiStudioService.NAME,
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
                        GoogleAiStudioService.NAME,
                        GoogleAiStudioService.NAME,
                        TaskType.CHAT_COMPLETION
                    )
                )

            );
        }
    }

    private Model createTestModel(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> GoogleAiStudioEmbeddingsModelTests.createModel(MODEL_ID_VALUE, API_KEY_VALUE, URL_VALUE);
            case COMPLETION -> GoogleAiStudioCompletionModelTests.createModel(MODEL_ID_VALUE, API_KEY_VALUE);
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
