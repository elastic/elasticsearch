/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.ChunkingSettings;
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
import org.elasticsearch.xpack.core.inference.ChunkingSettingsFeatureFlag;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModelTests;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CohereServiceTests extends ESTestCase {
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

    public void testParseRequestConfig_CreatesACohereEmbeddingsModel() throws IOException {
        try (var service = createCohereService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

                var embeddingsModel = (CohereEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(CohereEmbeddingType.FLOAT));
                MatcherAssert.assertThat(
                    embeddingsModel.getTaskSettings(),
                    is(new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START))
                );
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", CohereEmbeddingType.FLOAT),
                    getTaskSettingsMap(InputType.INGEST, CohereTruncation.START),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_ThrowsElasticsearchStatusExceptionWhenChunkingSettingsProvidedAndFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createCohereService()) {
            var serviceSettings = CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null);

            var config = getRequestConfigMap(
                serviceSettings,
                getTaskSettingsMap(null, null),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var failureListener = ActionListener.<Model>wrap((model) -> fail("Model parsing should have failed"), e -> {
                MatcherAssert.assertThat(e, instanceOf(ElasticsearchStatusException.class));
                MatcherAssert.assertThat(e.getMessage(), containsString("Model configuration contains settings"));
            });
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_CreatesACohereEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createCohereService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

                var embeddingsModel = (CohereEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(CohereEmbeddingType.FLOAT));
                MatcherAssert.assertThat(
                    embeddingsModel.getTaskSettings(),
                    is(new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START))
                );
                MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", CohereEmbeddingType.FLOAT),
                    getTaskSettingsMap(InputType.INGEST, CohereTruncation.START),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_CreatesACohereEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createCohereService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

                var embeddingsModel = (CohereEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(CohereEmbeddingType.FLOAT));
                MatcherAssert.assertThat(
                    embeddingsModel.getTaskSettings(),
                    is(new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START))
                );
                MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", CohereEmbeddingType.FLOAT),
                    getTaskSettingsMap(InputType.INGEST, CohereTruncation.START),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_OptionalTaskSettings() throws IOException {
        try (var service = createCohereService()) {

            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

                var embeddingsModel = (CohereEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(CohereEmbeddingType.FLOAT));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), equalTo(CohereEmbeddingsTaskSettings.EMPTY_SETTINGS));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", CohereEmbeddingType.FLOAT),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createCohereService()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [cohere] service does not support task type [sparse_embedding]"
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null),
                    getTaskSettingsMapEmpty(),
                    getSecretSettingsMap("secret")
                ),
                failureListener
            );
        }
    }

    private static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.<Model>wrap((model) -> fail("Model parsing should have failed"), e -> {
            MatcherAssert.assertThat(e, instanceOf(exceptionClass));
            MatcherAssert.assertThat(e.getMessage(), is(expectedMessage));
        });
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createCohereService()) {
            var config = getRequestConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [cohere] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createCohereService()) {
            var serviceSettings = CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMap(null, null), getSecretSettingsMap("secret"));

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [cohere] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createCohereService()) {
            var taskSettingsMap = getTaskSettingsMap(InputType.INGEST, null);
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [cohere] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);

        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createCohereService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null),
                getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [cohere] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_CreatesACohereEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createCohereService()) {
            var modelListener = ActionListener.<Model>wrap((model) -> {
                MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

                var embeddingsModel = (CohereEmbeddingsModel) model;
                assertNull(embeddingsModel.getServiceSettings().getCommonSettings().uri());
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(CohereEmbeddingsTaskSettings.EMPTY_SETTINGS));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, (e) -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, null, null),
                    getTaskSettingsMapEmpty(),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesACohereEmbeddingsModel() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, null)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesACohereEmbeddingsModelWithoutChunkingSettingsWhenFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, null),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, null)));
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesACohereEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, null),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, null)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesACohereEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, null)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap("secret")
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfigWithSecrets(
                    "id",
                    TaskType.SPARSE_EMBEDDING,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            MatcherAssert.assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [cohere] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesACohereEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, null, null),
                getTaskSettingsMap(InputType.INGEST, null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().getCommonSettings().uri());
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(InputType.INGEST, null)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", DenseVectorFieldMapper.ElementType.BYTE),
                getTaskSettingsMap(InputType.SEARCH, CohereTruncation.NONE),
                getSecretSettingsMap("secret")
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(CohereEmbeddingType.BYTE));
            MatcherAssert.assertThat(
                embeddingsModel.getTaskSettings(),
                is(new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE))
            );
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createCohereService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null),
                getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(CohereEmbeddingsTaskSettings.EMPTY_SETTINGS));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, null),
                getSecretSettingsMap("secret")
            );
            persistedConfig.secrets().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, null)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createCohereService()) {
            var serviceSettingsMap = CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMapEmpty(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(CohereEmbeddingsTaskSettings.EMPTY_SETTINGS));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createCohereService()) {
            var taskSettingsMap = getTaskSettingsMap(InputType.SEARCH, null);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(InputType.SEARCH, null)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesACohereEmbeddingsModel() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, CohereTruncation.NONE)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesACohereEmbeddingsModelWithoutChunkingSettingsWhenChunkingSettingsFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, CohereTruncation.NONE),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE)));
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesACohereEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, CohereTruncation.NONE),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesACohereEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                getTaskSettingsMap(null, CohereTruncation.NONE)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null),
                getTaskSettingsMapEmpty()
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config())
            );

            MatcherAssert.assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [cohere] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_CreatesACohereEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, "model", CohereEmbeddingType.FLOAT),
                getTaskSettingsMap(null, null)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().getCommonSettings().uri());
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(CohereEmbeddingType.FLOAT));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(null, null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createCohereService()) {
            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null),
                getTaskSettingsMapEmpty()
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(CohereEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createCohereService()) {
            var serviceSettingsMap = CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null, null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMap(InputType.SEARCH, null));

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(InputType.SEARCH, null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createCohereService()) {
            var taskSettingsMap = getTaskSettingsMap(InputType.INGEST, null);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", null),
                taskSettingsMap
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(CohereEmbeddingsModel.class));

            var embeddingsModel = (CohereEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new CohereEmbeddingsTaskSettings(InputType.INGEST, null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotCohereModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new CohereService(factory, createWithEmptySettings(threadPool))) {
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

    public void testInfer_SendsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new CohereEmbeddingsTaskSettings(InputType.INGEST, null),
                1024,
                1024,
                "model",
                null
            );
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

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("texts", List.of("abc"), "model", "model", "input_type", "search_document", "embedding_types", List.of("float")))
            );
        }
    }

    public void testCheckModelConfig_UpdatesDimensions() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                10,
                1,
                null,
                null
            );
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(
                result,
                // the dimension is set to 2 because there are 2 embeddings returned from the mock server
                is(
                    CohereEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "secret",
                        CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        10,
                        2,
                        null,
                        null
                    )
                )
            );
        }
    }

    public void testCheckModelConfig_UpdatesSimilarityToDotProduct_WhenItIsNull() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                10,
                1,
                null,
                null
            );
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(
                result,
                // the dimension is set to 2 because there are 2 embeddings returned from the mock server
                is(
                    CohereEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "secret",
                        CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        10,
                        2,
                        null,
                        null,
                        SimilarityMeasure.DOT_PRODUCT
                    )
                )
            );
        }
    }

    public void testCheckModelConfig_DoesNotUpdateSimilarity_WhenItIsSpecifiedAsCosine() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                10,
                1,
                null,
                null,
                SimilarityMeasure.COSINE
            );
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(
                result,
                // the dimension is set to 2 because there are 2 embeddings returned from the mock server
                is(
                    CohereEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "secret",
                        CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        10,
                        2,
                        null,
                        null,
                        SimilarityMeasure.COSINE
                    )
                )
            );
        }
    }

    public void testInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "message": "invalid api token"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                null,
                null
            );
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
            MatcherAssert.assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            MatcherAssert.assertThat(error.getMessage(), containsString("Error message: [invalid api token]"));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_SetsInputTypeToIngest_FromInferParameter_WhenTaskSettingsAreEmpty() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "model",
                null
            );
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

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("texts", List.of("abc"), "model", "model", "input_type", "search_document", "embedding_types", List.of("float")))
            );
        }
    }

    public void testInfer_SetsInputTypeToIngestFromInferParameter_WhenModelSettingIsNull_AndRequestTaskSettingsIsSearch()
        throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new CohereEmbeddingsTaskSettings(null, null),
                1024,
                1024,
                "model",
                null
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("abc"),
                false,
                CohereEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH, null),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("texts", List.of("abc"), "model", "model", "input_type", "search_document", "embedding_types", List.of("float")))
            );
        }
    }

    public void testInfer_DoesNotSetInputType_WhenNotPresentInTaskSettings_AndUnspecifiedIsPassedInRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new CohereEmbeddingsTaskSettings(null, null),
                1024,
                1024,
                "model",
                null
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            MatcherAssert.assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("texts", List.of("abc"), "model", "model", "embedding_types", List.of("float")))
            );
        }
    }

    public void testChunkedInfer_BatchesCalls() throws IOException {
        var model = CohereEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "secret",
            new CohereEmbeddingsTaskSettings(null, null),
            1024,
            1024,
            "model",
            null
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_BatchesCallsChunkingSettingsSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var model = CohereEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "secret",
            new CohereEmbeddingsTaskSettings(null, null),
            createRandomChunkingSettings(),
            1024,
            1024,
            "model",
            null
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsNotSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var model = CohereEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "secret",
            new CohereEmbeddingsTaskSettings(null, null),
            null,
            1024,
            1024,
            "model",
            null
        );

        testChunkedInfer(model);
    }

    private void testChunkedInfer(CohereEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            // Batching will call the service with 2 inputs
            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "float": [
                            [
                                0.123,
                                -0.123
                            ],
                            [
                                0.223,
                                -0.223
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            // 2 inputs
            service.chunkedInfer(
                model,
                null,
                List.of("foo", "bar"),
                new HashMap<>(),
                InputType.UNSPECIFIED,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals("foo", floatResult.chunks().get(0).matchedText());
                assertArrayEquals(new float[] { 0.123f, -0.123f }, floatResult.chunks().get(0).embedding(), 0.0f);
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals("bar", floatResult.chunks().get(0).matchedText());
                assertArrayEquals(new float[] { 0.223f, -0.223f }, floatResult.chunks().get(0).embedding(), 0.0f);
            }

            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("texts", List.of("foo", "bar"), "model", "model", "embedding_types", List.of("float")))
            );
        }
    }

    public void testChunkedInfer_BatchesCalls_Bytes() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {

            // Batching will call the service with 2 inputs
            String responseJson = """
                {
                    "id": "de37399c-5df6-47cb-bc57-e3c5680c977b",
                    "texts": [
                        "hello"
                    ],
                    "embeddings": {
                        "int8": [
                            [
                                23,
                                -23
                            ],
                            [
                                24,
                                -24
                            ]
                        ]
                    },
                    "meta": {
                        "api_version": {
                            "version": "1"
                        },
                        "billed_units": {
                            "input_tokens": 1
                        }
                    },
                    "response_type": "embeddings_by_type"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = CohereEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new CohereEmbeddingsTaskSettings(null, null),
                1024,
                1024,
                "model",
                CohereEmbeddingType.BYTE
            );
            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            // 2 inputs
            service.chunkedInfer(
                model,
                null,
                List.of("foo", "bar"),
                new HashMap<>(),
                InputType.UNSPECIFIED,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingByteResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingByteResults) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals("foo", floatResult.chunks().get(0).matchedText());
                assertArrayEquals(new byte[] { 23, -23 }, floatResult.chunks().get(0).embedding());
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingByteResults.class));
                var byteResult = (InferenceChunkedTextEmbeddingByteResults) results.get(1);
                assertThat(byteResult.chunks(), hasSize(1));
                assertEquals("bar", byteResult.chunks().get(0).matchedText());
                assertArrayEquals(new byte[] { 24, -24 }, byteResult.chunks().get(0).embedding());
            }

            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("texts", List.of("foo", "bar"), "model", "model", "embedding_types", List.of("int8")))
            );
        }
    }

    public void testDefaultSimilarity() {
        assertEquals(SimilarityMeasure.DOT_PRODUCT, CohereService.defaultSimilarity());
    }

    public void testInfer_StreamRequest() throws Exception {
        String responseJson = """
            {"event_type":"text-generation", "text":"hello"}
            {"event_type":"text-generation", "text":"there"}
            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var result = streamChatCompletion();

        InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"hello"},{"delta":"there"}]}""");
    }

    private InferenceServiceResults streamChatCompletion() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new CohereService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = CohereCompletionModelTests.createModel(getUrl(webServer), "secret", "model");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("abc"),
                true,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            return listener.actionGet(TIMEOUT);
        }
    }

    public void testInfer_StreamRequest_ErrorResponse() throws Exception {
        String responseJson = """
            { "event_type":"stream-end", "finish_reason":"ERROR", "response":{ "text": "how dare you" } }
            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var result = streamChatCompletion();

        InferenceEventsAssertion.assertThat(result)
            .hasFinishedStream()
            .hasNoEvents()
            .hasErrorWithStatusCode(500)
            .hasErrorContaining("how dare you");
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

    private Map<String, Object> getRequestConfigMap(Map<String, Object> serviceSettings, Map<String, Object> secretSettings) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings));
    }

    private CohereService createCohereService() {
        return new CohereService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }

}
