/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.apache.http.HttpHeaders;
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
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.ChunkingSettingsFeatureFlag;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModelTests;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModelTests;
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

import static org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResultsTests.asMapWithListsInsteadOfArrays;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class HuggingFaceServiceTests extends ESTestCase {
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

    public void testParseRequestConfig_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.<Model>wrap((model) -> {
                assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

                var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, (e) -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret")),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsElasticsearchStatusExceptionWhenChunkingSettingsProvidedAndFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createHuggingFaceService()) {
            var config = getRequestConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret"));
            config.put("extra_key", "value");

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
                getRequestConfigMap(getServiceSettingsMap("url"), createRandomChunkingSettingsMap(), getSecretSettingsMap("secret")),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createHuggingFaceService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.<Model>wrap((model) -> {
                assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

                var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, (e) -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap("url"), createRandomChunkingSettingsMap(), getSecretSettingsMap("secret")),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createHuggingFaceService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.<Model>wrap((model) -> {
                assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

                var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, (e) -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret")),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnElserModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.<Model>wrap((model) -> {
                assertThat(model, instanceOf(HuggingFaceElserModel.class));

                var embeddingsModel = (HuggingFaceElserModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, (e) -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret")),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createHuggingFaceService()) {
            var config = getRequestConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret"));
            config.put("extra_key", "value");

            ActionListener<Model> modelVerificationActionListener = ActionListener.<Model>wrap(
                (model) -> { fail("parse request should fail"); },
                (e) -> {
                    assertThat(e, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        e.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [hugging_face] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationActionListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createHuggingFaceService()) {
            var serviceSettings = getServiceSettingsMap("url");
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getSecretSettingsMap("secret"));

            ActionListener<Model> modelVerificationActionListener = ActionListener.<Model>wrap(
                (model) -> { fail("parse request should fail"); },
                (e) -> {
                    assertThat(e, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        e.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [hugging_face] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationActionListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createHuggingFaceService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(getServiceSettingsMap("url"), secretSettingsMap);

            ActionListener<Model> modelVerificationActionListener = ActionListener.<Model>wrap(
                (model) -> { fail("parse request should fail"); },
                (e) -> {
                    assertThat(e, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        e.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [hugging_face] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationActionListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), new HashMap<>(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWithoutChunkingSettingsWhenFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url"),
                new HashMap<>(),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url"),
                new HashMap<>(),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), new HashMap<>(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnElserModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), new HashMap<>(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.SPARSE_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceElserModel.class));

            var embeddingsModel = (HuggingFaceElserModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), new HashMap<>(), getSecretSettingsMap("secret"));
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createHuggingFaceService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), new HashMap<>(), secretSettingsMap);

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), new HashMap<>(), getSecretSettingsMap("secret"));
            persistedConfig.secrets().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createHuggingFaceService()) {
            var serviceSettingsMap = getServiceSettingsMap("url");
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, new HashMap<>(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createHuggingFaceService()) {
            var taskSettingsMap = new HashMap<String, Object>();
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), taskSettingsMap, getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"));

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWithoutChunkingSettingsWhenChunkingSettingsFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), createRandomChunkingSettingsMap());

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), createRandomChunkingSettingsMap());

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"));

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnElserModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), new HashMap<>());

            var model = service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceElserModel.class));

            var embeddingsModel = (HuggingFaceElserModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"));
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createHuggingFaceService()) {
            var serviceSettingsMap = getServiceSettingsMap("url");
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap);

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createHuggingFaceService()) {
            var taskSettingsMap = new HashMap<String, Object>();
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), taskSettingsMap, null);

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(HuggingFaceEmbeddingsModel.class));

            var embeddingsModel = (HuggingFaceEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testInfer_SendsEmbeddingsRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123,
                            0.0123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret");
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

            assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { -0.0123F, 0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(1));
            assertThat(requestMap.get("inputs"), Matchers.is(List.of("abc")));
        }
    }

    public void testInfer_SendsElserRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                [
                    {
                        ".": 0.133155956864357
                    }
                ]
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceElserModelTests.createModel(getUrl(webServer), "secret");
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

            assertThat(
                result.asMap(),
                Matchers.is(
                    SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings(
                        List.of(new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(".", 0.13315596f), false))
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(1));
            assertThat(requestMap.get("inputs"), Matchers.is(List.of("abc")));
        }
    }

    public void testCheckModelConfig_IncludesMaxTokens() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1, 1, SimilarityMeasure.DOT_PRODUCT);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(
                result,
                is(HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1, 1, SimilarityMeasure.DOT_PRODUCT))
            );
        }
    }

    public void testCheckModelConfig_UsesUserSpecifiedSimilarity() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1, 2, SimilarityMeasure.COSINE);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(
                result,
                is(HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1, 1, SimilarityMeasure.COSINE))
            );
        }
    }

    public void testCheckModelConfig_DefaultsSimilarityToCosine() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1, 2, null);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(
                result,
                is(HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1, 1, SimilarityMeasure.COSINE))
            );
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = HuggingFaceElserModelTests.createModel(randomAlphaOfLength(10), randomAlphaOfLength(10));
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
        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {
            var embeddingSize = randomNonNegativeInt();
            var model = HuggingFaceEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                similarityMeasure
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null ? SimilarityMeasure.COSINE : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testChunkedInfer_CallsInfer_TextEmbedding_ConvertsFloatResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                "embeddings": [
                [
                -0.0123,
                0.0123
                ]
                ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret");
            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of("abc"),
                new HashMap<>(),
                InputType.INGEST,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT).get(0);
            assertThat(result, CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));

            MatcherAssert.assertThat(
                asMapWithListsInsteadOfArrays((InferenceChunkedTextEmbeddingFloatResults) result),
                Matchers.is(
                    Map.of(
                        InferenceChunkedTextEmbeddingFloatResults.FIELD_NAME,
                        List.of(
                            Map.of(ChunkedNlpInferenceResults.TEXT, "abc", ChunkedNlpInferenceResults.INFERENCE, List.of(-0.0123f, 0.0123f))
                        )
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(1));
            assertThat(requestMap.get("inputs"), Matchers.is(List.of("abc")));
        }
    }

    public void testChunkedInfer() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new HuggingFaceService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                [
                      [
                          0.123,
                          -0.123
                      ]
                ]
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret");
            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of("abc"),
                new HashMap<>(),
                InputType.INGEST,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(1));
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals("abc", floatResult.chunks().get(0).matchedText());
                assertArrayEquals(new float[] { 0.123f, -0.123f }, floatResult.chunks().get(0).embedding(), 0.0f);
            }

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(1));
            assertThat(requestMap.get("inputs"), Matchers.is(List.of("abc")));
        }
    }

    private HuggingFaceService createHuggingFaceService() {
        return new HuggingFaceService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }

    private Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        var requestConfigMap = getRequestConfigMap(serviceSettings, secretSettings);
        requestConfigMap.put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return requestConfigMap;
    }

    private Map<String, Object> getRequestConfigMap(Map<String, Object> serviceSettings, Map<String, Object> secretSettings) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings));
    }

}
