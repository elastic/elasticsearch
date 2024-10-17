/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.ChunkingSettingsFeatureFlag;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankTaskSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GoogleVertexAiServiceTests extends ESTestCase {

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

    public void testParseRequestConfig_CreatesGoogleVertexAiEmbeddingsModel() throws IOException {
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

                var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;

                assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
                assertThat(embeddingsModel.getServiceSettings().location(), is(location));
                assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
                assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
            }, e -> fail("Model parsing should succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(
                        Map.of(
                            ServiceFields.MODEL_ID,
                            modelId,
                            GoogleVertexAiServiceFields.LOCATION,
                            location,
                            GoogleVertexAiServiceFields.PROJECT_ID,
                            projectId
                        )
                    ),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(serviceAccountJson)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsElasticsearchStatusExceptionWhenChunkingSettingsProvidedAndFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createGoogleVertexAiService()) {
            var config = getRequestConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "model",
                        GoogleVertexAiServiceFields.LOCATION,
                        "location",
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        "project"
                    )
                ),
                getTaskSettingsMap(true),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("{}")
            );

            var failureListener = ActionListener.<Model>wrap(model -> fail("Expected exception, but got model: " + model), exception -> {
                assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                assertThat(exception.getMessage(), containsString("Model configuration contains settings"));
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

                var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;

                assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
                assertThat(embeddingsModel.getServiceSettings().location(), is(location));
                assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
            }, e -> fail("Model parsing should succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(
                        Map.of(
                            ServiceFields.MODEL_ID,
                            modelId,
                            GoogleVertexAiServiceFields.LOCATION,
                            location,
                            GoogleVertexAiServiceFields.PROJECT_ID,
                            projectId
                        )
                    ),
                    new HashMap<>(Map.of()),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap(serviceAccountJson)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

                var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;

                assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
                assertThat(embeddingsModel.getServiceSettings().location(), is(location));
                assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
            }, e -> fail("Model parsing should succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(
                        Map.of(
                            ServiceFields.MODEL_ID,
                            modelId,
                            GoogleVertexAiServiceFields.LOCATION,
                            location,
                            GoogleVertexAiServiceFields.PROJECT_ID,
                            projectId
                        )
                    ),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(serviceAccountJson)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesGoogleVertexAiRerankModel() throws IOException {
        var projectId = "project";
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(GoogleVertexAiRerankModel.class));

                var rerankModel = (GoogleVertexAiRerankModel) model;

                assertThat(rerankModel.getServiceSettings().projectId(), is(projectId));
                assertThat(rerankModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
            }, e -> fail("Model parsing should succeeded, but failed: " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.RERANK,
                getRequestConfigMap(
                    new HashMap<>(Map.of(GoogleVertexAiServiceFields.PROJECT_ID, projectId)),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap(serviceAccountJson)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createGoogleVertexAiService()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [googlevertexai] service does not support task type [sparse_embedding]"
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    new HashMap<>(
                        Map.of(
                            ServiceFields.MODEL_ID,
                            "model",
                            GoogleVertexAiServiceFields.LOCATION,
                            "location",
                            GoogleVertexAiServiceFields.PROJECT_ID,
                            "project"
                        )
                    ),
                    new HashMap<>(Map.of()),
                    getSecretSettingsMap("{}")
                ),
                failureListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createGoogleVertexAiService()) {
            var config = getRequestConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "model",
                        GoogleVertexAiServiceFields.LOCATION,
                        "location",
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        "project"
                    )
                ),
                getTaskSettingsMap(true),
                getSecretSettingsMap("{}")
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googlevertexai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createGoogleVertexAiService()) {
            Map<String, Object> serviceSettings = new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    "model",
                    GoogleVertexAiServiceFields.LOCATION,
                    "location",
                    GoogleVertexAiServiceFields.PROJECT_ID,
                    "project"
                )
            );
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMap(true), getSecretSettingsMap("{}"));

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googlevertexai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createGoogleVertexAiService()) {
            Map<String, Object> taskSettingsMap = new HashMap<>();
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "model",
                        GoogleVertexAiServiceFields.LOCATION,
                        "location",
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        "project"
                    )
                ),
                taskSettingsMap,
                getSecretSettingsMap("{}")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googlevertexai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createGoogleVertexAiService()) {
            Map<String, Object> secretSettings = getSecretSettingsMap("{}");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "model",
                        GoogleVertexAiServiceFields.LOCATION,
                        "location",
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        "project"
                    )
                ),
                getTaskSettingsMap(true),
                secretSettings
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [googlevertexai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesGoogleVertexAiEmbeddingsModel() throws IOException {
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                getSecretSettingsMap(serviceAccountJson)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleVertexAiEmbeddingsModelWithoutChunkingSettingsWhenFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap(serviceAccountJson)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap(serviceAccountJson)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                getSecretSettingsMap(serviceAccountJson)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesGoogleVertexAiRerankModel() throws IOException {
        var projectId = "project";
        var topN = 1;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(Map.of(GoogleVertexAiServiceFields.PROJECT_ID, projectId)),
                getTaskSettingsMap(topN),
                getSecretSettingsMap(serviceAccountJson)
            );

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.RERANK, persistedConfig.config(), persistedConfig.secrets());

            assertThat(model, instanceOf(GoogleVertexAiRerankModel.class));

            var rerankModel = (GoogleVertexAiRerankModel) model;
            assertThat(rerankModel.getServiceSettings().projectId(), is(projectId));
            assertThat(rerankModel.getTaskSettings(), is(new GoogleVertexAiRerankTaskSettings(topN)));
            assertThat(rerankModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                getSecretSettingsMap(serviceAccountJson)
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var secretSettingsMap = getSecretSettingsMap(serviceAccountJson);
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "model",
                        GoogleVertexAiServiceFields.LOCATION,
                        "location",
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        "project",
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var serviceSettingsMap = new HashMap<String, Object>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    "model",
                    GoogleVertexAiServiceFields.LOCATION,
                    "location",
                    GoogleVertexAiServiceFields.PROJECT_ID,
                    "project",
                    GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                    true
                )
            );
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                getTaskSettingsMap(autoTruncate),
                getSecretSettingsMap(serviceAccountJson)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;
        var serviceAccountJson = """
            {
                "some json"
            }
            """;

        try (var service = createGoogleVertexAiService()) {
            var taskSettings = getTaskSettingsMap(autoTruncate);
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "model",
                        GoogleVertexAiServiceFields.LOCATION,
                        "location",
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        "project",
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                taskSettings,
                getSecretSettingsMap(serviceAccountJson)
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfig_CreatesAGoogleVertexAiEmbeddingsModelWithoutChunkingSettingsWhenFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
        var projectId = "project";
        var location = "location";
        var modelId = "model";
        var autoTruncate = true;

        try (var service = createGoogleVertexAiService()) {
            var persistedConfig = getPersistedConfigMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        modelId,
                        GoogleVertexAiServiceFields.LOCATION,
                        location,
                        GoogleVertexAiServiceFields.PROJECT_ID,
                        projectId,
                        GoogleVertexAiEmbeddingsServiceSettings.DIMENSIONS_SET_BY_USER,
                        true
                    )
                ),
                getTaskSettingsMap(autoTruncate)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    // testInfer tested via end-to-end notebook tests in AppEx repo

    private GoogleVertexAiService createGoogleVertexAiService() {
        return new GoogleVertexAiService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
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

    private static Map<String, Object> getSecretSettingsMap(String serviceAccountJson) {
        return new HashMap<>(Map.of(GoogleVertexAiSecretSettings.SERVICE_ACCOUNT_JSON, serviceAccountJson));
    }

    private static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.<Model>wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            assertThat(e.getMessage(), CoreMatchers.is(expectedMessage));
        });
    }

    private static Map<String, Object> getTaskSettingsMap(Boolean autoTruncate) {
        var taskSettings = new HashMap<String, Object>();

        taskSettings.put(GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, autoTruncate);

        return taskSettings;
    }

    private static Map<String, Object> getTaskSettingsMap(Integer topN) {
        var taskSettings = new HashMap<String, Object>();

        taskSettings.put(GoogleVertexAiRerankTaskSettings.TOP_N, topN);

        return taskSettings;
    }
}
