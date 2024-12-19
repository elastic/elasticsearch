/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModelTests;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankTaskSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
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
                    getTaskSettingsMap(true, InputType.INGEST),
                    getSecretSettingsMap(serviceAccountJson)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
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
                    getTaskSettingsMap(true, InputType.INGEST),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap(serviceAccountJson)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
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
                    getTaskSettingsMap(false, InputType.SEARCH),
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
                getTaskSettingsMap(true, InputType.SEARCH),
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

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMap(true, InputType.CLUSTERING), getSecretSettingsMap("{}"));

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
                getTaskSettingsMap(true, null),
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
                getTaskSettingsMap(autoTruncate, InputType.SEARCH),
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
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, InputType.SEARCH)));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
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
                getTaskSettingsMap(autoTruncate, null),
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
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
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
                getTaskSettingsMap(autoTruncate, null),
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
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, null)));
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
                getTaskSettingsMap(autoTruncate, InputType.INGEST),
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
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, InputType.INGEST)));
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
                getTaskSettingsMap(autoTruncate, null),
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
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, null)));
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
                getTaskSettingsMap(autoTruncate, InputType.CLUSTERING),
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
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, InputType.CLUSTERING)));
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
            var taskSettings = getTaskSettingsMap(autoTruncate, InputType.SEARCH);
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
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, InputType.SEARCH)));
            assertThat(embeddingsModel.getSecretSettings().serviceAccountJson().toString(), is(serviceAccountJson));
        }
    }

    public void testParsePersistedConfig_CreatesAGoogleVertexAiEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
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
                getTaskSettingsMap(autoTruncate, null),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testParsePersistedConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
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
                getTaskSettingsMap(autoTruncate, null)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(GoogleVertexAiEmbeddingsModel.class));

            var embeddingsModel = (GoogleVertexAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is(modelId));
            assertThat(embeddingsModel.getServiceSettings().location(), is(location));
            assertThat(embeddingsModel.getServiceSettings().projectId(), is(projectId));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(Boolean.TRUE));
            assertThat(embeddingsModel.getTaskSettings(), is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate, null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        try (var service = createGoogleVertexAiService()) {
            var model = GoogleVertexAiRerankModelTests.createModel(randomAlphaOfLength(10), randomNonNegativeInt());
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
        try (var service = createGoogleVertexAiService()) {
            var embeddingSize = randomNonNegativeInt();
            var model = GoogleVertexAiEmbeddingsModelTests.createModel(randomAlphaOfLength(10), randomBoolean(), similarityMeasure);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null ? SimilarityMeasure.DOT_PRODUCT : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    // testInfer tested via end-to-end notebook tests in AppEx repo

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createGoogleVertexAiService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                           "provider": "googlevertexai",
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
                                                     "label": "classification",
                                                     "value": "classification"
                                                 },
                                                 {
                                                     "label": "clustering",
                                                     "value": "clustering"
                                                 },
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
                                        },
                                        "auto_truncate": {
                                            "default_value": null,
                                            "depends_on": [],
                                            "display": "toggle",
                                            "label": "Auto Truncate",
                                            "order": 2,
                                            "required": false,
                                            "sensitive": false,
                                            "tooltip": "Specifies if the API truncates inputs longer than the maximum token length automatically.",
                                            "type": "bool",
                                            "ui_restrictions": [],
                                            "validations": [],
                                            "value": false
                                        }
                                    }
                                },
                                {
                                    "task_type": "rerank",
                                    "configuration": {
                                        "top_n": {
                                            "default_value": null,
                                            "depends_on": [],
                                            "display": "toggle",
                                            "label": "Top N",
                                            "order": 1,
                                            "required": false,
                                            "sensitive": false,
                                            "tooltip": "Specifies the number of the top n documents, which should be returned.",
                                            "type": "bool",
                                            "ui_restrictions": [],
                                            "validations": [],
                                            "value": false
                                        }
                                    }
                                }
                           ],
                           "configuration": {
                               "service_account_json": {
                                   "default_value": null,
                                   "depends_on": [],
                                   "display": "textbox",
                                   "label": "Credentials JSON",
                                   "order": 1,
                                   "required": true,
                                   "sensitive": true,
                                   "tooltip": "API Key for the provider you're connecting to.",
                                   "type": "str",
                                   "ui_restrictions": [],
                                   "validations": [],
                                   "value": null
                               },
                               "project_id": {
                                   "default_value": null,
                                   "depends_on": [],
                                   "display": "textbox",
                                   "label": "GCP Project",
                                   "order": 4,
                                   "required": true,
                                   "sensitive": false,
                                   "tooltip": "The GCP Project ID which has Vertex AI API(s) enabled. For more information on the URL, refer to the {geminiVertexAIDocs}.",
                                   "type": "str",
                                   "ui_restrictions": [],
                                   "validations": [],
                                   "value": null
                               },
                               "location": {
                                   "default_value": null,
                                   "depends_on": [],
                                   "display": "textbox",
                                   "label": "GCP Region",
                                   "order": 3,
                                   "required": true,
                                   "sensitive": false,
                                   "tooltip": "Please provide the GCP region where the Vertex AI API(s) is enabled. For more information, refer to the {geminiVertexAIDocs}.",
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
                               "model_id": {
                                   "default_value": null,
                                   "depends_on": [],
                                   "display": "textbox",
                                   "label": "Model ID",
                                   "order": 2,
                                   "required": true,
                                   "sensitive": false,
                                   "tooltip": "ID of the LLM you're using.",
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

    private static Map<String, Object> getTaskSettingsMap(Boolean autoTruncate, @Nullable InputType inputType) {
        var taskSettings = new HashMap<String, Object>();

        taskSettings.put(GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, autoTruncate);

        if (inputType != null) {
            taskSettings.put(GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        return taskSettings;
    }

    private static Map<String, Object> getTaskSettingsMap(Integer topN) {
        var taskSettings = new HashMap<String, Object>();

        taskSettings.put(GoogleVertexAiRerankTaskSettings.TOP_N, topN);

        return taskSettings;
    }
}
