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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModelTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
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
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    public static final String DEFAULT_EMBEDDING_URL = "https://api.jina.ai/v1/embeddings";
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

    public void testParseRequestConfig_CreatesAJinaAIEmbeddingsModel() throws IOException {
        try (var service = createJinaAIService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(JinaAIEmbeddingType.FLOAT));
                assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                    JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createJinaAIService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(JinaAIEmbeddingType.FLOAT));
                assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                    JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createJinaAIService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(JinaAIEmbeddingType.BIT));
                assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.BIT),
                    JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_OptionalTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {

            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getTaskSettings(), equalTo(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedTaskType() throws IOException {
        try (var service = createJinaAIService()) {
            var failureListener = getModelListenerForStatusException("The [jinaai] service does not support task type [sparse_embedding]");

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                    Map.of(),
                    getSecretSettingsMap("secret")
                ),
                failureListener
            );
        }
    }

    private static ActionListener<Model> getModelListenerForStatusException(String expectedMessage) {
        return ActionListener.wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, instanceOf(ElasticsearchStatusException.class));
            assertThat(e.getMessage(), is(expectedMessage));
        });
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettings = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, Map.of(), getSecretSettingsMap("secret"));

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);

        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                secretSettingsMap
            );

            var failureListener = getModelListenerForStatusException(
                "Configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("oldmodel", JinaAIEmbeddingType.FLOAT),
                Map.of(),
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

            assertThat(thrownException.getMessage(), containsString("Failed to parse stored model [id] for [jinaai] service"));
            assertThat(thrownException.getMessage(), containsString("The [jinaai] service does not support task type [sparse_embedding]"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH),
                getSecretSettingsMap("secret")
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.SEARCH)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                getSecretSettingsMap("secret")
            );
            persistedConfig.secrets().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettingsMap = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, Map.of(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.SEARCH)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of(),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model_old", JinaAIEmbeddingType.FLOAT),
                Map.of()
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config())
            );

            assertThat(thrownException.getMessage(), containsString("Failed to parse stored model [id] for [jinaai] service"));
            assertThat(thrownException.getMessage(), containsString("The [jinaai] service does not support task type [sparse_embedding]"));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                Map.of()
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettingsMap = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.SEARCH)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("model", JinaAIEmbeddingType.FLOAT),
                taskSettingsMap
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());
            assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is(DEFAULT_EMBEDDING_URL));
            assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
            assertNull(embeddingsModel.getSecretSettings());
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

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
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

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(null);
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel() throws IOException {
        testUpdateModelWithEmbeddingDetails_Successful(randomFrom(SimilarityMeasure.values()));
    }

    private void testUpdateModelWithEmbeddingDetails_Successful(SimilarityMeasure similarityMeasure) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            var embeddingSize = randomNonNegativeInt();
            var embeddingType = randomFrom(JinaAIEmbeddingType.values());
            var model = JinaAIEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                RateLimitSettingsTests.createRandom(),
                similarityMeasure,
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                embeddingType,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                null,
                randomAlphaOfLength(10),
                false
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null
                ? JinaAIService.defaultSimilarity(embeddingType)
                : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testInfer_Embedding_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = JinaAIEmbeddingsModelTests.createModel(getUrl(webServer), "model", "secret");
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

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_TextEmbedding_Get_Response_Ingest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "jina-clip-v2",
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

            String apiKey = "apiKey";
            int dimensions = 1024;
            String modelName = "jina-clip-v2";
            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                apiKey,
                dimensions
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
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + apiKey));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        input,
                        "model",
                        modelName,
                        "task",
                        "retrieval.passage",
                        "embedding_type",
                        "float",
                        "dimensions",
                        dimensions
                    )
                )
            );
        }
    }

    public void testInfer_TextEmbedding_Get_Response_Search() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "jina-clip-v2",
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

            String apiKey = "apiKey";
            int dimensions = 1024;
            String modelName = "jina-clip-v2";
            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                apiKey,
                dimensions
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
                InputType.SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + apiKey));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        input,
                        "model",
                        modelName,
                        "task",
                        "retrieval.query",
                        "embedding_type",
                        "float",
                        "dimensions",
                        dimensions
                    )
                )
            );
        }
    }

    public void testInfer_TextEmbedding_Get_Response_clustering() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {"model":"jina-clip-v2","object":"list","usage":{"total_tokens":5,"prompt_tokens":5},
                "data":[{"object":"embedding","index":0,"embedding":[0.123, -0.123]}]}
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            String apiKey = "apiKey";
            int dimensions = 1024;
            String modelName = "jina-clip-v2";
            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                apiKey,
                dimensions
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
                InputType.CLUSTERING,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + apiKey));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(Map.of("input", input, "model", modelName, "task", "separation", "embedding_type", "float", "dimensions", dimensions))
            );
        }
    }

    public void testInfer_TextEmbedding_Get_Response_NullInputType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "jina-clip-v2",
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

            String apiKey = "apiKey";
            int dimensions = 1024;
            String modelName = "jina-clip-v2";
            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                apiKey,
                dimensions
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            List<String> input = List.of("abc");
            service.infer(model, null, null, null, input, false, new HashMap<>(), null, InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + apiKey));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap, is(Map.of("input", input, "model", modelName, "embedding_type", "float", "dimensions", dimensions)));
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

            var result = listener.actionGet(TIMEOUT);
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

            var result = listener.actionGet(TIMEOUT);
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

            var result = listener.actionGet(TIMEOUT);
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

            var result = listener.actionGet(TIMEOUT);
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

    public void testInfer_TextEmbedding_DoesNotSetInputType_WhenNotPresentInTaskSettings_AndUnspecifiedIsPassedInRequest()
        throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "jina-clip-v2",
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

            String apiKey = "apiKey";
            int dimensions = 1024;
            String modelName = "jina-clip-v2";

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                modelName,
                JinaAIEmbeddingType.FLOAT,
                new JinaAIEmbeddingsTaskSettings(null, null),
                apiKey,
                dimensions
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
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + apiKey));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap, is(Map.of("input", input, "model", modelName, "embedding_type", "float", "dimensions", dimensions)));
        }
    }

    public void test_Embedding_ChunkedInfer_BatchesCallsChunkingSettingsSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-clip-v2",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            createRandomChunkingSettings(),
            "secret"
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    public void test_Embedding_ChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-clip-v2",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            "secret"
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    public void test_Embedding_ChunkedInfer_LateChunkingEnabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-clip-v2",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true),
            "secret"
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    public void test_Embedding_ChunkedInfer_LateChunkingDisabled() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "jina-clip-v2",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST, false),
            "secret"
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    public void test_Embedding_ChunkedInfer_noInputs() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var model = JinaAIEmbeddingsModelTests.createModel(getUrl(webServer), "jina-clip-v2", "secret");

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

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    private void test_Embedding_ChunkedInfer_BatchesCalls(JinaAIEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            queueResponsesForChunkedInfer(model.getTaskSettings().getLateChunking());

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            // 2 input
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));
            {
                assertThat(results.getFirst(), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.getFirst();
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().getFirst().offset());
                assertThat(floatResult.chunks().getFirst().embedding(), Matchers.instanceOf(EmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.123f, -0.123f },
                    ((EmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values(),
                    0.0f
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().getFirst().offset());
                assertThat(floatResult.chunks().getFirst().embedding(), Matchers.instanceOf(EmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.223f, -0.223f },
                    ((EmbeddingFloatResults.Embedding) floatResult.chunks().getFirst().embedding()).values(),
                    0.0f
                );
            }
        }
    }

    private void queueResponsesForChunkedInfer(Boolean lateChunking) {
        if (Boolean.TRUE.equals(lateChunking)) {
            var responseJson = """
                {
                    "model": "jina-clip-v2",
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
            var responseJson2 = """
                {
                    "model": "jina-clip-v2",
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
                                0.223,
                                -0.223
                            ]
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson2));
        } else {
            var responseJson = """
                {
                    "model": "jina-clip-v2",
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
                        },
                        {
                            "object": "embedding",
                            "index": 1,
                            "embedding": [
                                0.223,
                                -0.223
                            ]
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
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
                            "task_types": ["text_embedding", "rerank"],
                            "configurations": {
                                "api_key": {
                                    "description": "API Key for the provider you're connecting to.",
                                    "label": "API Key",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank"]
                                },
                                "dimensions": {
                                    "description": "The number of dimensions the resulting embeddings should have. For more information refer to https://api.jina.ai/docs#tag/embeddings/operation/create_embedding_v1_embeddings_post.",
                                    "label": "Dimensions",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding"]
                                },
                                "embedding_type": {
                                    "description": "The type of embedding to return. One of [float, bit, binary]. bit and binary are equivalent and are encoded as bytes with signed int8 precision.",
                                    "label": "Embedding type",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "default_value": "float",
                                    "type": "str",
                                    "supported_task_types": ["text_embedding"]
                                },
                                "similarity": {
                                    "description": "The similarity measure. One of [cosine, dot_product, l2_norm]. For float embeddings, the default similarity is dot_product. For bit and binary embeddings, the default similarity is l2_norm.",
                                    "label": "Similarity",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding"]
                                },
                                "model_id": {
                                    "description": "The name of the model to use for the inference task.",
                                    "label": "Model ID",
                                    "required": true,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "rerank"]
                                },
                                "rate_limit.requests_per_minute": {
                                    "description": "Minimize the number of rate limit errors.",
                                    "label": "Rate Limit",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding", "rerank"]
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
}
