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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
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
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests;
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

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
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
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty;
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

public class JinaAIServiceTests extends ESTestCase {
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

    public void testParseRequestConfig_CreatesAJinaAIEmbeddingsModel() throws IOException {
        try (var service = createJinaAIService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                    getTaskSettingsMap(InputType.INGEST),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createJinaAIService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                    getTaskSettingsMap(InputType.INGEST),
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
                MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                    getTaskSettingsMap(InputType.INGEST),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_OptionalTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {

            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), equalTo(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createJinaAIService()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [JinaAI] service does not support task type [sparse_embedding]"
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null),
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
        try (var service = createJinaAIService()) {
            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null),
                getTaskSettingsMapEmpty(),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [JinaAI] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettings = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model");
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMap(null), getSecretSettingsMap("secret"));

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [JinaAI] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [JinaAI] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);

        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null),
                getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [JinaAI] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_CreatesAJinaAIEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createJinaAIService()) {
            var modelListener = ActionListener.<Model>wrap((model) -> {
                MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                assertNull(embeddingsModel.getServiceSettings().getCommonSettings().uri());
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, (e) -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, null),
                    getTaskSettingsMapEmpty(),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(null),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null),
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
                is("Failed to parse stored model [id] for [JinaAI] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, null),
                getTaskSettingsMap(InputType.INGEST),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().getCommonSettings().uri());
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(InputType.SEARCH),
                getSecretSettingsMap("secret")
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.SEARCH)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null),
                getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            );
            persistedConfig.secrets().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettingsMap = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMapEmpty(), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = getTaskSettingsMap(InputType.SEARCH);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.SEARCH)));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(null),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null),
                getTaskSettingsMapEmpty()
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config())
            );

            MatcherAssert.assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [JinaAI] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, "model"),
                getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().getCommonSettings().uri());
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null),
                getTaskSettingsMapEmpty()
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettingsMap = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", null);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMap(InputType.SEARCH));

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.SEARCH)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model"),
                taskSettingsMap
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotJinaAIModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new JinaAIService(factory, createWithEmptySettings(threadPool))) {
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
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
                    JinaAIEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
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
                    JinaAIEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        10,
                        2,
                        null,
                        SimilarityMeasure.DOT_PRODUCT
                    )
                )
            );
        }
    }

    public void testCheckModelConfig_DoesNotUpdateSimilarity_WhenItIsSpecifiedAsCosine() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                10,
                1,
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
                    JinaAIEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "secret",
                        JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                        10,
                        2,
                        null,
                        SimilarityMeasure.COSINE
                    )
                )
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {
            var embeddingSize = randomNonNegativeInt();
            var model = JinaAIEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                randomAlphaOfLength(10),
                similarityMeasure
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null ? JinaAIService.defaultSimilarity() : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "message": "invalid api token"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new JinaAIEmbeddingsTaskSettings((InputType) null),
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
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH),
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new JinaAIEmbeddingsTaskSettings((InputType) null),
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

    public void testChunkedInfer_BatchesCallsChunkingSettingsSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "secret",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            createRandomChunkingSettings(),
            1024,
            1024,
            "model"
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "secret",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            null,
            1024,
            1024,
            "model"
        );

        testChunkedInfer(model);
    }

    private void testChunkedInfer(JinaAIEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new JinaAIEmbeddingsTaskSettings((InputType) null),
                1024,
                1024,
                "model"
            );
            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            // 2 inputs
            service.chunkedInfer(
                model,
                null,
                List.of("foo", "bar"),
                new HashMap<>(),
                InputType.UNSPECIFIED,
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
        assertEquals(SimilarityMeasure.DOT_PRODUCT, JinaAIService.defaultSimilarity());
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createJinaAIService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                            "provider": "JinaAI",
                            "task_types": [
                                 {
                                     "task_type": "text_embedding",
                                     "configuration": {
                                         "truncate": {
                                             "default_value": null,
                                             "depends_on": [],
                                             "display": "dropdown",
                                             "label": "Truncate",
                                             "options": [
                                                 {
                                                     "label": "NONE",
                                                     "value": "NONE"
                                                 },
                                                 {
                                                     "label": "START",
                                                     "value": "START"
                                                 },
                                                 {
                                                     "label": "END",
                                                     "value": "END"
                                                 }
                                             ],
                                             "order": 2,
                                             "required": false,
                                             "sensitive": false,
                                             "tooltip": "Specifies how the API handles inputs longer than the maximum token length.",
                                             "type": "str",
                                             "ui_restrictions": [],
                                             "validations": [],
                                             "value": ""
                                         },
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
                                                     "label": "clusterning",
                                                     "value": "clusterning"
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
                                         }
                                     }
                                 },
                                 {
                                     "task_type": "rerank",
                                     "configuration": {
                                         "top_n": {
                                             "default_value": null,
                                             "depends_on": [],
                                             "display": "numeric",
                                             "label": "Top N",
                                             "order": 2,
                                             "required": false,
                                             "sensitive": false,
                                             "tooltip": "The number of most relevant documents to return, defaults to the number of the documents.",
                                             "type": "int",
                                             "ui_restrictions": [],
                                             "validations": [],
                                             "value": null
                                         },
                                         "return_documents": {
                                             "default_value": null,
                                             "depends_on": [],
                                             "display": "toggle",
                                             "label": "Return Documents",
                                             "order": 1,
                                             "required": false,
                                             "sensitive": false,
                                             "tooltip": "Specify whether to return doc text within the results.",
                                             "type": "bool",
                                             "ui_restrictions": [],
                                             "validations": [],
                                             "value": false
                                         }
                                     }
                                 },
                                 {
                                     "task_type": "completion",
                                     "configuration": {}
                                 }
                            ],
                            "configuration": {
                                "api_key": {
                                    "default_value": null,
                                    "depends_on": [],
                                    "display": "textbox",
                                    "label": "API Key",
                                    "order": 1,
                                    "required": true,
                                    "sensitive": true,
                                    "tooltip": "API Key for the provider you're connecting to.",
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
        try (var service = new JinaAIService(mock(), createWithEmptySettings(mock()))) {
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
        return new JinaAIService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }

}
