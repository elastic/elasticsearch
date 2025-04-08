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
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModelTests;
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
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
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
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(JinaAIEmbeddingType.FLOAT));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
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
                MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(JinaAIEmbeddingType.FLOAT));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
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
                MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

                var embeddingsModel = (JinaAIEmbeddingsModel) model;
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
                MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getEmbeddingType(), is(JinaAIEmbeddingType.BIT));
                MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
                MatcherAssert.assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.BIT),
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
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedTaskType() throws IOException {
        try (var service = createJinaAIService()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [jinaai] service does not support task type [sparse_embedding]"
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                    JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettings = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                serviceSettings,
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
            );
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, failureListener);

        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createJinaAIService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Model configuration contains settings [{extra_key=value}] unknown to the [jinaai] service"
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
                    JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, "model", JinaAIEmbeddingType.FLOAT),
                    JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                    getSecretSettingsMap("secret")
                ),
                modelListener
            );

        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModel() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "oldmodel", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
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
                is("Failed to parse stored model [id] for [jinaai] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAJinaAIEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST),
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
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
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
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
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
            var serviceSettingsMap = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
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
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            MatcherAssert.assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null)
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null)
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model_old", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty()
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config())
            );

            MatcherAssert.assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [jinaai] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_CreatesAJinaAIEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createJinaAIService()) {
            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null)
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
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty()
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var serviceSettingsMap = JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            MatcherAssert.assertThat(model, instanceOf(JinaAIEmbeddingsModel.class));

            var embeddingsModel = (JinaAIEmbeddingsModel) model;
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().uri().toString(), is("url"));
            MatcherAssert.assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("model"));
            MatcherAssert.assertThat(embeddingsModel.getTaskSettings(), is(new JinaAIEmbeddingsTaskSettings(InputType.SEARCH)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createJinaAIService()) {
            var taskSettingsMap = JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                JinaAIEmbeddingsServiceSettingsTests.getServiceSettingsMap("url", "model", JinaAIEmbeddingType.FLOAT),
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
                is("The internal model was invalid, please delete the service [service_name] with id [model_id] and add it again.")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {
            var embeddingSize = randomNonNegativeInt();
            var embeddingType = randomFrom(JinaAIEmbeddingType.values());
            var model = JinaAIEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                randomAlphaOfLength(10),
                similarityMeasure,
                embeddingType
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "model",
                null,
                JinaAIEmbeddingType.FLOAT
            );
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
            MatcherAssert.assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            MatcherAssert.assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_Rerank_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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
            MatcherAssert.assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            MatcherAssert.assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_Embedding_Get_Response_Ingest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "jina-clip-v2",
                null,
                JinaAIEmbeddingType.FLOAT
            );
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
                is(Map.of("input", List.of("abc"), "model", "jina-clip-v2", "task", "retrieval.passage", "embedding_type", "float"))
            );
        }
    }

    public void testInfer_Embedding_Get_Response_Search() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "jina-clip-v2",
                null,
                JinaAIEmbeddingType.FLOAT
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.SEARCH,
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
                is(Map.of("input", List.of("abc"), "model", "jina-clip-v2", "task", "retrieval.query", "embedding_type", "float"))
            );
        }
    }

    public void testInfer_Embedding_Get_Response_clustering() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {"model":"jina-clip-v2","object":"list","usage":{"total_tokens":5,"prompt_tokens":5},
                "data":[{"object":"embedding","index":0,"embedding":[0.123, -0.123]}]}
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "jina-clip-v2",
                null,
                JinaAIEmbeddingType.FLOAT
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.CLUSTERING,
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
                is(Map.of("input", List.of("abc"), "model", "jina-clip-v2", "task", "separation", "embedding_type", "float"))
            );
        }
    }

    public void testInfer_Embedding_Get_Response_NullInputType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "jina-clip-v2",
                null,
                JinaAIEmbeddingType.FLOAT
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                null,
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
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "jina-clip-v2", "embedding_type", "float")));
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {
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

            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {
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

            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {
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
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
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

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {
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
            MatcherAssert.assertThat(webServer.requests(), hasSize(1));
            MatcherAssert.assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaType())
            );
            MatcherAssert.assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
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

    public void testInfer_Embedding_DoesNotSetInputType_WhenNotPresentInTaskSettings_AndUnspecifiedIsPassedInRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = JinaAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "secret",
                new JinaAIEmbeddingsTaskSettings((InputType) null),
                1024,
                1024,
                "jina-clip-v2",
                null,
                JinaAIEmbeddingType.FLOAT
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
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
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "jina-clip-v2", "embedding_type", "float")));
        }
    }

    public void test_Embedding_ChunkedInfer_BatchesCallsChunkingSettingsSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "secret",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            createRandomChunkingSettings(),
            1024,
            1024,
            "jina-clip-v2",
            JinaAIEmbeddingType.FLOAT
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    public void test_Embedding_ChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = JinaAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "secret",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            null,
            1024,
            1024,
            "jina-clip-v2",
            JinaAIEmbeddingType.FLOAT
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    private void test_Embedding_ChunkedInfer_BatchesCalls(JinaAIEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new JinaAIService(senderFactory, createWithEmptySettings(threadPool))) {

            // Batching will call the service with 2 input
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
                assertThat(results.get(0), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.123f, -0.123f },
                    ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.223f, -0.223f },
                    ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
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
                is(Map.of("input", List.of("a", "bb"), "model", "jina-clip-v2", "embedding_type", "float"))
            );
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
                                    "description": "The number of dimensions the resulting embeddings should have. For more information refer to https://api.jina.ai/redoc#tag/embeddings/operation/create_embedding_v1_embeddings_post.",
                                    "label": "Dimensions",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
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
