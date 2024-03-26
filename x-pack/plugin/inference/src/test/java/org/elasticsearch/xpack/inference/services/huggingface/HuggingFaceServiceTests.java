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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModelTests;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModelTests;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectation;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
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
                Set.of(),
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
                Set.of(),
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

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationActionListener);
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

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationActionListener);
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

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationActionListener);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret"));

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

    public void testParsePersistedConfigWithSecrets_CreatesAnElserModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret"));

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
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret"));
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

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), secretSettingsMap);

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
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"), getSecretSettingsMap("secret"));
            persistedConfig.secrets.put("extra_key", "value");

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

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getSecretSettingsMap("secret"));

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

    public void testParsePersistedConfig_CreatesAnElserModel() throws IOException {
        try (var service = createHuggingFaceService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("url"));

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
            service.infer(model, List.of("abc"), new HashMap<>(), InputType.INGEST, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), Matchers.is(buildExpectation(List.of(List.of(-0.0123F, 0.0123F)))));
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
            service.infer(model, List.of("abc"), new HashMap<>(), InputType.INGEST, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(
                result.asMap(),
                Matchers.is(
                    SparseEmbeddingResultsTests.buildExpectation(
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

            var model = HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result, is(HuggingFaceEmbeddingsModelTests.createModel(getUrl(webServer), "secret", 1, 1)));
        }
    }

    private HuggingFaceService createHuggingFaceService() {
        return new HuggingFaceService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }

    private Map<String, Object> getRequestConfigMap(Map<String, Object> serviceSettings, Map<String, Object> secretSettings) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings));
    }

    private HuggingFaceServiceTests.PeristedConfig getPersistedConfigMap(Map<String, Object> serviceSettings) {
        return getPersistedConfigMap(serviceSettings, Map.of(), null);
    }

    private HuggingFaceServiceTests.PeristedConfig getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secretSettings
    ) {
        return getPersistedConfigMap(serviceSettings, Map.of(), secretSettings);
    }

    private HuggingFaceServiceTests.PeristedConfig getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {

        var secrets = secretSettings == null ? null : new HashMap<String, Object>(Map.of(ModelSecrets.SECRET_SETTINGS, secretSettings));

        return new HuggingFaceServiceTests.PeristedConfig(
            new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, serviceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)),
            secrets
        );
    }

    private record PeristedConfig(Map<String, Object> config, Map<String, Object> secrets) {}
}
