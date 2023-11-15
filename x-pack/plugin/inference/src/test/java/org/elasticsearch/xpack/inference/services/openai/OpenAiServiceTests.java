/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.http.HttpHeaders;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
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
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.external.http.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class OpenAiServiceTests extends ESTestCase {
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

    public void testParseRequestConfig_CreatesAnOpenAiEmbeddingsModel() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var model = service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    getServiceSettingsMap("url", "org"),
                    getTaskSettingsMap("model", "user"),
                    getSecretSettingsMap("secret")
                ),
                Set.of()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getTaskSettings().model(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parseRequestConfig(
                    "id",
                    TaskType.SPARSE_EMBEDDING,
                    getRequestConfigMap(
                        getServiceSettingsMap("url", "org"),
                        getTaskSettingsMap("model", "user"),
                        getSecretSettingsMap("secret")
                    ),
                    Set.of()
                )
            );

            assertThat(thrownException.getMessage(), is("The [openai] service does not support task type [sparse_embedding]"));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var config = getRequestConfigMap(
                getServiceSettingsMap("url", "org"),
                getTaskSettingsMap("model", "user"),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var serviceSettings = getServiceSettingsMap("url", "org");
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMap("model", "user"), getSecretSettingsMap("secret"));

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var taskSettingsMap = getTaskSettingsMap("model", "user");
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(getServiceSettingsMap("url", "org"), taskSettingsMap, getSecretSettingsMap("secret"));

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(getServiceSettingsMap("url", "org"), getTaskSettingsMap("model", "user"), secretSettingsMap);

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiEmbeddingsModelWithoutUserUrlOrganization() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var model = service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap(null, null), getTaskSettingsMap("model", null), getSecretSettingsMap("secret")),
                Set.of()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getTaskSettings().model(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModel() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url", "org"),
                getTaskSettingsMap("model", "user"),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config(), persistedConfig.secrets());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getTaskSettings().model(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url", "org"),
                getTaskSettingsMap("model", "user"),
                getSecretSettingsMap("secret")
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config(), persistedConfig.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [openai] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModelWithoutUserUrlOrganization() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(null, null),
                getTaskSettingsMap("model", null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config(), persistedConfig.secrets());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getTaskSettings().model(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url", "org"),
                getTaskSettingsMap("model", "user"),
                getSecretSettingsMap("secret")
            );
            persistedConfig.config().put("extra_key", "value");

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config(), persistedConfig.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParsePersistedConfig_ThrowsWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url", "org"),
                getTaskSettingsMap("model", "user"),
                secretSettingsMap
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config(), persistedConfig.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParsePersistedConfig_ThrowsWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url", "org"),
                getTaskSettingsMap("model", "user"),
                getSecretSettingsMap("secret")
            );
            persistedConfig.secrets.put("extra_key", "value");

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config(), persistedConfig.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParsePersistedConfig_ThrowsWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var serviceSettingsMap = getServiceSettingsMap("url", "org");
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                getTaskSettingsMap("model", "user"),
                getSecretSettingsMap("secret")
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config(), persistedConfig.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testParsePersistedConfig_ThrowsWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (
            var service = new OpenAiService(
                new SetOnce<>(mock(HttpRequestSenderFactory.class)),
                new SetOnce<>(createWithEmptySettings(threadPool))
            )
        ) {
            var taskSettingsMap = getTaskSettingsMap("model", "user");
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("url", "org"),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config(), persistedConfig.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
            );
        }
    }

    public void testStart_InitializesTheSender() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSenderFactory.class);
        when(factory.createSender(anyString())).thenReturn(sender);

        try (var service = new OpenAiService(new SetOnce<>(factory), new SetOnce<>(createWithEmptySettings(threadPool)))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);

            listener.actionGet(TIMEOUT);
            verify(sender, times(1)).start();
            verify(factory, times(1)).createSender(anyString());
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testStart_CallingStartTwiceKeepsSameSenderReference() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSenderFactory.class);
        when(factory.createSender(anyString())).thenReturn(sender);

        try (var service = new OpenAiService(new SetOnce<>(factory), new SetOnce<>(createWithEmptySettings(threadPool)))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            verify(factory, times(1)).createSender(anyString());
            verify(sender, times(2)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsErrorWhenModelIsNotOpenAiModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSenderFactory.class);
        when(factory.createSender(anyString())).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new OpenAiService(new SetOnce<>(factory), new SetOnce<>(createWithEmptySettings(threadPool)))) {
            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            service.infer(mockModel, List.of(""), new HashMap<>(), listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("The internal model was invalid, please delete the service [service_name] with id [model_id] and add it again.")
            );

            verify(factory, times(1)).createSender(anyString());
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_SendsRequest() throws IOException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var service = new OpenAiService(new SetOnce<>(senderFactory), new SetOnce<>(createWithEmptySettings(threadPool)))) {

            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      }
                  ],
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            service.infer(model, List.of("abc"), new HashMap<>(), listener);

            InferenceResults result = listener.actionGet(TIMEOUT).get(0);

            assertThat(
                result.asMap(),
                Matchers.is(
                    Map.of(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        List.of(Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.0123F, -0.0123F)))
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(3));
            assertThat(requestMap.get("input"), Matchers.is(List.of("abc")));
            assertThat(requestMap.get("model"), Matchers.is("model"));
            assertThat(requestMap.get("user"), Matchers.is("user"));
        }
    }

    private static Model getInvalidModel(String modelId, String serviceName) {
        var mockConfigs = mock(ModelConfigurations.class);
        when(mockConfigs.getModelId()).thenReturn(modelId);
        when(mockConfigs.getService()).thenReturn(serviceName);

        var mockModel = mock(Model.class);
        when(mockModel.getConfigurations()).thenReturn(mockConfigs);

        return mockModel;
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

    private PeristedConfig getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {

        return new PeristedConfig(
            new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, serviceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)),
            new HashMap<>(Map.of(ModelSecrets.SECRET_SETTINGS, secretSettings))
        );
    }

    private record PeristedConfig(Map<String, Object> config, Map<String, Object> secrets) {}
}
