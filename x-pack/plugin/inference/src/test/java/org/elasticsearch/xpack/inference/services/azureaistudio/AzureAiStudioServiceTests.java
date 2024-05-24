/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsTaskSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils.API_KEY_HEADER;
import static org.elasticsearch.xpack.inference.results.ChunkedTextEmbeddingResultsTests.asMapWithListsInsteadOfArrays;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.API_KEY_FIELD;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AzureAiStudioServiceTests extends ESTestCase {
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

    public void testParseRequestConfig_CreatesAnAzureAiStudioEmbeddingsModel() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AzureAiStudioEmbeddingsModel.class));

                var embeddingsModel = (AzureAiStudioEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().target(), is("http://target.local"));
                assertThat(embeddingsModel.getServiceSettings().provider(), is(AzureAiStudioProvider.OPENAI));
                assertThat(embeddingsModel.getServiceSettings().endpointType(), is(AzureAiStudioEndpointType.TOKEN));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
                assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", null, null, null, null),
                    getEmbeddingsTaskSettingsMap("user"),
                    getSecretSettingsMap("secret")
                ),
                Set.of(),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnAzureAiStudioChatCompletionModel() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AzureAiStudioChatCompletionModel.class));

                var completionModel = (AzureAiStudioChatCompletionModel) model;
                assertThat(completionModel.getServiceSettings().target(), is("http://target.local"));
                assertThat(completionModel.getServiceSettings().provider(), is(AzureAiStudioProvider.OPENAI));
                assertThat(completionModel.getServiceSettings().endpointType(), is(AzureAiStudioEndpointType.TOKEN));
                assertThat(completionModel.getSecretSettings().apiKey().toString(), is("secret"));
                assertNull(completionModel.getTaskSettings().temperature());
                assertTrue(completionModel.getTaskSettings().doSample());
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(
                    getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                    getChatCompletionTaskSettingsMap(null, null, true, null),
                    getSecretSettingsMap("secret")
                ),
                Set.of(),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [azureaistudio] service does not support task type [sparse_embedding]"));
                }
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                    getChatCompletionTaskSettingsMap(null, null, true, null),
                    getSecretSettingsMap("secret")
                ),
                Set.of(),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createService()) {
            var config = getRequestConfigMap(
                getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                getChatCompletionTaskSettingsMap(null, null, true, null),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [azureaistudio] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInEmbeddingServiceSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", null, null, null, null);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getEmbeddingsTaskSettingsMap("user"), getSecretSettingsMap("secret"));

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [azureaistudio] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenDimsSetByUserExistsInEmbeddingServiceSettingsMap() throws IOException {
        try (var service = createService()) {
            var config = getRequestConfigMap(
                getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", 1024, true, null, null),
                getEmbeddingsTaskSettingsMap("user"),
                getSecretSettingsMap("secret")
            );

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ValidationException.class));
                    assertThat(
                        exception.getMessage(),
                        containsString("[service_settings] does not allow the setting [dimensions_set_by_user]")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInEmbeddingTaskSettingsMap() throws IOException {
        try (var service = createService()) {
            var taskSettings = getEmbeddingsTaskSettingsMap("user");
            taskSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", null, null, null, null),
                taskSettings,
                getSecretSettingsMap("secret")
            );

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [azureaistudio] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInEmbeddingSecretSettingsMap() throws IOException {
        try (var service = createService()) {
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", null, null, null, null),
                getEmbeddingsTaskSettingsMap("user"),
                secretSettings
            );

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [azureaistudio] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInChatCompletionServiceSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getChatCompletionServiceSettingsMap("http://target.local", "openai", "token");
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                serviceSettings,
                getChatCompletionTaskSettingsMap(null, 2.0, null, null),
                getSecretSettingsMap("secret")
            );

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [azureaistudio] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInChatCompletionTaskSettingsMap() throws IOException {
        try (var service = createService()) {
            var taskSettings = getChatCompletionTaskSettingsMap(null, 2.0, null, null);
            taskSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                taskSettings,
                getSecretSettingsMap("secret")
            );

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [azureaistudio] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInChatCompletionSecretSettingsMap() throws IOException {
        try (var service = createService()) {
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                getChatCompletionTaskSettingsMap(null, 2.0, null, null),
                secretSettings
            );

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [azureaistudio] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenProviderIsNotValidForEmbeddings() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap("http://target.local", "databricks", "token", null, null, null, null);

            var config = getRequestConfigMap(serviceSettings, getEmbeddingsTaskSettingsMap("user"), getSecretSettingsMap("secret"));

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [text_embedding] task type for provider [databricks] is not available"));
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenEndpointTypeIsNotValidForEmbeddingsProvider() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap("http://target.local", "openai", "realtime", null, null, null, null);

            var config = getRequestConfigMap(serviceSettings, getEmbeddingsTaskSettingsMap("user"), getSecretSettingsMap("secret"));

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("The [realtime] endpoint type with [text_embedding] task type for provider [openai] is not available")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenEndpointTypeIsNotValidForChatCompletionProvider() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getChatCompletionServiceSettingsMap("http://target.local", "openai", "realtime");

            var config = getRequestConfigMap(
                serviceSettings,
                getChatCompletionTaskSettingsMap(null, null, null, null),
                getSecretSettingsMap("secret")
            );

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("The [realtime] endpoint type with [completion] task type for provider [openai] is not available")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.COMPLETION, config, Set.of(), modelVerificationListener);
        }
    }

    public void testParsePersistedConfig_CreatesAnAzureAiStudioEmbeddingsModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", 1024, true, 512, null),
                getEmbeddingsTaskSettingsMap("user"),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioEmbeddingsModel.class));

            var embeddingsModel = (AzureAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().target(), is("http://target.local"));
            assertThat(embeddingsModel.getServiceSettings().provider(), is(AzureAiStudioProvider.OPENAI));
            assertThat(embeddingsModel.getServiceSettings().endpointType(), is(AzureAiStudioEndpointType.TOKEN));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(true));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
        }
    }

    public void testParsePersistedConfig_CreatesAnAzureAiStudioChatCompletionModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                getChatCompletionTaskSettingsMap(1.0, 2.0, true, 512),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.COMPLETION, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioChatCompletionModel.class));

            var chatCompletionModel = (AzureAiStudioChatCompletionModel) model;
            assertThat(chatCompletionModel.getServiceSettings().target(), is("http://target.local"));
            assertThat(chatCompletionModel.getServiceSettings().provider(), is(AzureAiStudioProvider.OPENAI));
            assertThat(chatCompletionModel.getServiceSettings().endpointType(), is(AzureAiStudioEndpointType.TOKEN));
            assertThat(chatCompletionModel.getTaskSettings().temperature(), is(1.0));
            assertThat(chatCompletionModel.getTaskSettings().topP(), is(2.0));
            assertThat(chatCompletionModel.getTaskSettings().doSample(), is(true));
            assertThat(chatCompletionModel.getTaskSettings().maxNewTokens(), is(512));
        }
    }

    public void testParsePersistedConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [azureaistudio] service does not support task type [sparse_embedding]"));
                }
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                    getChatCompletionTaskSettingsMap(null, null, true, null),
                    getSecretSettingsMap("secret")
                ),
                Set.of(),
                modelVerificationListener
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                getChatCompletionTaskSettingsMap(1.0, 2.0, true, 512),
                getSecretSettingsMap("secret")
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfigWithSecrets("id", TaskType.SPARSE_EMBEDDING, config.config(), config.secrets())
            );

            assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [azureaistudio] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", 1024, true, 512, null);
            var taskSettings = getEmbeddingsTaskSettingsMap("user");
            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);
            config.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioEmbeddingsModel.class));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenExtraKeyExistsInEmbeddingServiceSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", 1024, true, 512, null);
            serviceSettings.put("extra_key", "value");

            var taskSettings = getEmbeddingsTaskSettingsMap("user");
            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioEmbeddingsModel.class));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInEmbeddingTaskSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", 1024, true, 512, null);
            var taskSettings = getEmbeddingsTaskSettingsMap("user");
            taskSettings.put("extra_key", "value");

            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioEmbeddingsModel.class));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInEmbeddingSecretSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", 1024, true, 512, null);
            var taskSettings = getEmbeddingsTaskSettingsMap("user");
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");

            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.TEXT_EMBEDDING, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioEmbeddingsModel.class));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInChatCompletionServiceSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getChatCompletionServiceSettingsMap("http://target.local", "openai", "token");
            serviceSettings.put("extra_key", "value");
            var taskSettings = getChatCompletionTaskSettingsMap(1.0, 2.0, true, 512);
            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.COMPLETION, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioChatCompletionModel.class));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInChatCompletionTaskSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getChatCompletionServiceSettingsMap("http://target.local", "openai", "token");
            var taskSettings = getChatCompletionTaskSettingsMap(1.0, 2.0, true, 512);
            taskSettings.put("extra_key", "value");
            var secretSettings = getSecretSettingsMap("secret");
            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.COMPLETION, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioChatCompletionModel.class));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInChatCompletionSecretSettingsMap() throws IOException {
        try (var service = createService()) {
            var serviceSettings = getChatCompletionServiceSettingsMap("http://target.local", "openai", "token");
            var taskSettings = getChatCompletionTaskSettingsMap(1.0, 2.0, true, 512);
            var secretSettings = getSecretSettingsMap("secret");
            secretSettings.put("extra_key", "value");
            var config = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets("id", TaskType.COMPLETION, config.config(), config.secrets());

            assertThat(model, instanceOf(AzureAiStudioChatCompletionModel.class));
        }
    }

    public void testParsePersistedConfig_WithoutSecretsCreatesEmbeddingsModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getEmbeddingsServiceSettingsMap("http://target.local", "openai", "token", 1024, true, 512, null),
                getEmbeddingsTaskSettingsMap("user"),
                Map.of()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, config.config());

            assertThat(model, instanceOf(AzureAiStudioEmbeddingsModel.class));

            var embeddingsModel = (AzureAiStudioEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().target(), is("http://target.local"));
            assertThat(embeddingsModel.getServiceSettings().provider(), is(AzureAiStudioProvider.OPENAI));
            assertThat(embeddingsModel.getServiceSettings().endpointType(), is(AzureAiStudioEndpointType.TOKEN));
            assertThat(embeddingsModel.getServiceSettings().dimensions(), is(1024));
            assertThat(embeddingsModel.getServiceSettings().dimensionsSetByUser(), is(true));
            assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), is(512));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
        }
    }

    public void testParsePersistedConfig_WithoutSecretsCreatesChatCompletionModel() throws IOException {
        try (var service = createService()) {
            var config = getPersistedConfigMap(
                getChatCompletionServiceSettingsMap("http://target.local", "openai", "token"),
                getChatCompletionTaskSettingsMap(1.0, 2.0, true, 512),
                Map.of()
            );

            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, config.config());

            assertThat(model, instanceOf(AzureAiStudioChatCompletionModel.class));

            var chatCompletionModel = (AzureAiStudioChatCompletionModel) model;
            assertThat(chatCompletionModel.getServiceSettings().target(), is("http://target.local"));
            assertThat(chatCompletionModel.getServiceSettings().provider(), is(AzureAiStudioProvider.OPENAI));
            assertThat(chatCompletionModel.getServiceSettings().endpointType(), is(AzureAiStudioEndpointType.TOKEN));
            assertThat(chatCompletionModel.getTaskSettings().temperature(), is(1.0));
            assertThat(chatCompletionModel.getTaskSettings().topP(), is(2.0));
            assertThat(chatCompletionModel.getTaskSettings().doSample(), is(true));
            assertThat(chatCompletionModel.getTaskSettings().maxNewTokens(), is(512));
        }
    }

    public void testCheckModelConfig_ForEmbeddingsModel_Works() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AzureAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testEmbeddingResultJson));

            var model = AzureAiStudioEmbeddingsModelTests.createModel(
                "id",
                getUrl(webServer),
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey",
                null,
                false,
                null,
                null,
                null,
                null
            );

            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(
                result,
                is(
                    AzureAiStudioEmbeddingsModelTests.createModel(
                        "id",
                        getUrl(webServer),
                        AzureAiStudioProvider.OPENAI,
                        AzureAiStudioEndpointType.TOKEN,
                        "apikey",
                        2,
                        false,
                        null,
                        SimilarityMeasure.DOT_PRODUCT,
                        null,
                        null
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(requestMap, Matchers.is(Map.of("input", List.of("how big"))));
        }
    }

    public void testCheckModelConfig_ForEmbeddingsModel_ThrowsIfEmbeddingSizeDoesNotMatchValueSetByUser() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AzureAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testEmbeddingResultJson));

            var model = AzureAiStudioEmbeddingsModelTests.createModel(
                "id",
                getUrl(webServer),
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey",
                3,
                true,
                null,
                null,
                null,
                null
            );

            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                exception.getMessage(),
                is(
                    "The retrieved embeddings size [2] does not match the size specified in the settings [3]. "
                        + "Please recreate the [id] configuration with the correct dimensions"
                )
            );

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(requestMap, Matchers.is(Map.of("input", List.of("how big"), "dimensions", 3)));
        }
    }

    public void testCheckModelConfig_WorksForChatCompletionsModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AzureAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testChatCompletionResultJson));

            var model = AzureAiStudioChatCompletionModelTests.createModel(
                "id",
                getUrl(webServer),
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey",
                null,
                null,
                null,
                null,
                null
            );

            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(
                result,
                is(
                    AzureAiStudioChatCompletionModelTests.createModel(
                        "id",
                        getUrl(webServer),
                        AzureAiStudioProvider.OPENAI,
                        AzureAiStudioEndpointType.TOKEN,
                        "apikey",
                        null,
                        null,
                        null,
                        AzureAiStudioChatCompletionTaskSettings.DEFAULT_MAX_NEW_TOKENS,
                        null
                    )
                )
            );
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotAzureAiStudioModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender(anyString())).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new AzureAiStudioService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                List.of(""),
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

            verify(factory, times(1)).createSender(anyString());
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testChunkedInfer_Embeddings_CallsInfer_ConvertsFloatResponse() throws IOException, URISyntaxException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AzureAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = AzureAiStudioEmbeddingsModelTests.createModel(
                "id",
                getUrl(webServer),
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey",
                null,
                false,
                null,
                null,
                "user",
                null
            );
            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                List.of("abc"),
                new HashMap<>(),
                InputType.INGEST,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT).get(0);
            assertThat(result, CoreMatchers.instanceOf(ChunkedTextEmbeddingResults.class));

            assertThat(
                asMapWithListsInsteadOfArrays((ChunkedTextEmbeddingResults) result),
                Matchers.is(
                    Map.of(
                        ChunkedTextEmbeddingResults.FIELD_NAME,
                        List.of(
                            Map.of(
                                ChunkedNlpInferenceResults.TEXT,
                                "abc",
                                ChunkedNlpInferenceResults.INFERENCE,
                                List.of((double) 0.0123f, (double) -0.0123f)
                            )
                        )
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(API_KEY_HEADER), equalTo("apikey"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(2));
            assertThat(requestMap.get("input"), Matchers.is(List.of("abc")));
            assertThat(requestMap.get("user"), Matchers.is("user"));
        }
    }

    public void testInfer_ThrowsWhenQueryIsPresent() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AzureAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testChatCompletionResultJson));

            var model = AzureAiStudioChatCompletionModelTests.createModel(
                "id",
                getUrl(webServer),
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey"
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            UnsupportedOperationException exception = expectThrows(
                UnsupportedOperationException.class,
                () -> service.infer(
                    model,
                    "should throw",
                    List.of("abc"),
                    new HashMap<>(),
                    InputType.INGEST,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                )
            );

            assertThat(exception.getMessage(), is("Azure AI Studio service does not support inference with query input"));
        }
    }

    public void testInfer_WithChatCompletionModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AzureAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testChatCompletionResultJson));

            var model = AzureAiStudioChatCompletionModelTests.createModel(
                "id",
                getUrl(webServer),
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey"
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("abc"),
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);
            assertThat(result, CoreMatchers.instanceOf(ChatCompletionResults.class));

            var completionResults = (ChatCompletionResults) result;
            assertThat(completionResults.getResults().size(), is(1));
            assertThat(completionResults.getResults().get(0).content(), is("test completion content"));
        }
    }

    public void testInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AzureAiStudioService(senderFactory, createWithEmptySettings(threadPool))) {

            String responseJson = """
                {
                    "error": {
                        "message": "Incorrect API key provided:",
                        "type": "invalid_request_error",
                        "param": null,
                        "code": "invalid_api_key"
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = AzureAiStudioEmbeddingsModelTests.createModel(
                "id",
                getUrl(webServer),
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey",
                null,
                false,
                null,
                null,
                "user",
                null
            );
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                List.of("abc"),
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Incorrect API key provided:]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    // ----------------------------------------------------------------

    private AzureAiStudioService createService() {
        return new AzureAiStudioService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
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

    private record PeristedConfigRecord(Map<String, Object> config, Map<String, Object> secrets) {}

    private PeristedConfigRecord getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {

        return new PeristedConfigRecord(
            new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, serviceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)),
            new HashMap<>(Map.of(ModelSecrets.SECRET_SETTINGS, secretSettings))
        );
    }

    private PeristedConfigRecord getPersistedConfigMap(Map<String, Object> serviceSettings, Map<String, Object> taskSettings) {

        return new PeristedConfigRecord(
            new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, serviceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)),
            null
        );
    }

    private static Map<String, Object> getEmbeddingsServiceSettingsMap(
        String target,
        String provider,
        String endpointType,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return AzureAiStudioEmbeddingsServiceSettingsTests.createRequestSettingsMap(
            target,
            provider,
            endpointType,
            dimensions,
            dimensionsSetByUser,
            maxTokens,
            similarityMeasure
        );
    }

    private static Map<String, Object> getEmbeddingsTaskSettingsMap(@Nullable String user) {
        return AzureAiStudioEmbeddingsTaskSettingsTests.getTaskSettingsMap(user);
    }

    private static HashMap<String, Object> getChatCompletionServiceSettingsMap(String target, String provider, String endpointType) {
        return AzureAiStudioChatCompletionServiceSettingsTests.createRequestSettingsMap(target, provider, endpointType);
    }

    public static Map<String, Object> getChatCompletionTaskSettingsMap(
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens
    ) {
        return AzureAiStudioChatCompletionTaskSettingsTests.getTaskSettingsMap(temperature, topP, doSample, maxNewTokens);
    }

    private static Map<String, Object> getSecretSettingsMap(String apiKey) {
        return new HashMap<>(Map.of(API_KEY_FIELD, apiKey));
    }

    private static final String testEmbeddingResultJson = """
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

    private static final String testChatCompletionResultJson = """
        {
            "choices": [
                {
                    "finish_reason": "stop",
                    "index": 0,
                    "message": {
                        "content": "test completion content",
                        "role": "assistant",
                        "tool_calls": null
                    }
                }
            ],
            "created": 1714006424,
            "id": "f92b5b4d-0de3-4152-a3c6-5aae8a74555c",
            "model": "",
            "object": "chat.completion",
            "usage": {
                "completion_tokens": 35,
                "prompt_tokens": 8,
                "total_tokens": 43
            }
        }
        """;
}
