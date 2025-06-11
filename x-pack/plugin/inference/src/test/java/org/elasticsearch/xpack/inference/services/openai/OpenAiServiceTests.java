/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
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
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.InferenceEventsAssertion;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createCompletionModel;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
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
        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

                var embeddingsModel = (OpenAiEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
                assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    getServiceSettingsMap("model", "url", "org"),
                    getTaskSettingsMap("user"),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiChatCompletionsModel() throws IOException {
        var url = "url";
        var organization = "org";
        var model = "model";
        var user = "user";
        var secret = "secret";

        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(OpenAiChatCompletionModel.class));

                var completionsModel = (OpenAiChatCompletionModel) m;

                assertThat(completionsModel.getServiceSettings().uri().toString(), is(url));
                assertThat(completionsModel.getServiceSettings().organizationId(), is(organization));
                assertThat(completionsModel.getServiceSettings().modelId(), is(model));
                assertThat(completionsModel.getTaskSettings().user(), is(user));
                assertThat(completionsModel.getSecretSettings().apiKey().toString(), is(secret));

            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(
                    getServiceSettingsMap(model, url, organization),
                    getTaskSettingsMap(user),
                    getSecretSettingsMap(secret)
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [openai] service does not support task type [sparse_embedding]"));
                }
            );

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    getServiceSettingsMap("model", "url", "org"),
                    getTaskSettingsMap("user"),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createOpenAiService()) {
            var config = getRequestConfigMap(
                getServiceSettingsMap("model", "url", "org"),
                getTaskSettingsMap("user"),
                getSecretSettingsMap("secret")
            );
            config.put("extra_key", "value");

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Configuration contains settings [{extra_key=value}] unknown to the [openai] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createOpenAiService()) {
            var serviceSettings = getServiceSettingsMap("model", "url", "org");
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, getTaskSettingsMap("user"), getSecretSettingsMap("secret"));

            ActionListener<Model> modelVerificationListener = ActionListener.<Model>wrap((model) -> {
                fail("Expected exception, but got model: " + model);
            }, e -> {
                assertThat(e, instanceOf(ElasticsearchStatusException.class));
                assertThat(e.getMessage(), is("Configuration contains settings [{extra_key=value}] unknown to the [openai] service"));
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createOpenAiService()) {
            var taskSettingsMap = getTaskSettingsMap("user");
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(getServiceSettingsMap("model", "url", "org"), taskSettingsMap, getSecretSettingsMap("secret"));

            ActionListener<Model> modelVerificationListener = ActionListener.<Model>wrap((model) -> {
                fail("Expected exception, but got model: " + model);
            }, e -> {
                assertThat(e, instanceOf(ElasticsearchStatusException.class));
                assertThat(e.getMessage(), is("Configuration contains settings [{extra_key=value}] unknown to the [openai] service"));
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createOpenAiService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(getServiceSettingsMap("model", "url", "org"), getTaskSettingsMap("user"), secretSettingsMap);

            ActionListener<Model> modelVerificationListener = ActionListener.<Model>wrap((model) -> {
                fail("Expected exception, but got model: " + model);
            }, e -> {
                assertThat(e, instanceOf(ElasticsearchStatusException.class));
                assertThat(e.getMessage(), is("Configuration contains settings [{extra_key=value}] unknown to the [openai] service"));
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiEmbeddingsModelWithoutUserUrlOrganization() throws IOException {
        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

                var embeddingsModel = (OpenAiEmbeddingsModel) model;
                assertNull(embeddingsModel.getServiceSettings().uri());
                assertNull(embeddingsModel.getServiceSettings().organizationId());
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertNull(embeddingsModel.getTaskSettings().user());
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap("model", null, null), getTaskSettingsMap(null), getSecretSettingsMap("secret")),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiChatCompletionsModelWithoutUserWithoutUserUrlOrganization() throws IOException {
        var model = "model";
        var secret = "secret";

        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(m -> {
                assertThat(m, instanceOf(OpenAiChatCompletionModel.class));

                var completionsModel = (OpenAiChatCompletionModel) m;
                assertNull(completionsModel.getServiceSettings().uri());
                assertNull(completionsModel.getServiceSettings().organizationId());
                assertThat(completionsModel.getServiceSettings().modelId(), is(model));
                assertNull(completionsModel.getTaskSettings().user());
                assertThat(completionsModel.getSecretSettings().apiKey().toString(), is(secret));

            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(getServiceSettingsMap(model, null, null), getTaskSettingsMap(null), getSecretSettingsMap(secret)),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_MovesModel() throws IOException {
        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

                var embeddingsModel = (OpenAiEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
                assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    getServiceSettingsMap("model", "url", "org"),
                    getTaskSettingsMap("user"),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

                var embeddingsModel = (OpenAiEmbeddingsModel) model;
                assertNull(embeddingsModel.getServiceSettings().uri());
                assertNull(embeddingsModel.getServiceSettings().organizationId());
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertNull(embeddingsModel.getTaskSettings().user());
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    getServiceSettingsMap("model", null, null),
                    getTaskSettingsMap(null),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createOpenAiService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

                var embeddingsModel = (OpenAiEmbeddingsModel) model;
                assertNull(embeddingsModel.getServiceSettings().uri());
                assertNull(embeddingsModel.getServiceSettings().organizationId());
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertNull(embeddingsModel.getTaskSettings().user());
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(getServiceSettingsMap("model", null, null), getTaskSettingsMap(null), getSecretSettingsMap("secret")),
                modelVerificationListener
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnOpenAiEmbeddingsModel() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org", 100, null, false),
                getTaskSettingsMap("user"),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org"),
                getTaskSettingsMap("user"),
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

            assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [openai] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnOpenAiEmbeddingsModelWithoutUserUrlOrganization() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", null, null, null, null, true),
                getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", null, null, null, null, true),
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

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", null, null, null, null, true),
                getTaskSettingsMap(null),
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org", null, null, true),
                getTaskSettingsMap("user"),
                getSecretSettingsMap("secret")
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createOpenAiService()) {
            var secretSettingsMap = getSecretSettingsMap("secret");
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org", null, null, true),
                getTaskSettingsMap("user"),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org", null, null, true),
                getTaskSettingsMap("user"),
                getSecretSettingsMap("secret")
            );
            persistedConfig.secrets().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createOpenAiService()) {
            var serviceSettingsMap = getServiceSettingsMap("model", "url", "org", null, null, true);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMap("user"), getSecretSettingsMap("secret"));

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createOpenAiService()) {
            var taskSettingsMap = getTaskSettingsMap("user");
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org", null, null, true),
                taskSettingsMap,
                getSecretSettingsMap("secret")
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModel() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org", null, null, true),
                getTaskSettingsMap("user")
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("model", "url", "org"), getTaskSettingsMap("user"));

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config())
            );

            assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [openai] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModelWithoutUserUrlOrganization() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", null, null, null, null, true),
                getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", null, null, null, null, true),
                getTaskSettingsMap(null),
                createRandomChunkingSettingsMap()
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", null, null, null, null, true),
                getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertNull(embeddingsModel.getServiceSettings().uri());
            assertNull(embeddingsModel.getServiceSettings().organizationId());
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertNull(embeddingsModel.getTaskSettings().user());
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createOpenAiService()) {
            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap("model", "url", "org", null, null, true),
                getTaskSettingsMap("user")
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createOpenAiService()) {
            var serviceSettingsMap = getServiceSettingsMap("model", "url", "org", null, null, true);
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettingsMap, getTaskSettingsMap("user"));

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createOpenAiService()) {
            var taskSettingsMap = getTaskSettingsMap("user");
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap("model", "url", "org", null, null, true), taskSettingsMap);

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(OpenAiEmbeddingsModel.class));

            var embeddingsModel = (OpenAiEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getServiceSettings().uri().toString(), is("url"));
            assertThat(embeddingsModel.getServiceSettings().organizationId(), is("org"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings().user(), is("user"));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotOpenAiModel() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new OpenAiService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                null,
                null,
                List.of(""),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
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

    public void testInfer_ThrowsErrorWhenInputTypeIsSpecified() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user");

        try (var service = new OpenAiService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var thrownException = expectThrows(
                ValidationException.class,
                () -> service.infer(
                    model,
                    null,
                    null,
                    null,
                    List.of(""),
                    false,
                    new HashMap<>(),
                    InputType.INGEST,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                )
            );
            assertThat(
                thrownException.getMessage(),
                is("Validation Failed: 1: Invalid input_type [ingest]. The input_type option is not supported by this service;")
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.SPARSE_EMBEDDING);

        try (var service = new OpenAiService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                mockModel,
                null,
                null,
                null,
                List.of(""),
                false,
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(
                    "Inference entity [model_id] does not support task type [sparse_embedding] "
                        + "for inference, the task type must be one of [text_embedding, completion]."
                )
            );

            verify(factory, times(1)).createSender();
            verify(sender, times(1)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_ThrowsErrorWhenTaskTypeIsNotValid_ChatCompletion() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var mockModel = getInvalidModel("model_id", "service_name", TaskType.CHAT_COMPLETION);

        try (var service = new OpenAiService(factory, createWithEmptySettings(threadPool))) {
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
                is(
                    "Inference entity [model_id] does not support task type [chat_completion] "
                        + "for inference, the task type must be one of [text_embedding, completion]. "
                        + "The task type for the inference entity is chat_completion, "
                        + "please use the _inference/chat_completion/model_id/_stream URL."
                )
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

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool))) {

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
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
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

    public void testUnifiedCompletionInfer() throws Exception {
        // The escapes are because the streaming response must be on a single line
        String responseJson = """
            data: {\
                "id":"12345",\
                "object":"chat.completion.chunk",\
                "created":123456789,\
                "model":"gpt-4o-mini",\
                "system_fingerprint": "123456789",\
                "choices":[\
                    {\
                        "index":0,\
                        "delta":{\
                            "content":"hello, world"\
                        },\
                        "logprobs":null,\
                        "finish_reason":"stop"\
                    }\
                ],\
                "usage":{\
                    "prompt_tokens": 16,\
                    "completion_tokens": 28,\
                    "total_tokens": 44,\
                    "prompt_tokens_details": {\
                        "cached_tokens": 0,\
                        "audio_tokens": 0\
                    },\
                    "completion_tokens_details": {\
                        "reasoning_tokens": 0,\
                        "audio_tokens": 0,\
                        "accepted_prediction_tokens": 0,\
                        "rejected_prediction_tokens": 0\
                    }\
                }\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = OpenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);
            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoErrors().hasEvent("""
                {"id":"12345","choices":[{"delta":{"content":"hello, world"},"finish_reason":"stop","index":0}],""" + """
                "model":"gpt-4o-mini","object":"chat.completion.chunk",""" + """
                "usage":{"completion_tokens":28,"prompt_tokens":16,"total_tokens":44}}""");
        }
    }

    public void testUnifiedCompletionError() throws Exception {
        String responseJson = """
            {
                "error": {
                    "message": "The model `gpt-4awero` does not exist or you do not have access to it.",
                    "type": "invalid_request_error",
                    "param": null,
                    "code": "model_not_found"
                }
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(404).setBody(responseJson));

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = OpenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
            var latch = new CountDownLatch(1);
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                ActionListener.runAfter(ActionTestUtils.assertNoSuccessListener(e -> {
                    try (var builder = XContentFactory.jsonBuilder()) {
                        var t = unwrapCause(e);
                        assertThat(t, isA(UnifiedChatCompletionException.class));
                        ((UnifiedChatCompletionException) t).toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                            try {
                                xContent.toXContent(builder, EMPTY_PARAMS);
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        });
                        var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());

                        assertThat(json, is(String.format(Locale.ROOT, """
                            {\
                            "error":{\
                            "code":"model_not_found",\
                            "message":"Resource not found at [%s] for request from inference entity id [id] status \
                            [404]. Error message: [The model `gpt-4awero` does not exist or you do not have access to it.]",\
                            "type":"invalid_request_error"\
                            }}""", getUrl(webServer))));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }), latch::countDown)
            );
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }
    }

    public void testMidStreamUnifiedCompletionError() throws Exception {
        String responseJson = """
            event: error
            data: { "error": { "message": "Timed out waiting for more data", "type": "timeout" } }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError("""
            {\
            "error":{\
            "message":"Received an error response for request from inference entity id [id]. Error message: \
            [Timed out waiting for more data]",\
            "type":"timeout"\
            }}""");
    }

    private void testStreamError(String expectedResponse) throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = OpenAiChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.unifiedCompletionInfer(
                model,
                UnifiedCompletionRequest.of(
                    List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "user", null, null))
                ),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            InferenceEventsAssertion.assertThat(result).hasFinishedStream().hasNoEvents().hasErrorMatching(e -> {
                e = unwrapCause(e);
                assertThat(e, isA(UnifiedChatCompletionException.class));
                try (var builder = XContentFactory.jsonBuilder()) {
                    ((UnifiedChatCompletionException) e).toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                        try {
                            xContent.toXContent(builder, EMPTY_PARAMS);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
                    var json = XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());

                    assertThat(json, is(expectedResponse));
                }
            });
        }
    }

    public void testUnifiedCompletionMalformedError() throws Exception {
        String responseJson = """
            data: { invalid json }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        testStreamError("""
            {\
            "error":{\
            "code":"bad_request",\
            "message":"[1:3] Unexpected character ('i' (code 105)): was expecting double-quote to start field name\\n\
             at [Source: (String)\\"{ invalid json }\\"; line: 1, column: 3]",\
            "type":"x_content_parse_exception"\
            }}""");
    }

    public void testInfer_StreamRequest() throws Exception {
        String responseJson = """
            data: {\
                "id":"12345",\
                "object":"chat.completion.chunk",\
                "created":123456789,\
                "model":"gpt-4o-mini",\
                "system_fingerprint": "123456789",\
                "choices":[\
                    {\
                        "index":0,\
                        "delta":{\
                            "content":"hello, world"\
                        },\
                        "logprobs":null,\
                        "finish_reason":null\
                    }\
                ]\
            }

            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"hello, world"}]}""");
    }

    private InferenceEventsAssertion streamCompletion() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool))) {
            var model = OpenAiChatCompletionModelTests.createCompletionModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                true,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            return InferenceEventsAssertion.assertThat(listener.actionGet(TIMEOUT)).hasFinishedStream();
        }
    }

    public void testInfer_StreamRequest_ErrorResponse() throws Exception {
        String responseJson = """
            {
              "error": {
                "message": "You didn't provide an API key...",
                "type": "invalid_request_error",
                "param": null,
                "code": null
              }
            }""";
        webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

        var e = assertThrows(ElasticsearchStatusException.class, this::streamCompletion);
        assertThat(e.status(), equalTo(RestStatus.UNAUTHORIZED));
        assertThat(
            e.getMessage(),
            equalTo(
                "Received an authentication error status code for request from inference entity id [id] status [401]. "
                    + "Error message: [You didn't provide an API key...]"
            )
        );
    }

    public void testInfer_StreamRequestRetry() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(503).setBody("""
            {
              "error": {
                "message": "server busy",
                "type": "server_busy"
              }
            }"""));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("""
            data: {\
                "id":"12345",\
                "object":"chat.completion.chunk",\
                "created":123456789,\
                "model":"gpt-4o-mini",\
                "system_fingerprint": "123456789",\
                "choices":[\
                    {\
                        "index":0,\
                        "delta":{\
                            "content":"hello, world"\
                        },\
                        "logprobs":null,\
                        "finish_reason":null\
                    }\
                ]\
            }

            """));

        streamCompletion().hasNoErrors().hasEvent("""
            {"completion":[{"delta":"hello, world"}]}""");
    }

    public void testSupportsStreaming() throws IOException {
        try (var service = new OpenAiService(mock(), createWithEmptySettings(mock()))) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        try (var service = createOpenAiService()) {
            var model = createCompletionModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10)
            );
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
        try (var service = createOpenAiService()) {
            var embeddingSize = randomNonNegativeInt();
            var model = OpenAiEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                similarityMeasure,
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                randomBoolean()
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null ? SimilarityMeasure.DOT_PRODUCT : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testInfer_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool))) {

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

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                null,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Incorrect API key provided:]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testMoveModelFromTaskToServiceSettings() {
        var taskSettings = new HashMap<String, Object>();
        taskSettings.put(ServiceFields.MODEL_ID, "model");
        var serviceSettings = new HashMap<String, Object>();
        OpenAiService.moveModelFromTaskToServiceSettings(taskSettings, serviceSettings);
        assertThat(taskSettings.keySet(), empty());
        assertEquals("model", serviceSettings.get(ServiceFields.MODEL_ID));
    }

    public void testMoveModelFromTaskToServiceSettings_OldID() {
        var taskSettings = new HashMap<String, Object>();
        taskSettings.put("model", "model");
        var serviceSettings = new HashMap<String, Object>();
        OpenAiService.moveModelFromTaskToServiceSettings(taskSettings, serviceSettings);
        assertThat(taskSettings.keySet(), empty());
        assertEquals("model", serviceSettings.get(ServiceFields.MODEL_ID));
    }

    public void testMoveModelFromTaskToServiceSettings_AlreadyMoved() {
        var taskSettings = new HashMap<String, Object>();
        var serviceSettings = new HashMap<String, Object>();
        taskSettings.put(ServiceFields.MODEL_ID, "model");
        OpenAiService.moveModelFromTaskToServiceSettings(taskSettings, serviceSettings);
        assertThat(taskSettings.keySet(), empty());
        assertEquals("model", serviceSettings.get(ServiceFields.MODEL_ID));
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = OpenAiEmbeddingsModelTests.createModel(
            getUrl(webServer),
            "org",
            "secret",
            "model",
            "user",
            ChunkingSettingsTests.createRandomChunkingSettings()
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", (ChunkingSettings) null);

        testChunkedInfer(model);
    }

    private void testChunkedInfer(OpenAiEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new OpenAiService(senderFactory, createWithEmptySettings(threadPool))) {

            // response with 2 embeddings
            String responseJson = """
                {
                  "object": "list",
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
                  ],
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.INTERNAL_INGEST,
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
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.123f, -0.123f },
                        ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().get(0).offset());
                assertThat(floatResult.chunks().get(0).embedding(), Matchers.instanceOf(TextEmbeddingFloatResults.Embedding.class));
                assertTrue(
                    Arrays.equals(
                        new float[] { 0.223f, -0.223f },
                        ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values()
                    )
                );
            }

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(3));
            assertThat(requestMap.get("input"), Matchers.is(List.of("a", "bb")));
            assertThat(requestMap.get("model"), Matchers.is("model"));
            assertThat(requestMap.get("user"), Matchers.is("user"));
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createOpenAiService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                            "service": "openai",
                            "name": "OpenAI",
                            "task_types": ["text_embedding", "completion", "chat_completion"],
                            "configurations": {
                                "api_key": {
                                    "description": "The OpenAI API authentication key. For more details about generating OpenAI API keys, refer to the https://platform.openai.com/account/api-keys.",
                                    "label": "API Key",
                                    "required": true,
                                    "sensitive": true,
                                    "updatable": true,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                                },
                                "url": {
                                    "description": "The absolute URL of the external service to send requests to.",
                                    "label": "URL",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                                },
                                "dimensions": {
                                    "description": "The number of dimensions the resulting embeddings should have. For more information refer to https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-dimensions.",
                                    "label": "Dimensions",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding"]
                                },
                                "organization_id": {
                                    "description": "The unique identifier of your organization.",
                                    "label": "Organization ID",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                                },
                                "rate_limit.requests_per_minute": {
                                    "description": "Default number of requests allowed per minute. For text_embedding is 3000. For completion is 500.",
                                    "label": "Rate Limit",
                                    "required": false,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "int",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion"]
                                },
                                "model_id": {
                                    "description": "The name of the model to use for the inference task.",
                                    "label": "Model ID",
                                    "required": true,
                                    "sensitive": false,
                                    "updatable": false,
                                    "type": "str",
                                    "supported_task_types": ["text_embedding", "completion", "chat_completion"]
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

    private OpenAiService createOpenAiService() {
        return new OpenAiService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }
}
