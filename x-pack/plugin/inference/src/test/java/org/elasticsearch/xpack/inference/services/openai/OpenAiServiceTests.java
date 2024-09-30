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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
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
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createChatCompletionModel;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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
                Set.of(),
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
                Set.of(),
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
                Set.of(),
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
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
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
                assertThat(e.getMessage(), is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service"));
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
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
                assertThat(e.getMessage(), is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service"));
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
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
                assertThat(e.getMessage(), is("Model configuration contains settings [{extra_key=value}] unknown to the [openai] service"));
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, Set.of(), modelVerificationListener);
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
                Set.of(),
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
                Set.of(),
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
                Set.of(),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsElasticsearchStatusExceptionWhenChunkingSettingsProvidedAndFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
        try (var service = createOpenAiService()) {
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
                getRequestConfigMap(
                    getServiceSettingsMap("model", null, null),
                    getTaskSettingsMap(null),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap("secret")
                ),
                Set.of(),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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
                Set.of(),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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
                Set.of(),
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

    public void testParsePersistedConfigWithSecrets_CreatesAnOpenAiEmbeddingsModelWithoutChunkingSettingsWhenFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
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
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParsePersistedConfigWithSecrets_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModelWithoutChunkingSettingsWhenChunkingSettingsFeatureFlagDisabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is disabled", ChunkingSettingsFeatureFlag.isEnabled() == false);
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
            assertNull(embeddingsModel.getConfigurations().getChunkingSettings());
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testParsePersistedConfig_CreatesAnOpenAiEmbeddingsModelWhenChunkingSettingsNotProvidedAndFeatureFlagEnabled()
        throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INGEST,
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

    public void testCheckModelConfig_IncludesMaxTokens() throws IOException {
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

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", 100);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var result = listener.actionGet(TIMEOUT);
            assertThat(result, is(OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", 100, 2)));

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(requestMap, Matchers.is(Map.of("input", List.of("how big"), "model", "model", "user", "user")));
        }
    }

    public void testCheckModelConfig_ThrowsIfEmbeddingSizeDoesNotMatchValueSetByUser() throws IOException {
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

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", null, 100, 3, true);
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
            MatcherAssert.assertThat(
                requestMap,
                Matchers.is(Map.of("input", List.of("how big"), "model", "model", "user", "user", "dimensions", 3))
            );
        }
    }

    public void testCheckModelConfig_ReturnsModelWithDimensionsSetTo2_AndDocProductSet_IfDimensionsSetByUser_ButSetToNull()
        throws IOException {
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

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", null, 100, null, true);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var returnedModel = listener.actionGet(TIMEOUT);
            assertThat(
                returnedModel,
                is(
                    OpenAiEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "org",
                        "secret",
                        "model",
                        "user",
                        SimilarityMeasure.DOT_PRODUCT,
                        100,
                        2,
                        true
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            // since dimensions were null they should not be sent in the request
            MatcherAssert.assertThat(requestMap, Matchers.is(Map.of("input", List.of("how big"), "model", "model", "user", "user")));
        }
    }

    public void testCheckModelConfig_ReturnsModelWithSameDimensions_AndDocProductSet_IfDimensionsSetByUser_AndTheyMatchReturnedSize()
        throws IOException {
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

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", null, 100, 2, true);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var returnedModel = listener.actionGet(TIMEOUT);
            assertThat(
                returnedModel,
                is(
                    OpenAiEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "org",
                        "secret",
                        "model",
                        "user",
                        SimilarityMeasure.DOT_PRODUCT,
                        100,
                        2,
                        true
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(
                requestMap,
                Matchers.is(Map.of("input", List.of("how big"), "model", "model", "user", "user", "dimensions", 2))
            );
        }
    }

    public void testCheckModelConfig_ReturnsNewModelReference_AndDoesNotSendDimensionsField_WhenNotSetByUser() throws IOException {
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

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", null, 100, 100, false);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var returnedModel = listener.actionGet(TIMEOUT);
            assertThat(
                returnedModel,
                is(
                    OpenAiEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "org",
                        "secret",
                        "model",
                        "user",
                        SimilarityMeasure.DOT_PRODUCT,
                        100,
                        2,
                        false
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(requestMap, Matchers.is(Map.of("input", List.of("how big"), "model", "model", "user", "user")));
        }
    }

    public void testCheckModelConfig_ReturnsNewModelReference_SetsSimilarityToDocProduct_WhenNull() throws IOException {
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

            var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user", null, 100, 100, false);
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var returnedModel = listener.actionGet(TIMEOUT);
            assertThat(
                returnedModel,
                is(
                    OpenAiEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "org",
                        "secret",
                        "model",
                        "user",
                        SimilarityMeasure.DOT_PRODUCT,
                        100,
                        2,
                        false
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(requestMap, Matchers.is(Map.of("input", List.of("how big"), "model", "model", "user", "user")));
        }
    }

    public void testCheckModelConfig_ReturnsNewModelReference_DoesNotOverrideSimilarity_WhenNotNull() throws IOException {
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

            var model = OpenAiEmbeddingsModelTests.createModel(
                getUrl(webServer),
                "org",
                "secret",
                "model",
                "user",
                SimilarityMeasure.COSINE,
                100,
                100,
                false
            );
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);

            var returnedModel = listener.actionGet(TIMEOUT);
            assertThat(
                returnedModel,
                is(
                    OpenAiEmbeddingsModelTests.createModel(
                        getUrl(webServer),
                        "org",
                        "secret",
                        "model",
                        "user",
                        SimilarityMeasure.COSINE,
                        100,
                        2,
                        false
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            MatcherAssert.assertThat(requestMap, Matchers.is(Map.of("input", List.of("how big"), "model", "model", "user", "user")));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        try (var service = createOpenAiService()) {
            var model = createChatCompletionModel(
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
                List.of("abc"),
                false,
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

    public void testChunkedInfer_Batches() throws IOException {
        var model = OpenAiEmbeddingsModelTests.createModel(getUrl(webServer), "org", "secret", "model", "user");
        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

    public void testChunkedInfer_ChunkingSettingsNotSetAndFeatureFlagEnabled() throws IOException {
        assumeTrue("Only if 'inference_chunking_settings' feature flag is enabled", ChunkingSettingsFeatureFlag.isEnabled());
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

            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                List.of("foo", "bar"),
                new HashMap<>(),
                InputType.INGEST,
                new ChunkingOptions(null, null),
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
                assertTrue(Arrays.equals(new float[] { 0.123f, -0.123f }, floatResult.chunks().get(0).embedding()));
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals("bar", floatResult.chunks().get(0).matchedText());
                assertTrue(Arrays.equals(new float[] { 0.223f, -0.223f }, floatResult.chunks().get(0).embedding()));
            }

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), Matchers.is(3));
            assertThat(requestMap.get("input"), Matchers.is(List.of("foo", "bar")));
            assertThat(requestMap.get("model"), Matchers.is("model"));
            assertThat(requestMap.get("user"), Matchers.is("user"));
        }
    }

    private OpenAiService createOpenAiService() {
        return new OpenAiService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool));
    }
}
