/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;

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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockMockRequestSender;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettingsTests.getAmazonBedrockSecretSettingsMap;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProviderCapabilities.getProviderDefaultSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionServiceSettingsTests.createChatCompletionRequestSettingsMap;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettingsTests.createEmbeddingsRequestSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AmazonBedrockServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testParseRequestConfig_CreatesAnAmazonBedrockModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoFailureListener(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AwsSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey().toString(), is("access"));
                assertThat(secretSettings.secretKey().toString(), is("secret"));
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null),
                    Map.of(),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesACohereModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoFailureListener(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.COHERE));
                var secretSettings = (AwsSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey().toString(), is("access"));
                assertThat(secretSettings.secretKey().toString(), is("secret"));
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "cohere", null, null, null, null),
                    AmazonBedrockEmbeddingsTaskSettingsTests.mutableMap("truncate", CohereTruncation.START),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CohereSettingsWithNoCohereModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(exception -> {
                assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                assertThat(
                    exception.getMessage(),
                    is("The [text_embedding] task type for provider [amazontitan] does not allow [truncate] field")
                );
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null),
                    AmazonBedrockEmbeddingsTaskSettingsTests.mutableMap("truncate", CohereTruncation.START),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(exception -> {
                assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                assertThat(exception.getMessage(), is("The [amazonbedrock] service does not support task type [sparse_embedding]"));
            });

            service.parseRequestConfig(
                "id",
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null),
                    Map.of(),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createAmazonBedrockService()) {
            String content = XContentHelper.stripWhitespace(
                """
                    {
                         "service": "amazonbedrock",
                         "name": "Amazon Bedrock",
                         "task_types": ["text_embedding", "completion"],
                         "configurations": {
                              "dimensions": {
                                 "description": "The number of dimensions the resulting embeddings should have. For more information refer to https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-embed-text.html.",
                                 "label": "Dimensions",
                                 "required": false,
                                 "sensitive": false,
                                 "updatable": false,
                                 "type": "int",
                                 "supported_task_types": ["text_embedding"]
                             },
                             "secret_key": {
                                 "description": "A valid AWS secret key that is paired with the access_key.",
                                 "label": "Secret Key",
                                 "required": true,
                                 "sensitive": true,
                                 "updatable": true,
                                 "type": "str",
                                 "supported_task_types": ["text_embedding", "completion"]
                             },
                             "provider": {
                                 "description": "The model provider for your deployment.",
                                 "label": "Provider",
                                 "required": true,
                                 "sensitive": false,
                                 "updatable": false,
                                 "type": "str",
                                 "supported_task_types": ["text_embedding", "completion"]
                             },
                             "access_key": {
                                 "description": "A valid AWS access key that has permissions to use Amazon Bedrock.",
                                 "label": "Access Key",
                                 "required": true,
                                 "sensitive": true,
                                 "updatable": true,
                                 "type": "str",
                                 "supported_task_types": ["text_embedding", "completion"]
                             },
                             "model": {
                                 "description": "The base model ID or an ARN to a custom model based on a foundational model.",
                                 "label": "Model",
                                 "required": true,
                                 "sensitive": false,
                                 "updatable": false,
                                 "type": "str",
                                 "supported_task_types": ["text_embedding", "completion"]
                             },
                             "rate_limit.requests_per_minute": {
                                 "description": "By default, the amazonbedrock service sets the number of requests allowed per minute to 240.",
                                 "label": "Rate Limit",
                                 "required": false,
                                 "sensitive": false,
                                 "updatable": false,
                                 "type": "int",
                                 "supported_task_types": ["text_embedding", "completion"]
                             },
                             "region": {
                                 "description": "The region that your model or ARN is deployed in.",
                                 "label": "Region",
                                 "required": true,
                                 "sensitive": false,
                                 "updatable": false,
                                 "type": "str",
                                 "supported_task_types": ["text_embedding", "completion"]
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

    public void testCreateModel_ForEmbeddingsTask_InvalidProvider() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(exception -> {
                assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                assertThat(exception.getMessage(), is("The [text_embedding] task type for provider [anthropic] is not available"));
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "anthropic", null, null, null, null),
                    Map.of(),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testCreateModel_TopKParameter_NotAvailable() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(exception -> {
                assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                assertThat(exception.getMessage(), is("The [top_k] task parameter is not available for provider [amazontitan]"));
            });

            service.parseRequestConfig(
                "id",
                TaskType.COMPLETION,
                getRequestConfigMap(
                    createChatCompletionRequestSettingsMap("region", "model", "amazontitan"),
                    getChatCompletionTaskSettingsMap(1.0, 0.5, 0.2, 128),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var config = getRequestConfigMap(
                createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null),
                Map.of(),
                getAmazonBedrockSecretSettingsMap("access", "secret")
            );

            config.put("extra_key", "value");

            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(exception -> {
                assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                assertThat(
                    exception.getMessage(),
                    is("Model configuration contains settings [{extra_key=value}] unknown to the [amazonbedrock] service")
                );
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var serviceSettings = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, Map.of(), getAmazonBedrockSecretSettingsMap("access", "secret"));

            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(e -> {
                assertThat(e, instanceOf(ElasticsearchStatusException.class));
                assertThat(
                    e.getMessage(),
                    is("Model configuration contains settings [{extra_key=value}] unknown to the [amazonbedrock] service")
                );
            });

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createChatCompletionRequestSettingsMap("region", "model", "anthropic");
            var taskSettingsMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.2, 128);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(settingsMap, taskSettingsMap, secretSettingsMap);

            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(e -> {
                assertThat(e, instanceOf(ElasticsearchStatusException.class));
                assertThat(
                    e.getMessage(),
                    is("Model configuration contains settings [{extra_key=value}] unknown to the [amazonbedrock] service")
                );
            });

            service.parseRequestConfig("id", TaskType.COMPLETION, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createChatCompletionRequestSettingsMap("region", "model", "anthropic");
            var taskSettingsMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.2, 128);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(settingsMap, taskSettingsMap, secretSettingsMap);

            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(e -> {
                assertThat(e, instanceOf(ElasticsearchStatusException.class));
                assertThat(
                    e.getMessage(),
                    is("Model configuration contains settings [{extra_key=value}] unknown to the [amazonbedrock] service")
                );
            });

            service.parseRequestConfig("id", TaskType.COMPLETION, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_MovesModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoFailureListener(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AwsSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey().toString(), is("access"));
                assertThat(secretSettings.secretKey().toString(), is("secret"));
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null),
                    Map.of(),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnAmazonBedrockEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoFailureListener(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AwsSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey().toString(), is("access"));
                assertThat(secretSettings.secretKey().toString(), is("secret"));
                assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null),
                    Map.of(),
                    createRandomChunkingSettingsMap(),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnAmazonBedrockEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoFailureListener(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AwsSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey().toString(), is("access"));
                assertThat(secretSettings.secretKey().toString(), is("secret"));
                assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null),
                    Map.of(),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testCreateModel_ForEmbeddingsTask_DimensionsIsNotAllowed() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionTestUtils.assertNoSuccessListener(exception -> {
                assertThat(exception, instanceOf(ValidationException.class));
                assertThat(exception.getMessage(), containsString("[service_settings] does not allow the setting [dimensions]"));
            });

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", 512, null, null, null),
                    Map.of(),
                    getAmazonBedrockSecretSettingsMap("access", "secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnAmazonBedrockEmbeddingsModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnAmazonBedrockEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(
                settingsMap,
                new HashMap<>(Map.of()),
                createRandomChunkingSettingsMap(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnAmazonBedrockEmbeddingsModelWhenChunkingSettingsNotProvided()
        throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createChatCompletionRequestSettingsMap("region", "model", "amazontitan");
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, Map.of(), secretSettingsMap);

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
                is("Failed to parse stored model [id] for [amazonbedrock] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);
            persistedConfig.secrets().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            settingsMap.put("extra_key", "value");
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createChatCompletionRequestSettingsMap("region", "model", "anthropic");
            var taskSettingsMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.2, 128);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(settingsMap, taskSettingsMap, secretSettingsMap);

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, instanceOf(AmazonBedrockChatCompletionModel.class));

            var settings = (AmazonBedrockChatCompletionServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.ANTHROPIC));
            var taskSettings = (AmazonBedrockChatCompletionTaskSettings) model.getTaskSettings();
            assertThat(taskSettings.temperature(), is(1.0));
            assertThat(taskSettings.topP(), is(0.5));
            assertThat(taskSettings.topK(), is(0.2));
            assertThat(taskSettings.maxNewTokens(), is(128));
            var secretSettings = (AwsSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey().toString(), is("access"));
            assertThat(secretSettings.secretKey().toString(), is("secret"));
        }
    }

    public void testParsePersistedConfig_CreatesAnAmazonBedrockEmbeddingsModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            assertNull(model.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnAmazonBedrockEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(
                settingsMap,
                new HashMap<>(Map.of()),
                createRandomChunkingSettingsMap(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(model.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnAmazonBedrockEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(model.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAnAmazonBedrockChatCompletionModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createChatCompletionRequestSettingsMap("region", "model", "anthropic");
            var taskSettingsMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.2, 128);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, taskSettingsMap, secretSettingsMap);
            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(AmazonBedrockChatCompletionModel.class));

            var settings = (AmazonBedrockChatCompletionServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.ANTHROPIC));
            var taskSettings = (AmazonBedrockChatCompletionTaskSettings) model.getTaskSettings();
            assertThat(taskSettings.temperature(), is(1.0));
            assertThat(taskSettings.topP(), is(0.5));
            assertThat(taskSettings.topK(), is(0.2));
            assertThat(taskSettings.maxNewTokens(), is(128));
            assertNull(model.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig("id", TaskType.SPARSE_EMBEDDING, persistedConfig.config())
            );

            assertThat(
                thrownException.getMessage(),
                is("Failed to parse stored model [id] for [amazonbedrock] service, please delete and add the service again")
            );
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            assertNull(model.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            settingsMap.put("extra_key", "value");
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<>(Map.of()), secretSettingsMap);
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config());

            assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

            var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
            assertNull(model.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createChatCompletionRequestSettingsMap("region", "model", "anthropic");
            var taskSettingsMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.2, 128);
            taskSettingsMap.put("extra_key", "value");
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, taskSettingsMap, secretSettingsMap);
            var model = service.parsePersistedConfig("id", TaskType.COMPLETION, persistedConfig.config());

            assertThat(model, instanceOf(AmazonBedrockChatCompletionModel.class));

            var settings = (AmazonBedrockChatCompletionServiceSettings) model.getServiceSettings();
            assertThat(settings.region(), is("region"));
            assertThat(settings.modelId(), is("model"));
            assertThat(settings.provider(), is(AmazonBedrockProvider.ANTHROPIC));
            var taskSettings = (AmazonBedrockChatCompletionTaskSettings) model.getTaskSettings();
            assertThat(taskSettings.temperature(), is(1.0));
            assertThat(taskSettings.topP(), is(0.5));
            assertThat(taskSettings.topK(), is(0.2));
            assertThat(taskSettings.maxNewTokens(), is(128));
            assertNull(model.getSecretSettings());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotAmazonBedrockModel() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );
        var mockModel = getInvalidModel("model_id", "service_name");

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
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
            verify(sender, times(1)).start();
        }
        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_SendsRequest_ForTitanEmbeddingsModel() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );
        var model = AmazonBedrockEmbeddingsModelTests.createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            "access",
            "secret"
        );

        try (
            var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool));
            var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()
        ) {
            var results = new TextEmbeddingFloatResults(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.123F, 0.678F })));
            requestSender.enqueue(results);
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

            assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.123F, 0.678F }))));
        }
    }

    public void testInfer_SendsRequest_ForCohereEmbeddingsModel() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                var results = new TextEmbeddingFloatResults(
                    List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.123F, 0.678F }))
                );
                requestSender.enqueue(results);

                var model = AmazonBedrockEmbeddingsModelTests.createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.COHERE,
                    "access",
                    "secret"
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
                    InputType.CLASSIFICATION,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                );

                var result = listener.actionGet(TIMEOUT);

                assertThat(result.asMap(), Matchers.is(buildExpectationFloat(List.of(new float[] { 0.123F, 0.678F }))));
            }
        }
    }

    public void testInfer_SendsRequest_ForChatCompletionModel() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                var mockResults = new ChatCompletionResults(List.of(new ChatCompletionResults.Result("test result")));
                requestSender.enqueue(mockResults);

                var model = AmazonBedrockChatCompletionModelTests.createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    "access",
                    "secret"
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

                assertThat(result.asMap(), Matchers.is(buildExpectationCompletion(List.of("test result"))));
            }
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            var model = AmazonBedrockChatCompletionModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomFrom(AmazonBedrockProvider.values()),
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
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            var embeddingSize = randomNonNegativeInt();
            var provider = randomFrom(AmazonBedrockProvider.values());
            var model = AmazonBedrockEmbeddingsModelTests.createModel(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                provider,
                randomNonNegativeInt(),
                randomBoolean(),
                randomNonNegativeInt(),
                similarityMeasure,
                RateLimitSettingsTests.createRandom(),
                createRandomChunkingSettings(),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10)
            );

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            SimilarityMeasure expectedSimilarityMeasure = similarityMeasure == null
                ? getProviderDefaultSimilarityMeasure(provider)
                : similarityMeasure;
            assertEquals(expectedSimilarityMeasure, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testInfer_UnauthorizedResponse() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (
            var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool));
            var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()
        ) {
            requestSender.enqueue(
                BedrockRuntimeException.builder().message("The security token included in the request is invalid").build()
            );

            var model = AmazonBedrockEmbeddingsModelTests.createModel(
                "id",
                "us-east-1",
                "amazon.titan-embed-text-v1",
                AmazonBedrockProvider.AMAZONTITAN,
                "_INVALID_AWS_ACCESS_KEY_",
                "_INVALID_AWS_SECRET_KEY_"
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
                InputType.INTERNAL_INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var exceptionThrown = assertThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exceptionThrown.getCause().getMessage(), containsString("The security token included in the request is invalid"));
        }
    }

    public void testSupportsStreaming() throws IOException {
        try (var service = new AmazonBedrockService(mock(), mock(), createWithEmptySettings(mock()))) {
            assertThat(service.supportedStreamingTasks(), is(EnumSet.of(TaskType.COMPLETION)));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }

    public void testChunkedInfer_ChunkingSettingsSet() throws IOException {
        var model = AmazonBedrockEmbeddingsModelTests.createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            createRandomChunkingSettings(),
            "access",
            "secret"
        );

        testChunkedInfer(model);
    }

    public void testChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = AmazonBedrockEmbeddingsModelTests.createModel(
            "id",
            "region",
            "model",
            AmazonBedrockProvider.AMAZONTITAN,
            null,
            "access",
            "secret"
        );

        testChunkedInfer(model);
    }

    private void testChunkedInfer(AmazonBedrockEmbeddingsModel model) throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                {
                    var mockResults1 = new TextEmbeddingFloatResults(
                        List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.123F, 0.678F }))
                    );
                    requestSender.enqueue(mockResults1);
                }
                {
                    var mockResults2 = new TextEmbeddingFloatResults(
                        List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.223F, 0.278F }))
                    );
                    requestSender.enqueue(mockResults2);
                }

                PlainActionFuture<List<ChunkedInference>> listener = new PlainActionFuture<>();
                service.chunkedInfer(
                    model,
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
                    assertThat(floatResult.chunks().get(0).embedding(), instanceOf(TextEmbeddingFloatResults.Embedding.class));
                    assertArrayEquals(
                        new float[] { 0.123F, 0.678F },
                        ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                        0.0f
                    );
                }
                {
                    assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                    var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                    assertThat(floatResult.chunks(), hasSize(1));
                    assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().get(0).offset());
                    assertThat(floatResult.chunks().get(0).embedding(), instanceOf(TextEmbeddingFloatResults.Embedding.class));
                    assertArrayEquals(
                        new float[] { 0.223F, 0.278F },
                        ((TextEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                        0.0f
                    );
                }
            }
        }
    }

    private AmazonBedrockService createAmazonBedrockService() {
        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );
        return new AmazonBedrockService(mock(HttpRequestSender.Factory.class), amazonBedrockFactory, createWithEmptySettings(threadPool));
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

    private Utils.PersistedConfig getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        var persistedConfigMap = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);
        persistedConfigMap.config().put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return persistedConfigMap;
    }

    private Utils.PersistedConfig getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {

        return new Utils.PersistedConfig(
            new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, serviceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)),
            new HashMap<>(Map.of(ModelSecrets.SECRET_SETTINGS, secretSettings))
        );
    }
}
