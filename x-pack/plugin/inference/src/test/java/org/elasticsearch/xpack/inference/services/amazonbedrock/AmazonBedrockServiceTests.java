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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
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
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockMockRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
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
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProviderCapabilities.getProviderDefaultSimilarityMeasure;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettingsTests.getAmazonBedrockSecretSettingsMap;
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
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey.toString(), is("access"));
                assertThat(secretSettings.secretKey.toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

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

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [amazonbedrock] service does not support task type [sparse_embedding]"));
                }
            );

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
                         "provider": "amazonbedrock",
                         "task_types": [
                               {
                                   "task_type": "text_embedding",
                                   "configuration": {}
                               },
                               {
                                   "task_type": "completion",
                                   "configuration": {
                                       "top_p": {
                                           "default_value": null,
                                           "depends_on": [],
                                           "display": "numeric",
                                           "label": "Top P",
                                           "order": 3,
                                           "required": false,
                                           "sensitive": false,
                                           "tooltip": "Alternative to temperature. A number in the range of 0.0 to 1.0, to eliminate low-probability tokens.",
                                           "type": "int",
                                           "ui_restrictions": [],
                                           "validations": [],
                                           "value": null
                                       },
                                       "max_new_tokens": {
                                           "default_value": null,
                                           "depends_on": [],
                                           "display": "numeric",
                                           "label": "Max New Tokens",
                                           "order": 1,
                                           "required": false,
                                           "sensitive": false,
                                           "tooltip": "Sets the maximum number for the output tokens to be generated.",
                                           "type": "int",
                                           "ui_restrictions": [],
                                           "validations": [],
                                           "value": null
                                       },
                                       "top_k": {
                                           "default_value": null,
                                           "depends_on": [],
                                           "display": "numeric",
                                           "label": "Top K",
                                           "order": 4,
                                           "required": false,
                                           "sensitive": false,
                                           "tooltip": "Only available for anthropic, cohere, and mistral providers. Alternative to temperature.",
                                           "type": "int",
                                           "ui_restrictions": [],
                                           "validations": [],
                                           "value": null
                                       },
                                       "temperature": {
                                           "default_value": null,
                                           "depends_on": [],
                                           "display": "numeric",
                                           "label": "Temperature",
                                           "order": 2,
                                           "required": false,
                                           "sensitive": false,
                                           "tooltip": "A number between 0.0 and 1.0 that controls the apparent creativity of the results.",
                                           "type": "int",
                                           "ui_restrictions": [],
                                           "validations": [],
                                           "value": null
                                       }
                                   }
                               }
                         ],
                         "configuration": {
                             "secret_key": {
                                 "default_value": null,
                                 "depends_on": [],
                                 "display": "textbox",
                                 "label": "Secret Key",
                                 "order": 2,
                                 "required": true,
                                 "sensitive": true,
                                 "tooltip": "A valid AWS secret key that is paired with the access_key.",
                                 "type": "str",
                                 "ui_restrictions": [],
                                 "validations": [],
                                 "value": null
                             },
                             "provider": {
                                 "default_value": null,
                                 "depends_on": [],
                                 "display": "dropdown",
                                 "label": "Provider",
                                 "options": [
                                     {
                                         "label": "amazontitan",
                                         "value": "amazontitan"
                                     },
                                     {
                                         "label": "anthropic",
                                         "value": "anthropic"
                                     },
                                     {
                                         "label": "ai21labs",
                                         "value": "ai21labs"
                                     },
                                     {
                                         "label": "cohere",
                                         "value": "cohere"
                                     },
                                     {
                                         "label": "meta",
                                         "value": "meta"
                                     },
                                     {
                                         "label": "mistral",
                                         "value": "mistral"
                                     }
                                 ],
                                 "order": 3,
                                 "required": true,
                                 "sensitive": false,
                                 "tooltip": "The model provider for your deployment.",
                                 "type": "str",
                                 "ui_restrictions": [],
                                 "validations": [],
                                 "value": null
                             },
                             "access_key": {
                                 "default_value": null,
                                 "depends_on": [],
                                 "display": "textbox",
                                 "label": "Access Key",
                                 "order": 1,
                                 "required": true,
                                 "sensitive": true,
                                 "tooltip": "A valid AWS access key that has permissions to use Amazon Bedrock.",
                                 "type": "str",
                                 "ui_restrictions": [],
                                 "validations": [],
                                 "value": null
                             },
                             "model": {
                                 "default_value": null,
                                 "depends_on": [],
                                 "display": "textbox",
                                 "label": "Model",
                                 "order": 4,
                                 "required": true,
                                 "sensitive": false,
                                 "tooltip": "The base model ID or an ARN to a custom model based on a foundational model.",
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
                                 "tooltip": "By default, the amazonbedrock service sets the number of requests allowed per minute to 240.",
                                 "type": "int",
                                 "ui_restrictions": [],
                                 "validations": [],
                                 "value": null
                             },
                             "region": {
                                 "default_value": null,
                                 "depends_on": [],
                                 "display": "textbox",
                                 "label": "Region",
                                 "order": 5,
                                 "required": true,
                                 "sensitive": false,
                                 "tooltip": "The region that your model or ARN is deployed in.",
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

    public void testCreateModel_ForEmbeddingsTask_InvalidProvider() throws IOException {
        try (var service = createAmazonBedrockService()) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [text_embedding] task type for provider [anthropic] is not available"));
                }
            );

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
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(exception.getMessage(), is("The [top_k] task parameter is not available for provider [amazontitan]"));
                }
            );

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

            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                    assertThat(
                        exception.getMessage(),
                        is("Model configuration contains settings [{extra_key=value}] unknown to the [amazonbedrock] service")
                    );
                }
            );

            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, modelVerificationListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var serviceSettings = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, null, null, null);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(serviceSettings, Map.of(), getAmazonBedrockSecretSettingsMap("access", "secret"));

            ActionListener<Model> modelVerificationListener = ActionListener.<Model>wrap((model) -> {
                fail("Expected exception, but got model: " + model);
            }, e -> {
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

            ActionListener<Model> modelVerificationListener = ActionListener.<Model>wrap((model) -> {
                fail("Expected exception, but got model: " + model);
            }, e -> {
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

            ActionListener<Model> modelVerificationListener = ActionListener.<Model>wrap((model) -> {
                fail("Expected exception, but got model: " + model);
            }, e -> {
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
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey.toString(), is("access"));
                assertThat(secretSettings.secretKey.toString(), is("secret"));
            }, exception -> fail("Unexpected exception: " + exception));

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
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey.toString(), is("access"));
                assertThat(secretSettings.secretKey.toString(), is("secret"));
                assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            }, exception -> fail("Unexpected exception: " + exception));

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
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AmazonBedrockEmbeddingsModel.class));

                var settings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
                assertThat(settings.region(), is("region"));
                assertThat(settings.modelId(), is("model"));
                assertThat(settings.provider(), is(AmazonBedrockProvider.AMAZONTITAN));
                var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
                assertThat(secretSettings.accessKey.toString(), is("access"));
                assertThat(secretSettings.secretKey.toString(), is("secret"));
                assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            }, exception -> fail("Unexpected exception: " + exception));

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
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ValidationException.class));
                    assertThat(exception.getMessage(), containsString("[service_settings] does not allow the setting [dimensions]"));
                }
            );

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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnAmazonBedrockEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(
                settingsMap,
                new HashMap<String, Object>(Map.of()),
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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
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

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);
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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");
            secretSettingsMap.put("extra_key", "value");

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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            var secretSettingsMap = getAmazonBedrockSecretSettingsMap("access", "secret");

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);
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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
        }
    }

    public void testParsePersistedConfigWithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createAmazonBedrockService()) {
            var settingsMap = createEmbeddingsRequestSettingsMap("region", "model", "amazontitan", null, false, null, null);
            settingsMap.put("extra_key", "value");
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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
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
            var secretSettings = (AmazonBedrockSecretSettings) model.getSecretSettings();
            assertThat(secretSettings.accessKey.toString(), is("access"));
            assertThat(secretSettings.secretKey.toString(), is("secret"));
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
                new HashMap<String, Object>(Map.of()),
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

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);

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

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);

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

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);
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

            var persistedConfig = getPersistedConfigMap(settingsMap, new HashMap<String, Object>(Map.of()), secretSettingsMap);
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

    public void testInfer_SendsRequest_ForEmbeddingsModel() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                var results = new InferenceTextEmbeddingFloatResults(
                    List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.123F, 0.678F }))
                );
                requestSender.enqueue(results);

                var model = AmazonBedrockEmbeddingsModelTests.createModel(
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

    public void testCheckModelConfig_IncludesMaxTokens_ForEmbeddingsModel() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                var results = new InferenceTextEmbeddingFloatResults(
                    List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.123F, 0.678F }))
                );
                requestSender.enqueue(results);

                var model = AmazonBedrockEmbeddingsModelTests.createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    false,
                    100,
                    null,
                    null,
                    "access",
                    "secret"
                );

                PlainActionFuture<Model> listener = new PlainActionFuture<>();
                service.checkModelConfig(model, listener);
                var result = listener.actionGet(TIMEOUT);
                assertThat(
                    result,
                    is(
                        AmazonBedrockEmbeddingsModelTests.createModel(
                            "id",
                            "region",
                            "model",
                            AmazonBedrockProvider.AMAZONTITAN,
                            2,
                            false,
                            100,
                            SimilarityMeasure.COSINE,
                            null,
                            "access",
                            "secret"
                        )
                    )
                );
                var inputStrings = requestSender.getInputs();

                MatcherAssert.assertThat(inputStrings, Matchers.is(List.of("how big")));
            }
        }
    }

    public void testCheckModelConfig_HasSimilarity_ForEmbeddingsModel() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                var results = new InferenceTextEmbeddingFloatResults(
                    List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.123F, 0.678F }))
                );
                requestSender.enqueue(results);

                var model = AmazonBedrockEmbeddingsModelTests.createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    null,
                    false,
                    null,
                    SimilarityMeasure.COSINE,
                    null,
                    "access",
                    "secret"
                );

                PlainActionFuture<Model> listener = new PlainActionFuture<>();
                service.checkModelConfig(model, listener);
                var result = listener.actionGet(TIMEOUT);
                assertThat(
                    result,
                    is(
                        AmazonBedrockEmbeddingsModelTests.createModel(
                            "id",
                            "region",
                            "model",
                            AmazonBedrockProvider.AMAZONTITAN,
                            2,
                            false,
                            null,
                            SimilarityMeasure.COSINE,
                            null,
                            "access",
                            "secret"
                        )
                    )
                );
                var inputStrings = requestSender.getInputs();

                MatcherAssert.assertThat(inputStrings, Matchers.is(List.of("how big")));
            }
        }
    }

    public void testCheckModelConfig_ThrowsIfEmbeddingSizeDoesNotMatchValueSetByUser() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                var results = new InferenceTextEmbeddingFloatResults(
                    List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.123F, 0.678F }))
                );
                requestSender.enqueue(results);

                var model = AmazonBedrockEmbeddingsModelTests.createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    3,
                    true,
                    null,
                    null,
                    null,
                    "access",
                    "secret"
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

                var inputStrings = requestSender.getInputs();
                MatcherAssert.assertThat(inputStrings, Matchers.is(List.of("how big")));
            }
        }
    }

    public void testCheckModelConfig_ReturnsNewModelReference_AndDoesNotSendDimensionsField_WhenNotSetByUser() throws IOException {
        var sender = mock(Sender.class);
        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var amazonBedrockFactory = new AmazonBedrockMockRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, Settings.EMPTY),
            mockClusterServiceEmpty()
        );

        try (var service = new AmazonBedrockService(factory, amazonBedrockFactory, createWithEmptySettings(threadPool))) {
            try (var requestSender = (AmazonBedrockMockRequestSender) amazonBedrockFactory.createSender()) {
                var results = new InferenceTextEmbeddingFloatResults(
                    List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.123F, 0.678F }))
                );
                requestSender.enqueue(results);

                var model = AmazonBedrockEmbeddingsModelTests.createModel(
                    "id",
                    "region",
                    "model",
                    AmazonBedrockProvider.AMAZONTITAN,
                    100,
                    false,
                    null,
                    SimilarityMeasure.COSINE,
                    null,
                    "access",
                    "secret"
                );

                PlainActionFuture<Model> listener = new PlainActionFuture<>();
                service.checkModelConfig(model, listener);
                var result = listener.actionGet(TIMEOUT);
                assertThat(
                    result,
                    is(
                        AmazonBedrockEmbeddingsModelTests.createModel(
                            "id",
                            "region",
                            "model",
                            AmazonBedrockProvider.AMAZONTITAN,
                            2,
                            false,
                            null,
                            SimilarityMeasure.COSINE,
                            null,
                            "access",
                            "secret"
                        )
                    )
                );
                var inputStrings = requestSender.getInputs();

                MatcherAssert.assertThat(inputStrings, Matchers.is(List.of("how big")));
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
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INGEST,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var exceptionThrown = assertThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exceptionThrown.getCause().getMessage(), containsString("The security token included in the request is invalid"));
        }
    }

    public void testSupportsStreaming() throws IOException {
        try (var service = new AmazonBedrockService(mock(), mock(), createWithEmptySettings(mock()))) {
            assertTrue(service.canStream(TaskType.COMPLETION));
            assertTrue(service.canStream(TaskType.ANY));
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
                    var mockResults1 = new InferenceTextEmbeddingFloatResults(
                        List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.123F, 0.678F }))
                    );
                    requestSender.enqueue(mockResults1);
                }
                {
                    var mockResults2 = new InferenceTextEmbeddingFloatResults(
                        List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.223F, 0.278F }))
                    );
                    requestSender.enqueue(mockResults2);
                }

                PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
                service.chunkedInfer(
                    model,
                    null,
                    List.of("abc", "xyz"),
                    new HashMap<>(),
                    InputType.INGEST,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                );

                var results = listener.actionGet(TIMEOUT);
                assertThat(results, hasSize(2));
                {
                    assertThat(results.get(0), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                    var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(0);
                    assertThat(floatResult.chunks(), hasSize(1));
                    assertEquals("abc", floatResult.chunks().get(0).matchedText());
                    assertArrayEquals(new float[] { 0.123F, 0.678F }, floatResult.chunks().get(0).embedding(), 0.0f);
                }
                {
                    assertThat(results.get(1), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                    var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(1);
                    assertThat(floatResult.chunks(), hasSize(1));
                    assertEquals("xyz", floatResult.chunks().get(0).matchedText());
                    assertArrayEquals(new float[] { 0.223F, 0.278F }, floatResult.chunks().get(0).embedding(), 0.0f);
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
