/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModelTests;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.InferenceString.fromStringList;
import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.SenderServiceTests.createMockSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceFields.TRUNCATION;
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

public class VoyageAIServiceTests extends InferenceServiceTestCase {
    private static final String INFERENCE_ENTITY_ID_VALUE = "id";
    private static final String MODEL_ID_VALUE = "model_id";
    private static final String API_KEY_VALUE = "secret";
    private static final String URL_VALUE = "url";

    public void testParseRequestConfig_CreatesAVoyageAIEmbeddingsModel() throws IOException {
        try (var service = createInferenceService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

                var embeddingsModel = (VoyageAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null)));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                    VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAVoyageAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createInferenceService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

                var embeddingsModel = (VoyageAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null)));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                    VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST),
                    createRandomChunkingSettingsMap(),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_CreatesAVoyageAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createInferenceService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

                var embeddingsModel = (VoyageAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null)));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                    VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_OptionalTaskSettings() throws IOException {
        try (var service = createInferenceService()) {

            ActionListener<Model> modelListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

                var embeddingsModel = (VoyageAIEmbeddingsModel) model;
                assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
                assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
                assertThat(embeddingsModel.getTaskSettings(), equalTo(VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                modelListener
            );

        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedTaskType() throws IOException {
        try (var service = createInferenceService()) {
            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "The [voyageai] service does not support task type [sparse_embedding]"
            );

            service.parseRequestConfig(
                INFERENCE_ENTITY_ID_VALUE,
                TaskType.SPARSE_EMBEDDING,
                getRequestConfigMap(
                    VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                    VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                    getSecretSettingsMap(API_KEY_VALUE)
                ),
                failureListener
            );
        }
    }

    private static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.<Model>wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, instanceOf(exceptionClass));
            assertThat(e.getMessage(), is(expectedMessage));
        });
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createInferenceService()) {
            var config = getRequestConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );
            config.put("extra_key", "value");

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [voyageai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettings = VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model");
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                serviceSettings,
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [voyageai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        try (var service = createInferenceService()) {
            var taskSettingsMap = VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                taskSettingsMap,
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [voyageai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, config, failureListener);

        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        try (var service = createInferenceService()) {
            var secretSettingsMap = getSecretSettingsMap(API_KEY_VALUE);
            secretSettingsMap.put("extra_key", "value");

            var config = getRequestConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var failureListener = getModelListenerForException(
                ElasticsearchStatusException.class,
                "Configuration contains settings [{extra_key=value}] unknown to the [voyageai] service"
            );
            service.parseRequestConfig(INFERENCE_ENTITY_ID_VALUE, TaskType.TEXT_EMBEDDING, config, failureListener);
        }
    }

    public void testParsePersistedConfig_WithSecrets_CreatesAVoyageAIEmbeddingsModel() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_WithSecrets_CreatesAVoyageAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                createRandomChunkingSettingsMap(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_WithSecrets_CreatesAVoyageAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_WithSecrets_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("oldmodel"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig(
                    new UnparsedModel(
                        INFERENCE_ENTITY_ID_VALUE,
                        TaskType.SPARSE_EMBEDDING,
                        VoyageAIService.NAME,
                        persistedConfig.config(),
                        persistedConfig.secrets()
                    )
                )
            );

            assertThat(thrownException.getMessage(), containsString("Failed to parse stored model [id] for [voyageai] service"));
            assertThat(
                thrownException.getMessage(),
                containsString("The [voyageai] service does not support task type [sparse_embedding]")
            );
        }
    }

    public void testParsePersistedConfig_WithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH),
                getSecretSettingsMap(API_KEY_VALUE)
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings(InputType.SEARCH, null)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_WithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        try (var service = createInferenceService()) {
            var secretSettingsMap = getSecretSettingsMap(API_KEY_VALUE);
            secretSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_WithSecrets_NotThrowWhenAnExtraKeyExistsInSecrets() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                getSecretSettingsMap(API_KEY_VALUE)
            );
            persistedConfig.secrets().put("extra_key", "value");

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_WithSecrets_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettingsMap = VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model");
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty(),
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );
            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_WithSecrets_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createInferenceService()) {
            var taskSettingsMap = VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                taskSettingsMap,
                getSecretSettingsMap(API_KEY_VALUE)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings(InputType.SEARCH, null)));
            assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is(API_KEY_VALUE));
        }
    }

    public void testParsePersistedConfig_CreatesAVoyageAIEmbeddingsModel() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAVoyageAIEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                createRandomChunkingSettingsMap(),
                null
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_CreatesAVoyageAIEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_ThrowsErrorTryingToParseInvalidModel() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model_old"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty()
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfig(
                    new UnparsedModel(
                        INFERENCE_ENTITY_ID_VALUE,
                        TaskType.SPARSE_EMBEDDING,
                        VoyageAIService.NAME,
                        persistedConfig.config(),
                        new HashMap<>()
                    )
                )
            );

            assertThat(thrownException.getMessage(), containsString("Failed to parse stored model [id] for [voyageai] service"));
            assertThat(
                thrownException.getMessage(),
                containsString("The [voyageai] service does not support task type [sparse_embedding]")
            );
        }
    }

    public void testParsePersistedConfig_CreatesAVoyageAIEmbeddingsModelWithoutUrl() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(null)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        try (var service = createInferenceService()) {
            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMapEmpty()
            );
            persistedConfig.config().put("extra_key", "value");

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        try (var service = createInferenceService()) {
            var serviceSettingsMap = VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model");
            serviceSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                serviceSettingsMap,
                VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.SEARCH)
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings(InputType.SEARCH, null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testParsePersistedConfig_NotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        try (var service = createInferenceService()) {
            var taskSettingsMap = VoyageAIEmbeddingsTaskSettingsTests.getTaskSettingsMap(InputType.INGEST);
            taskSettingsMap.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(
                VoyageAIEmbeddingsServiceSettingsTests.buildServiceSettingsMap("model"),
                taskSettingsMap
            );

            var model = service.parsePersistedConfig(
                new UnparsedModel(
                    INFERENCE_ENTITY_ID_VALUE,
                    TaskType.TEXT_EMBEDDING,
                    VoyageAIService.NAME,
                    persistedConfig.config(),
                    persistedConfig.secrets()
                )
            );

            assertThat(model, instanceOf(VoyageAIEmbeddingsModel.class));

            var embeddingsModel = (VoyageAIEmbeddingsModel) model;
            assertThat(embeddingsModel.uri().toString(), is("https://api.voyageai.com/v1/embeddings"));
            assertThat(embeddingsModel.getServiceSettings().modelId(), is("model"));
            assertThat(embeddingsModel.getTaskSettings(), is(new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null)));
            assertNull(embeddingsModel.getSecretSettings());
        }
    }

    public void testInfer_ThrowsValidationErrorForInvalidInputType() throws IOException {
        var sender = createMockSender();

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var model = VoyageAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            API_KEY_VALUE,
            VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            10,
            1,
            "voyage-3-large"
        );

        try (var service = new VoyageAIService(factory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            service.infer(model, List.of(""), false, new HashMap<>(), InputType.CLUSTERING, null, listener);

            var thrownException = expectThrows(ValidationException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(thrownException.getMessage(), is("Validation Failed: 1: Input type [clustering] is not supported for [Voyage AI];"));

            verify(factory, times(1)).createSender();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testInfer_Embedding_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = VoyageAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "model",
                (SimilarityMeasure) null
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(model, List.of("abc"), false, new HashMap<>(), InputType.INGEST, null, listener);

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testRerankInfer_Rerank_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = VoyageAIRerankModelTests.createModel(getUrl(webServer), "model", 1024, false, false);
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.rerankInfer(
                model,
                new RerankRequest(fromStringList(List.of("candidate1", "candidate2")), InferenceString.ofText("query"), null, null, null),
                null,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Error message: [Unauthorized]"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_Embedding_Get_Response_Ingest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "voyage-3-large",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5
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

            var model = VoyageAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "voyage-3-large",
                (SimilarityMeasure) null
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(model, List.of("abc"), false, new HashMap<>(), InputType.INGEST, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        List.of("abc"),
                        "model",
                        "voyage-3-large",
                        "input_type",
                        "document",
                        "output_dtype",
                        "float",
                        "output_dimension",
                        1024
                    )
                )
            );
        }
    }

    public void testInfer_Embedding_Get_Response_Search() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "voyage-3-large",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5
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

            var model = VoyageAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "voyage-3-large",
                (SimilarityMeasure) null
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(model, List.of("abc"), false, new HashMap<>(), InputType.SEARCH, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        List.of("abc"),
                        "model",
                        "voyage-3-large",
                        "input_type",
                        "query",
                        "output_dtype",
                        "float",
                        "output_dimension",
                        1024
                    )
                )
            );
        }
    }

    public void testInfer_Embedding_Get_Response_NullInputType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "voyage-3-large",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5
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

            var model = VoyageAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
                1024,
                1024,
                "voyage-3-large",
                (SimilarityMeasure) null
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(model, List.of("abc"), false, new HashMap<>(), null, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertEquals(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F })), result.asMap());

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "voyage-3-large", "output_dtype", "float", "output_dimension", 1024))
            );
        }
    }

    public void testRerankInfer_NoReturnDocuments_NoTopN_NoTruncation() throws IOException {
        testRerankInfer(null, null, null);
    }

    public void testRerankInfer_ReturnDocuments_TopN_Truncation() throws IOException {
        testRerankInfer(randomIntBetween(1, 128), randomBoolean(), randomBoolean());
    }

    private void testRerankInfer(Integer topN, Boolean returnDocuments, Boolean truncation) throws IOException {
        String responseJson = """
            {
                "object": "list",
                "model": "model",
                "data": [
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

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            String modelId = randomAlphaOfLength(8);
            var model = VoyageAIRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, modelId, null, null, null);

            var query = "some query";
            var inputs = List.of("candidate1", "candidate2", "candidate3");
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            Map<String, Object> taskSettings = new HashMap<>();
            if (truncation != null) {
                taskSettings.put(TRUNCATION, truncation);
            }
            service.rerankInfer(
                model,
                new RerankRequest(fromStringList(inputs), InferenceString.ofText(query), topN, returnDocuments, taskSettings),
                null,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
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
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                equalTo(Strings.format("Bearer %s", API_KEY_VALUE))
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            Map<String, Object> expectedRequestMap = new HashMap<>(Map.of("query", query, "documents", inputs, "model", modelId));
            if (returnDocuments != null) {
                expectedRequestMap.put("return_documents", returnDocuments);
            }
            if (topN != null) {
                expectedRequestMap.put("top_k", topN);
            }
            if (truncation != null) {
                expectedRequestMap.put("truncation", truncation);
            }
            assertThat(requestMap, is(expectedRequestMap));
        }
    }

    public void testRerankInfer_ThrowsError_WithNonTextQuery() throws IOException {
        var textInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.of(DataType.TEXT)));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(textInputs, nonTextQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputs() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var textQuery = createRandomUsingDataTypes(EnumSet.of(DataType.TEXT));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, textQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputsAndQuery() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, nonTextQuery);
    }

    private void testRerankInfer_ThrowsError_WithNonTextInputOrQuery(List<InferenceString> inputs, InferenceString query)
        throws IOException {
        var model = mock(VoyageAIRerankModel.class);

        try (var service = createInferenceService()) {
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            service.rerankInfer(model, new RerankRequest(inputs, query, null, null, new HashMap<>()), null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
            assertThat(thrownException.getMessage(), is("The voyageai service does not support rerank with non-text inputs or queries"));
        }
    }

    public void testInfer_Embedding_DoesNotSetInputType_WhenNotPresentInTaskSettings_AndUnspecifiedIsPassedInRequest() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "model": "voyage-3-large",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5
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

            var model = VoyageAIEmbeddingsModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
                1024,
                1024,
                "voyage-3-large",
                (SimilarityMeasure) null
            );
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();
            service.infer(model, List.of("abc"), false, new HashMap<>(), InputType.UNSPECIFIED, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.123F, -0.123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        List.of("abc"),
                        "model",
                        "voyage-3-large",
                        "input_type",
                        "document",
                        "output_dtype",
                        "float",
                        "output_dimension",
                        1024
                    )
                )
            );
        }
    }

    public void test_Embedding_ChunkedInfer_BatchesCallsChunkingSettingsSet() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            API_KEY_VALUE,
            new VoyageAIEmbeddingsTaskSettings((InputType) null, null),
            createRandomChunkingSettings(),
            1024,
            1024,
            "voyage-3-large"
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    public void test_Embedding_ChunkedInfer_ChunkingSettingsNotSet() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(
            getUrl(webServer),
            API_KEY_VALUE,
            new VoyageAIEmbeddingsTaskSettings((InputType) null, null),
            null,
            1024,
            1024,
            "voyage-3-large"
        );

        test_Embedding_ChunkedInfer_BatchesCalls(model);
    }

    public void test_Embedding_ChunkedInfer_noInputs() throws IOException {
        var model = VoyageAIEmbeddingsModelTests.createModel(getUrl(webServer), API_KEY_VALUE, 1024, "voyage-3-large");
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            service.chunkedInfer(model, List.of(), new HashMap<>(), InputType.UNSPECIFIED, null, listener);

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, empty());
            assertThat(webServer.requests(), empty());
        }
    }

    private void test_Embedding_ChunkedInfer_BatchesCalls(VoyageAIEmbeddingsModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new VoyageAIService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            // Batching will call the service with 2 input
            String responseJson = """
                {
                    "model": "voyage-3-large",
                    "object": "list",
                    "usage": {
                        "total_tokens": 5
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

            TestPlainActionFuture<List<ChunkedInference>> listener = new TestPlainActionFuture<>();
            // 2 input
            service.chunkedInfer(
                model,
                List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
                new HashMap<>(),
                InputType.UNSPECIFIED,
                null,
                listener
            );

            var results = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(results, hasSize(2));
            {
                assertThat(results.getFirst(), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.getFirst();
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 1), floatResult.chunks().getFirst().offset());
                assertThat(floatResult.chunks().get(0).embedding(), CoreMatchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class));
                assertArrayEquals(
                    new float[] { 0.123f, -0.123f },
                    ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(ChunkedInferenceEmbedding.class));
                var floatResult = (ChunkedInferenceEmbedding) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(new ChunkedInference.TextOffset(0, 2), floatResult.chunks().getFirst().offset());
                assertThat(
                    floatResult.chunks().getFirst().embedding(),
                    CoreMatchers.instanceOf(DenseEmbeddingFloatResults.Embedding.class)
                );
                assertArrayEquals(
                    new float[] { 0.223f, -0.223f },
                    ((DenseEmbeddingFloatResults.Embedding) floatResult.chunks().get(0).embedding()).values(),
                    0.0f
                );
            }

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(Map.of("input", List.of("a", "bb"), "model", "voyage-3-large", "output_dtype", "float", "output_dimension", 1024))
            );
        }
    }

    public void testDefaultSimilarity() {
        assertEquals(SimilarityMeasure.DOT_PRODUCT, VoyageAIService.defaultSimilarity());
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void testGetConfiguration() throws Exception {
        try (var service = createInferenceService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                        "service": "voyageai",
                        "name": "Voyage AI",
                        "task_types": ["text_embedding", "rerank"],
                        "configurations": {
                            "model_id": {
                                "description": "The name of the model to use for the inference task.",
                                "label": "Model ID",
                                "required": true,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank"]
                            },
                            "api_key": {
                                "description": "API Key for the provider you're connecting to.",
                                "label": "API Key",
                                "required": true,
                                "sensitive": true,
                                "updatable": true,
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
                """);
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

    @Override
    public InferenceService createInferenceService() {
        return new VoyageAIService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public Model createEmbeddingModel(SimilarityMeasure similarity) {
        return VoyageAIEmbeddingsModelTests.createModel(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            null,
            randomAlphaOfLength(8),
            similarity
        );
    }

    @Override
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.noneOf(TaskType.class);
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("rerank-lite-1"), is(2800));
        assertThat(rerankingInferenceService.rerankerWindowSize("any other model"), is(5500));
    }

    public void testBuildModelFromConfigAndSecrets_TextEmbedding() throws IOException {
        var model = createTestModel(TaskType.TEXT_EMBEDDING);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_Rerank() throws IOException {
        var model = createTestModel(TaskType.RERANK);
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            INFERENCE_ENTITY_ID_VALUE,
            TaskType.CHAT_COMPLETION,
            VoyageAIService.NAME,
            mock(ServiceSettings.class)
        );
        try (var inferenceService = createInferenceService()) {
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> inferenceService.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                thrownException.getMessage(),
                is(Strings.format("The [%s] service does not support task type [%s]", VoyageAIService.NAME, TaskType.CHAT_COMPLETION))

            );
        }
    }

    private Model createTestModel(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> VoyageAIEmbeddingsModelTests.createModel(URL_VALUE, API_KEY_VALUE, null, MODEL_ID_VALUE);
            case RERANK -> VoyageAIRerankModelTests.createModel(MODEL_ID_VALUE, null);
            default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
        };
    }

    private void validateModelBuilding(Model model) throws IOException {
        try (var inferenceService = createInferenceService()) {
            var resultModel = inferenceService.buildModelFromConfigAndSecrets(model.getConfigurations(), model.getSecrets());
            assertThat(resultModel, is(model));
        }
    }
}
