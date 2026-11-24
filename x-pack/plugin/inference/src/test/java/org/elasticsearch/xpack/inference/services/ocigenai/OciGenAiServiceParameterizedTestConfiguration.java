/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceParameterizedTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModel;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.junit.Assert;

import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.TaskType.CHAT_COMPLETION;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * The {@link AbstractInferenceServiceParameterizedTests.TestConfiguration} of the OCI Generative AI service. Minimal settings target the
 * public regional endpoint of a region; the full settings target an explicit {@code url} and a dedicated AI cluster endpoint, with every
 * optional field set.
 */
public class OciGenAiServiceParameterizedTestConfiguration {

    private static final String MODEL_ID_VALUE = "cohere.embed-v4.0";
    private static final String URL_VALUE = "https://inference.generativeai.example.com";
    private static final String API_VERSION_VALUE = "20260101";
    private static final int DIMENSIONS_VALUE = 1536;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;
    private static final SimilarityMeasure SIMILARITY_VALUE = SimilarityMeasure.DOT_PRODUCT;
    private static final int REQUESTS_PER_MINUTE = 123;
    private static final InputType INPUT_TYPE_VALUE = InputType.SEARCH;
    private static final Truncation TRUNCATION_VALUE = Truncation.END;
    private static final int TOP_N_VALUE = 5;
    private static final boolean RETURN_DOCUMENTS_VALUE = true;

    private static final String REGIONAL_URI_TEMPLATE = "https://inference.generativeai.%s.oci.oraclecloud.com/%s/actions/%s";

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TEXT_EMBEDDING, COMPLETION, CHAT_COMPLETION, RERANK),
            OciGenAiService.NAME
        ) {

            @Override
            protected OciGenAiService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new OciGenAiService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var map = new HashMap<String, Object>(
                    Map.of(
                        OciGenAiServiceFields.REGION,
                        OciGenAiTestUtils.REGION_VALUE,
                        OciGenAiServiceFields.COMPARTMENT_ID,
                        OciGenAiTestUtils.COMPARTMENT_ID,
                        MODEL_ID,
                        MODEL_ID_VALUE
                    )
                );
                // dimensions_set_by_user is written when the endpoint is stored, and is rejected as unknown in requests
                if (taskType == TEXT_EMBEDDING && parseContext == ConfigurationParseContext.PERSISTENT) {
                    map.put(ServiceFields.DIMENSIONS_SET_BY_USER, false);
                }
                return map;
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var map = new HashMap<String, Object>(
                    Map.of(
                        URL,
                        URL_VALUE,
                        OciGenAiServiceFields.COMPARTMENT_ID,
                        OciGenAiTestUtils.COMPARTMENT_ID,
                        MODEL_ID,
                        MODEL_ID_VALUE,
                        OciGenAiServiceFields.ENDPOINT_ID,
                        OciGenAiTestUtils.ENDPOINT_ID_VALUE,
                        OciGenAiServiceFields.API_VERSION,
                        API_VERSION_VALUE
                    )
                );
                RateLimitSettingsTests.addRateLimitSettingsToMap(map, REQUESTS_PER_MINUTE);
                if (taskType == TEXT_EMBEDDING) {
                    map.put(DIMENSIONS, DIMENSIONS_VALUE);
                    map.put(MAX_INPUT_TOKENS, MAX_INPUT_TOKENS_VALUE);
                    map.put(SIMILARITY, SIMILARITY_VALUE.toString());
                    if (parseContext == ConfigurationParseContext.PERSISTENT) {
                        map.put(ServiceFields.DIMENSIONS_SET_BY_USER, true);
                    }
                }
                return map;
            }

            @Override
            protected ServiceSettings getServiceSettings(
                Map<String, Object> serviceSettings,
                TaskType taskType,
                ConfigurationParseContext context
            ) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> OciGenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context);
                    case COMPLETION, CHAT_COMPLETION -> OciGenAiChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    case RERANK -> OciGenAiRerankServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case TEXT_EMBEDDING -> OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS;
                    case COMPLETION, CHAT_COMPLETION -> OciGenAiChatCompletionTaskSettings.EMPTY_SETTINGS;
                    case RERANK -> OciGenAiRerankTaskSettings.EMPTY_SETTINGS;
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(OciGenAiSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                var map = new HashMap<String, Object>();
                switch (taskType) {
                    case TEXT_EMBEDDING -> {
                        map.put(OciGenAiServiceFields.INPUT_TYPE, INPUT_TYPE_VALUE.toString());
                        map.put(OciGenAiServiceFields.TRUNCATE, TRUNCATION_VALUE.toString());
                    }
                    case RERANK -> {
                        map.put(OciGenAiServiceFields.TOP_N, TOP_N_VALUE);
                        map.put(OciGenAiServiceFields.RETURN_DOCUMENTS, RETURN_DOCUMENTS_VALUE);
                    }
                    case COMPLETION, CHAT_COMPLETION -> {
                        // the chat task settings have no fields
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                }
                return map;
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return OciGenAiTestUtils.secretSettingsMap();
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                assertModel(model, taskType, modelIncludesSecrets, minimalSettings, ConfigurationParseContext.REQUEST);
            }

            @Override
            protected void assertModel(
                Model model,
                TaskType taskType,
                boolean modelIncludesSecrets,
                boolean minimalSettings,
                ConfigurationParseContext parseContext
            ) {
                switch (taskType) {
                    case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets, minimalSettings);
                    case COMPLETION, CHAT_COMPLETION -> assertChatCompletionModel(model, taskType, modelIncludesSecrets, minimalSettings);
                    case RERANK -> assertRerankModel(model, modelIncludesSecrets, minimalSettings);
                    default -> Assert.fail("unexpected task type [" + taskType + "]");
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(COMPLETION, CHAT_COMPLETION);
            }
        };
    }

    private static void assertCommonSettings(OciGenAiModel model, boolean modelIncludesSecrets, String action, boolean minimalSettings) {
        var serviceSettings = model.getServiceSettings();
        assertThat(serviceSettings.compartmentId(), is(OciGenAiTestUtils.COMPARTMENT_ID));
        assertThat(serviceSettings.modelId(), is(MODEL_ID_VALUE));

        if (minimalSettings) {
            assertThat(serviceSettings.region(), is(OciGenAiTestUtils.REGION_VALUE));
            assertThat(serviceSettings.uri(), is(nullValue()));
            assertThat(serviceSettings.endpointId(), is(nullValue()));
            assertThat(serviceSettings.apiVersion(), is(OciGenAiUtils.DEFAULT_API_VERSION));
            assertThat(serviceSettings.rateLimitSettings(), is(OciGenAiServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS));
            assertThat(
                model.uri(),
                is(
                    URI.create(
                        Strings.format(REGIONAL_URI_TEMPLATE, OciGenAiTestUtils.REGION_VALUE, OciGenAiUtils.DEFAULT_API_VERSION, action)
                    )
                )
            );
        } else {
            assertThat(serviceSettings.region(), is(nullValue()));
            assertThat(serviceSettings.uri(), is(URI.create(URL_VALUE)));
            assertThat(serviceSettings.endpointId(), is(OciGenAiTestUtils.ENDPOINT_ID_VALUE));
            assertThat(serviceSettings.apiVersion(), is(API_VERSION_VALUE));
            assertThat(serviceSettings.rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE)));
            assertThat(model.uri(), is(URI.create(URL_VALUE + "/" + API_VERSION_VALUE + "/actions/" + action)));
        }

        if (modelIncludesSecrets) {
            assertThat(model.getSecretSettings().keyId(), is(OciGenAiTestUtils.keyId()));
            assertThat(model.getSecretSettings().privateKey().toString(), is(OciGenAiTestUtils.privateKeyPem()));
        } else {
            assertThat(model.getSecretSettings(), is(nullValue()));
        }
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(OciGenAiEmbeddingsModel.class));
        var embeddingsModel = (OciGenAiEmbeddingsModel) model;
        assertThat(embeddingsModel.getTaskType(), is(TEXT_EMBEDDING));
        assertCommonSettings(embeddingsModel, modelIncludesSecrets, OciGenAiUtils.EMBED_TEXT, minimalSettings);

        var serviceSettings = embeddingsModel.getServiceSettings();
        var taskSettings = embeddingsModel.getTaskSettings();
        if (minimalSettings) {
            assertThat(serviceSettings.dimensions(), is(nullValue()));
            assertThat(serviceSettings.dimensionsSetByUser(), is(false));
            assertThat(serviceSettings.maxInputTokens(), is(nullValue()));
            assertThat(serviceSettings.similarity(), is(nullValue()));
            assertThat(taskSettings, is(OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS));
        } else {
            assertThat(serviceSettings.dimensions(), is(DIMENSIONS_VALUE));
            assertThat(serviceSettings.dimensionsSetByUser(), is(true));
            assertThat(serviceSettings.maxInputTokens(), is(MAX_INPUT_TOKENS_VALUE));
            assertThat(serviceSettings.similarity(), is(SIMILARITY_VALUE));
            assertThat(taskSettings.getInputType(), is(INPUT_TYPE_VALUE));
            assertThat(taskSettings.getTruncation(), is(TRUNCATION_VALUE));
        }
    }

    private static void assertChatCompletionModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(OciGenAiChatCompletionModel.class));
        var chatModel = (OciGenAiChatCompletionModel) model;
        assertThat(chatModel.getTaskType(), is(taskType));
        assertCommonSettings(chatModel, modelIncludesSecrets, OciGenAiUtils.CHAT, minimalSettings);
        assertThat(chatModel.getServiceSettings().apiFormat(), is(OciGenAiChatApiFormat.COHERE));
        assertThat(chatModel.getTaskSettings(), is(OciGenAiChatCompletionTaskSettings.EMPTY_SETTINGS));
    }

    private static void assertRerankModel(Model model, boolean modelIncludesSecrets, boolean minimalSettings) {
        assertThat(model, instanceOf(OciGenAiRerankModel.class));
        var rerankModel = (OciGenAiRerankModel) model;
        assertThat(rerankModel.getTaskType(), is(RERANK));
        assertCommonSettings(rerankModel, modelIncludesSecrets, OciGenAiUtils.RERANK_TEXT, minimalSettings);
        if (minimalSettings) {
            assertThat(rerankModel.getTaskSettings(), is(OciGenAiRerankTaskSettings.EMPTY_SETTINGS));
        } else {
            assertThat(rerankModel.getTaskSettings().getTopN(), is(TOP_N_VALUE));
            assertThat(rerankModel.getTaskSettings().getReturnDocuments(), is(RETURN_DOCUMENTS_VALUE));
        }
    }
}
