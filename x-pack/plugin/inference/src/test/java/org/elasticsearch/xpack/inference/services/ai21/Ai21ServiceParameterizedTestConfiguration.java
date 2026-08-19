/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceParameterizedTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

public class Ai21ServiceParameterizedTestConfiguration {

    private static final String MODEL_ID_VALUE = "model_id";
    private static final int REQUESTS_PER_MINUTE_VALUE = 123;
    private static final String API_KEY_VALUE = "secret";

    public static AbstractInferenceServiceParameterizedTests.TestConfiguration createTestConfiguration() {
        return new AbstractInferenceServiceParameterizedTests.TestConfiguration(
            EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION),
            Ai21Service.NAME
        ) {

            @Override
            protected Ai21Service createService(ThreadPool threadPool, HttpClientManager clientManager) {
                var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
                return new Ai21Service(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
            }

            @Override
            protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType) {
                return new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID_VALUE));
            }

            @Override
            protected Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                var minimalSettings = createMinimalServiceSettingsMap(taskType);
                RateLimitSettingsTests.addRateLimitSettingsToMap(minimalSettings, REQUESTS_PER_MINUTE_VALUE);
                return minimalSettings;
            }

            @Override
            protected ModelSecrets createModelSecrets(ConfigurationParseContext context) {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap(), context));
            }

            @Override
            protected Map<String, Object> createAllTaskSettingsMap(TaskType taskType) {
                return new HashMap<>();
            }

            @Override
            protected ServiceSettings getServiceSettings(
                Map<String, Object> serviceSettings,
                TaskType taskType,
                ConfigurationParseContext context
            ) {
                return switch (taskType) {
                    case COMPLETION, CHAT_COMPLETION -> Ai21ChatCompletionServiceSettings.fromMap(serviceSettings, context);
                    default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
                };
            }

            @Override
            protected TaskSettings getEmptyTaskSettings(TaskType taskType) {
                return switch (taskType) {
                    case COMPLETION, CHAT_COMPLETION -> EmptyTaskSettings.INSTANCE;
                    default -> throw new IllegalArgumentException("Unsupported task type: " + taskType);
                };
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, API_KEY_VALUE));
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings) {
                switch (taskType) {
                    case COMPLETION -> assertCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    case CHAT_COMPLETION -> assertChatCompletionModel(model, modelIncludesSecrets, minimalSettings);
                    default -> fail("unexpected task type [" + taskType + "]");
                }
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
            }
        };
    }

    private static Ai21Model assertCommonModelFields(Model model, boolean modelIncludesSecrets, boolean minimalServiceSettings) {
        assertThat(model, instanceOf(Ai21Model.class));

        var ai21Model = (Ai21Model) model;
        assertThat(ai21Model.uri().toString(), is("https://api.ai21.com/studio/v1/chat/completions"));
        assertThat(ai21Model.getServiceSettings().modelId(), is(MODEL_ID_VALUE));

        // Task settings are always empty since Ai21 has no supported task settings
        assertThat(ai21Model.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
        if (minimalServiceSettings) {
            // Check default values
            assertThat(ai21Model.rateLimitSettings(), is(new RateLimitSettings(200)));
        } else {
            // Check configured values
            assertThat(ai21Model.rateLimitSettings(), is(new RateLimitSettings(REQUESTS_PER_MINUTE_VALUE)));
        }

        if (modelIncludesSecrets) {
            assertThat(ai21Model.getSecretSettings().apiKey(), is(new SecureString(API_KEY_VALUE.toCharArray())));
        } else {
            assertThat(ai21Model.getSecretSettings(), nullValue());
        }

        return ai21Model;
    }

    private static void assertCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalServiceSettings) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets, minimalServiceSettings);
        assertThat(customModel.getTaskType(), is(TaskType.COMPLETION));
    }

    private static void assertChatCompletionModel(Model model, boolean modelIncludesSecrets, boolean minimalServiceSettings) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets, minimalServiceSettings);
        assertThat(customModel.getTaskType(), is(TaskType.CHAT_COMPLETION));
    }
}
