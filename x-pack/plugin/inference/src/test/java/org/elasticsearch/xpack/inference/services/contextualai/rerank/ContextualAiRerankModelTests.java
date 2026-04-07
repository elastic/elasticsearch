/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ContextualAiRerankModelTests extends ESTestCase {

    private static final String MODEL_VALUE = "some_model";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String URL_INVALID_VALUE = "^^^";
    private static final String URL_DEFAULT_VALUE = "https://api.contextual.ai/v1/rerank";
    public static final String INFERENCE_ENTITY_ID = "inferenceEntityId";

    private static final long RATE_LIMIT_REQUESTS_PER_MINUTE_VALUE = 1000L;
    private static final int MODEL_TOP_N_VALUE = 5;
    private static final int REQUEST_TOP_N_OVERRIDE_VALUE = 10;
    private static final String ORIGINAL_INSTRUCTION_VALUE = "original instruction";
    private static final String NEW_INSTRUCTION_VALUE = "new instruction";
    private static final String TASK_SETTINGS_TOP_N_FIELD = "top_n";
    private static final String TASK_SETTINGS_INSTRUCTION_FIELD = "instruction";

    public static ContextualAiRerankModel createRerankModel(@Nullable String url, String apiKey, @Nullable String modelId) {
        var uri = url == null ? ServiceUtils.createUri(URL_DEFAULT_VALUE) : ServiceUtils.createUri(url);
        return new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(uri, modelId, new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE_VALUE))
            ),
            ContextualAiRerankTaskSettings.EMPTY_SETTINGS,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testCreateModel_NoUrl_DefaultUrl() {
        var model = new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID,
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_VALUE)),
            new HashMap<>(),
            new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, API_KEY_VALUE)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(model.getServiceSettings().uri().toString(), is(URL_DEFAULT_VALUE));
    }

    public void testCreateModel_InvalidUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> createRerankModel(URL_INVALID_VALUE, API_KEY_VALUE, MODEL_VALUE)
        );
        assertThat(
            thrownException.getMessage(),
            is(Strings.format("unable to parse url [%s]. Reason: Illegal character in path", URL_INVALID_VALUE))
        );
    }

    public void testOf_RequestOverridesTopN_KeepsModelInstruction() {
        var model = new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(
                    ServiceUtils.createUri(URL_VALUE),
                    MODEL_VALUE,
                    new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE_VALUE)
                )
            ),
            new ContextualAiRerankTaskSettings(MODEL_TOP_N_VALUE, ORIGINAL_INSTRUCTION_VALUE),
            new DefaultSecretSettings(new SecureString(API_KEY_VALUE.toCharArray()))
        );

        var merged = ContextualAiRerankModel.of(model, new HashMap<>(Map.of(TASK_SETTINGS_TOP_N_FIELD, REQUEST_TOP_N_OVERRIDE_VALUE)));

        assertThat(merged.getTaskSettings().getTopN(), is(REQUEST_TOP_N_OVERRIDE_VALUE));
        assertThat(merged.getTaskSettings().getInstruction(), is(ORIGINAL_INSTRUCTION_VALUE));
    }

    public void testOf_RequestOverridesInstruction_KeepsModelTopN() {
        var model = new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(
                    ServiceUtils.createUri(URL_VALUE),
                    MODEL_VALUE,
                    new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE_VALUE)
                )
            ),
            new ContextualAiRerankTaskSettings(MODEL_TOP_N_VALUE, ORIGINAL_INSTRUCTION_VALUE),
            new DefaultSecretSettings(new SecureString(API_KEY_VALUE.toCharArray()))
        );

        var merged = ContextualAiRerankModel.of(model, new HashMap<>(Map.of(TASK_SETTINGS_INSTRUCTION_FIELD, NEW_INSTRUCTION_VALUE)));

        assertThat(merged.getTaskSettings().getTopN(), is(MODEL_TOP_N_VALUE));
        assertThat(merged.getTaskSettings().getInstruction(), is(NEW_INSTRUCTION_VALUE));
    }

    public void testOf_EmptyTaskSettingsMap_ReturnsOriginalTaskSettingsInstance() {
        var model = new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(
                    ServiceUtils.createUri(URL_VALUE),
                    MODEL_VALUE,
                    new RateLimitSettings(RATE_LIMIT_REQUESTS_PER_MINUTE_VALUE)
                )
            ),
            new ContextualAiRerankTaskSettings(MODEL_TOP_N_VALUE, ORIGINAL_INSTRUCTION_VALUE),
            new DefaultSecretSettings(new SecureString(API_KEY_VALUE.toCharArray()))
        );

        var merged = ContextualAiRerankModel.of(model, new HashMap<>());

        assertThat(merged.getTaskSettings(), sameInstance(model.getTaskSettings()));
    }
}
