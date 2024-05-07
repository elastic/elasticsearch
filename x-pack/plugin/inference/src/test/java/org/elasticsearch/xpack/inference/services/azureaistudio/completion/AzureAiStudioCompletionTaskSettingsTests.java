/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_P_FIELD;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioCompletionTaskSettingsTests extends ESTestCase {

    public void testFromMap_AllValues() {
        var taskMap = getTaskSettingsMap(1.0f, 2.0f, true, 512);
        assertEquals(new AzureAiStudioCompletionTaskSettings(1.0f, 2.0f, true, 512), AzureAiStudioCompletionTaskSettings.fromMap(taskMap));
    }

    public void testFromMap_TemperatureIsInvalidValue_ThrowsStatusException() {
        var taskMap = getTaskSettingsMap(null, 2.0f, true, 512);
        taskMap.put(TEMPERATURE_FIELD, "invalid");

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> AzureAiStudioCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("field [temperature] is not of the expected type. The value [invalid] cannot be converted to a [Float]"))
        );
    }

    public void testFromMap_TopPIsInvalidValue_ThrowsStatusException() {
        var taskMap = getTaskSettingsMap(null, 2.0f, true, 512);
        taskMap.put(TOP_P_FIELD, "invalid");

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> AzureAiStudioCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("field [top_p] is not of the expected type. The value [invalid] cannot be converted to a [Float]"))
        );
    }

    public void testFromMap_DoSampleIsInvalidValue_ThrowsStatusException() {
        var taskMap = getTaskSettingsMap(null, 2.0f, true, 512);
        taskMap.put(DO_SAMPLE_FIELD, "invalid");

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> AzureAiStudioCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("field [do_sample] is not of the expected type. The value [invalid] cannot be converted to a [Boolean]"))
        );
    }

    public void testFromMap_MaxNewTokensIsInvalidValue_ThrowsStatusException() {
        var taskMap = getTaskSettingsMap(null, 2.0f, true, 512);
        taskMap.put(MAX_NEW_TOKENS_FIELD, "invalid");

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> AzureAiStudioCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("field [max_new_tokens] is not of the expected type. The value [invalid] cannot be converted to a [Integer]"))
        );
    }

    public void testFromMap_WithNoValues_DoesNotThrowException() {
        var taskMap = AzureAiStudioCompletionTaskSettings.fromMap(new HashMap<String, Object>(Map.of()));
        assertNull(taskMap.temperature());
        assertNull(taskMap.topP());
        assertNull(taskMap.doSample());
        assertNull(taskMap.maxNewTokens());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var settings = AzureAiStudioCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0f, 2.0f, true, 512));
        var overrideSettings = AzureAiStudioCompletionTaskSettings.of(settings, AzureAiStudioCompletionRequestTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overrideSettings, is(settings));
    }

    public void testOverrideWith_UsesTemperatureOverride() {
        var settings = AzureAiStudioCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0f, 2.0f, true, 512));
        var overrideSettings = AzureAiStudioCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(5.3f, null, null, null));
        var overriddenTaskSettings = AzureAiStudioCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioCompletionTaskSettings(5.3f, 2.0f, true, 512)));
    }

    public void testOverrideWith_UsesTopPOverride() {
        var settings = AzureAiStudioCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0f, 2.0f, true, 512));
        var overrideSettings = AzureAiStudioCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(null, 0.2f, null, null));
        var overriddenTaskSettings = AzureAiStudioCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioCompletionTaskSettings(1.0f, 0.2f, true, 512)));
    }

    public void testOverrideWith_UsesDoSampleOverride() {
        var settings = AzureAiStudioCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0f, 2.0f, true, 512));
        var overrideSettings = AzureAiStudioCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(null, null, false, null));
        var overriddenTaskSettings = AzureAiStudioCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioCompletionTaskSettings(1.0f, 2.0f, false, 512)));
    }

    public void testOverrideWith_UsesMaxNewTokensOverride() {
        var settings = AzureAiStudioCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0f, 2.0f, true, 512));
        var overrideSettings = AzureAiStudioCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(null, null, null, 128));
        var overriddenTaskSettings = AzureAiStudioCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioCompletionTaskSettings(1.0f, 2.0f, true, 128)));
    }

    public static Map<String, Object> getTaskSettingsMap(
        @Nullable Float temperature,
        @Nullable Float topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens
    ) {
        var map = new HashMap<String, Object>();

        if (temperature != null) {
            map.put(TEMPERATURE_FIELD, temperature);
        }

        if (topP != null) {
            map.put(TOP_P_FIELD, topP);
        }

        if (doSample != null) {
            map.put(DO_SAMPLE_FIELD, doSample);
        }

        if (maxNewTokens != null) {
            map.put(MAX_NEW_TOKENS_FIELD, maxNewTokens);
        }

        return map;
    }
}
