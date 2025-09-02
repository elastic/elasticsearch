/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_BUDGET_FIELD;
import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_CONFIG_FIELD;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiChatCompletionTaskSettingsTests extends InferenceSettingsTestCase<GoogleVertexAiChatCompletionTaskSettings> {

    public void testUpdatedTaskSettings_updatesTaskSettingsWhenDifferent() {
        var initialSettings = new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(123));
        int updatedThinkingBudget = 456;
        Map<String, Object> newSettingsMap = new HashMap<>(
            Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, updatedThinkingBudget)))
        );

        GoogleVertexAiChatCompletionTaskSettings updatedSettings = (GoogleVertexAiChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(newSettingsMap);
        assertThat(updatedSettings.thinkingConfig().getThinkingBudget(), is(updatedThinkingBudget));
    }

    public void testUpdatedTaskSettings_doesNotUpdateTaskSettingsWhenNewSettingsAreEmpty() {
        var initialSettings = new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(123));
        Map<String, Object> emptySettingsMap = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>()));

        GoogleVertexAiChatCompletionTaskSettings updatedSettings = (GoogleVertexAiChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(emptySettingsMap);
        assertThat(updatedSettings.thinkingConfig().getThinkingBudget(), is(initialSettings.thinkingConfig().getThinkingBudget()));
    }

    public void testFromMap_returnsSettings() {
        int thinkingBudget = 256;
        Map<String, Object> settings = new HashMap<>(
            Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, thinkingBudget)))
        );

        var result = GoogleVertexAiChatCompletionTaskSettings.fromMap(settings);
        assertThat(result.thinkingConfig().getThinkingBudget(), is(thinkingBudget));
    }

    public void testFromMap_throwsWhenValidationErrorEncountered() {
        Map<String, Object> settings = new HashMap<>(
            Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, "not_an_int")))
        );

        expectThrows(ValidationException.class, () -> GoogleVertexAiChatCompletionTaskSettings.fromMap(settings));
    }

    public void testOf_overridesOriginalSettings_whenNewSettingsPresent() {
        // Confirm we can overwrite empty settings
        var originalSettings = new GoogleVertexAiChatCompletionTaskSettings();
        int newThinkingBudget = 123;
        var newSettings = new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(newThinkingBudget));
        var updatedSettings = GoogleVertexAiChatCompletionTaskSettings.of(originalSettings, newSettings);

        assertThat(updatedSettings.thinkingConfig().getThinkingBudget(), is(newThinkingBudget));

        // Confirm we can overwrite existing settings
        int secondNewThinkingBudget = 456;
        var secondNewSettings = new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(secondNewThinkingBudget));
        var secondUpdatedSettings = GoogleVertexAiChatCompletionTaskSettings.of(updatedSettings, secondNewSettings);

        assertThat(secondUpdatedSettings.thinkingConfig().getThinkingBudget(), is(secondNewThinkingBudget));
    }

    public void testOf_doesNotOverrideOriginalSettings_whenNewSettingsNotPresent() {
        int originalThinkingBudget = 123;
        var originalSettings = new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(originalThinkingBudget));
        var emptySettings = new GoogleVertexAiChatCompletionTaskSettings();
        var updatedSettings = GoogleVertexAiChatCompletionTaskSettings.of(originalSettings, emptySettings);

        assertThat(updatedSettings.thinkingConfig().getThinkingBudget(), is(originalThinkingBudget));
    }

    @Override
    protected GoogleVertexAiChatCompletionTaskSettings fromMutableMap(Map<String, Object> mutableMap) {
        return GoogleVertexAiChatCompletionTaskSettings.fromMap(mutableMap);
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiChatCompletionTaskSettings> instanceReader() {
        return GoogleVertexAiChatCompletionTaskSettings::new;
    }

    @Override
    protected GoogleVertexAiChatCompletionTaskSettings createTestInstance() {
        return new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(randomInt()));
    }
}
