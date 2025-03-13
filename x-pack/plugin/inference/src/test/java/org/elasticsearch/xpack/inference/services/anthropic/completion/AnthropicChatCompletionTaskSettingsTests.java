/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AnthropicChatCompletionTaskSettingsTests extends AbstractBWCWireSerializationTestCase<AnthropicChatCompletionTaskSettings> {

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        AnthropicChatCompletionTaskSettings updatedSettings = (AnthropicChatCompletionTaskSettings) initialSettings.updatedTaskSettings(
            Map.of(
                AnthropicServiceFields.MAX_TOKENS,
                newSettings.maxTokens(),
                AnthropicServiceFields.TEMPERATURE_FIELD,
                newSettings.temperature(),
                AnthropicServiceFields.TOP_P_FIELD,
                newSettings.topP(),
                AnthropicServiceFields.TOP_K_FIELD,
                newSettings.topK()
            )
        );

        assertEquals(newSettings, updatedSettings);
    }

    public static Map<String, Object> getChatCompletionTaskSettingsMap(
        @Nullable Integer maxTokens,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Integer topK
    ) {
        var map = new HashMap<String, Object>();

        if (maxTokens != null) {
            map.put(AnthropicServiceFields.MAX_TOKENS, maxTokens);
        }

        if (temperature != null) {
            map.put(AnthropicServiceFields.TEMPERATURE_FIELD, temperature);
        }

        if (topP != null) {
            map.put(AnthropicServiceFields.TOP_P_FIELD, topP);
        }

        if (topK != null) {
            map.put(AnthropicServiceFields.TOP_K_FIELD, topK);
        }

        return map;
    }

    public static AnthropicChatCompletionTaskSettings createRandom() {
        return new AnthropicChatCompletionTaskSettings(randomNonNegativeInt(), randomDouble(), randomDouble(), randomInt());
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testFromMap_WithMaxTokens() {
        assertEquals(
            new AnthropicChatCompletionTaskSettings(1, null, null, null),
            AnthropicChatCompletionTaskSettings.fromMap(
                getChatCompletionTaskSettingsMap(1, null, null, null),
                ConfigurationParseContext.REQUEST
            )
        );
    }

    public void testFromMap_AllValues() {
        assertEquals(
            new AnthropicChatCompletionTaskSettings(1, -1.1, 2.2, 3),
            AnthropicChatCompletionTaskSettings.fromMap(
                getChatCompletionTaskSettingsMap(1, -1.1, 2.2, 3),
                ConfigurationParseContext.REQUEST
            )
        );
    }

    public void testFromMap_WithoutMaxTokens_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AnthropicChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            is("Validation Failed: 1: [task_settings] does not contain the required setting [max_tokens];")
        );
    }

    public void testOf_KeepsOriginalValuesWithOverridesAreEmpty() {
        var taskSettings = new AnthropicChatCompletionTaskSettings(1, null, null, null);

        var overriddenTaskSettings = AnthropicChatCompletionTaskSettings.of(
            taskSettings,
            AnthropicChatCompletionRequestTaskSettings.EMPTY_SETTINGS
        );
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOf_UsesOverriddenSettings() {
        var taskSettings = new AnthropicChatCompletionTaskSettings(1, -1.2, 2.1, 3);

        var requestTaskSettings = new AnthropicChatCompletionRequestTaskSettings(2, 3.0, 4.0, 4);

        var overriddenTaskSettings = AnthropicChatCompletionTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new AnthropicChatCompletionTaskSettings(2, 3.0, 4.0, 4)));
    }

    @Override
    protected AnthropicChatCompletionTaskSettings mutateInstanceForVersion(
        AnthropicChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AnthropicChatCompletionTaskSettings> instanceReader() {
        return AnthropicChatCompletionTaskSettings::new;
    }

    @Override
    protected AnthropicChatCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AnthropicChatCompletionTaskSettings mutateInstance(AnthropicChatCompletionTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
