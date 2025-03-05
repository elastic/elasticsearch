/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OpenAiChatCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<OpenAiChatCompletionTaskSettings> {

    public static OpenAiChatCompletionTaskSettings createRandomWithUser() {
        return new OpenAiChatCompletionTaskSettings(randomAlphaOfLength(15));
    }

    public void testIsEmpty() {
        var randomSettings = new OpenAiChatCompletionTaskSettings(randomBoolean() ? null : "username");
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandomWithUser();
        var newSettings = createRandomWithUser();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.user() != null) {
            newSettingsMap.put(OpenAiServiceFields.USER, newSettings.user());
        }
        OpenAiChatCompletionTaskSettings updatedSettings = (OpenAiChatCompletionTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.user() == null) {
            assertEquals(initialSettings.user(), updatedSettings.user());
        } else {
            assertEquals(newSettings.user(), updatedSettings.user());
        }
    }

    public void testFromMap_WithUser() {
        assertEquals(
            new OpenAiChatCompletionTaskSettings("user"),
            OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "")))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));

        var overriddenTaskSettings = OpenAiChatCompletionTaskSettings.of(
            taskSettings,
            OpenAiChatCompletionRequestTaskSettings.EMPTY_SETTINGS
        );
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));

        var requestTaskSettings = OpenAiChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user2")));

        var overriddenTaskSettings = OpenAiChatCompletionTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new OpenAiChatCompletionTaskSettings("user2")));
    }

    @Override
    protected Writeable.Reader<OpenAiChatCompletionTaskSettings> instanceReader() {
        return OpenAiChatCompletionTaskSettings::new;
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createTestInstance() {
        return createRandomWithUser();
    }

    @Override
    protected OpenAiChatCompletionTaskSettings mutateInstance(OpenAiChatCompletionTaskSettings instance) throws IOException {
        return createRandomWithUser();
    }
}
