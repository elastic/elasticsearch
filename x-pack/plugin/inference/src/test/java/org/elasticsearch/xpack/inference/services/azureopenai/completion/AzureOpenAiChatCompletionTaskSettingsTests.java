/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AzureOpenAiChatCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<AzureOpenAiChatCompletionTaskSettings> {

    public static AzureOpenAiChatCompletionTaskSettings createRandomWithUser() {
        return new AzureOpenAiChatCompletionTaskSettings(randomAlphaOfLength(15));
    }

    public static AzureOpenAiChatCompletionTaskSettings createRandom() {
        var user = randomBoolean() ? randomAlphaOfLength(15) : null;
        return new AzureOpenAiChatCompletionTaskSettings(user);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        AzureOpenAiChatCompletionTaskSettings updatedSettings = (AzureOpenAiChatCompletionTaskSettings) initialSettings.updatedTaskSettings(
            newSettings.user() == null ? Map.of() : Map.of(AzureOpenAiServiceFields.USER, newSettings.user())
        );

        assertEquals(newSettings.user() == null ? initialSettings.user() : newSettings.user(), updatedSettings.user());
    }

    public void testFromMap_WithUser() {
        var user = "user";

        assertThat(
            new AzureOpenAiChatCompletionTaskSettings(user),
            is(AzureOpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, user))))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = AzureOpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = AzureOpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "user")));

        var overriddenTaskSettings = AzureOpenAiChatCompletionTaskSettings.of(
            taskSettings,
            AzureOpenAiChatCompletionRequestTaskSettings.EMPTY_SETTINGS
        );
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var user = "user";
        var userOverride = "user override";

        var taskSettings = AzureOpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, user)));

        var requestTaskSettings = AzureOpenAiChatCompletionRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, userOverride))
        );

        var overriddenTaskSettings = AzureOpenAiChatCompletionTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new AzureOpenAiChatCompletionTaskSettings(userOverride)));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiChatCompletionTaskSettings> instanceReader() {
        return AzureOpenAiChatCompletionTaskSettings::new;
    }

    @Override
    protected AzureOpenAiChatCompletionTaskSettings createTestInstance() {
        return createRandomWithUser();
    }

    @Override
    protected AzureOpenAiChatCompletionTaskSettings mutateInstance(AzureOpenAiChatCompletionTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureOpenAiChatCompletionTaskSettingsTests::createRandomWithUser);
    }
}
