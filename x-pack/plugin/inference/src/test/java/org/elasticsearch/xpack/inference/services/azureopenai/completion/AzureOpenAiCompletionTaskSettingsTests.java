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

public class AzureOpenAiCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<AzureOpenAiCompletionTaskSettings> {

    public static AzureOpenAiCompletionTaskSettings createRandomWithUser() {
        return new AzureOpenAiCompletionTaskSettings(randomAlphaOfLength(15));
    }

    public static AzureOpenAiCompletionTaskSettings createRandom() {
        var user = randomBoolean() ? randomAlphaOfLength(15) : null;
        return new AzureOpenAiCompletionTaskSettings(user);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        AzureOpenAiCompletionTaskSettings updatedSettings = (AzureOpenAiCompletionTaskSettings) initialSettings.updatedTaskSettings(
            newSettings.user() == null ? Map.of() : Map.of(AzureOpenAiServiceFields.USER, newSettings.user())
        );

        assertEquals(newSettings.user() == null ? initialSettings.user() : newSettings.user(), updatedSettings.user());
    }

    public void testFromMap_WithUser() {
        var user = "user";

        assertThat(
            new AzureOpenAiCompletionTaskSettings(user),
            is(AzureOpenAiCompletionTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, user))))
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
        var taskSettings = AzureOpenAiCompletionTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = AzureOpenAiCompletionTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "user")));

        var overriddenTaskSettings = AzureOpenAiCompletionTaskSettings.of(
            taskSettings,
            AzureOpenAiCompletionRequestTaskSettings.EMPTY_SETTINGS
        );
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var user = "user";
        var userOverride = "user override";

        var taskSettings = AzureOpenAiCompletionTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, user)));

        var requestTaskSettings = AzureOpenAiCompletionRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, userOverride))
        );

        var overriddenTaskSettings = AzureOpenAiCompletionTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new AzureOpenAiCompletionTaskSettings(userOverride)));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiCompletionTaskSettings> instanceReader() {
        return AzureOpenAiCompletionTaskSettings::new;
    }

    @Override
    protected AzureOpenAiCompletionTaskSettings createTestInstance() {
        return createRandomWithUser();
    }

    @Override
    protected AzureOpenAiCompletionTaskSettings mutateInstance(AzureOpenAiCompletionTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureOpenAiCompletionTaskSettingsTests::createRandomWithUser);
    }
}
