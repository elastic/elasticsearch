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
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<AzureOpenAiCompletionTaskSettings> {

    public static AzureOpenAiCompletionTaskSettings createRandomWithUser() {
        return new AzureOpenAiCompletionTaskSettings(randomAlphaOfLength(15), null);
    }

    public static AzureOpenAiCompletionTaskSettings createRandom() {
        return new AzureOpenAiCompletionTaskSettings(randomAlphaOfLengthOrNull(15), null);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        var updatedSettings = initialSettings.updatedTaskSettings(
            newSettings.user() == null ? Map.of() : Map.of(AzureOpenAiServiceFields.USER, newSettings.user())
        );

        assertEquals(newSettings.user() == null ? initialSettings.user() : newSettings.user(), updatedSettings.user());
    }

    public void testFromMap_WithUser() {
        var user = "user";

        assertThat(
            new AzureOpenAiCompletionTaskSettings(user, null),
            is(new AzureOpenAiCompletionTaskSettings(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, user)), ConfigurationParseContext.PERSISTENT))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> new AzureOpenAiCompletionTaskSettings(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "")), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = new AzureOpenAiCompletionTaskSettings(new HashMap<>(Map.of()), ConfigurationParseContext.PERSISTENT);
        assertNull(taskSettings.user());
    }

    public void testFromMap_WithRequestContext_ReturnsEmptySettings_WhenMapIsEmpty() {
        var settings = new AzureOpenAiCompletionTaskSettings(new HashMap<>(Map.of()), ConfigurationParseContext.REQUEST);
        assertTrue(settings.isEmpty());
        assertNull(settings.user());
        assertNull(settings.headers());
        assertThat(settings, is(AzureOpenAiCompletionTaskSettings.EMPTY));
    }

    public void testFromMap_WithRequestContext_ReturnsEmptySettings_WhenMapDoesNotContainKnownFields() {
        var settings = new AzureOpenAiCompletionTaskSettings(
            new HashMap<>(Map.of("key", "model")),
            ConfigurationParseContext.REQUEST
        );
        assertTrue(settings.isEmpty());
        assertNull(settings.user());
        assertNull(settings.headers());
        assertThat(settings, is(AzureOpenAiCompletionTaskSettings.EMPTY));
    }

    public void testFromMap_WithRequestContext_ReturnsUser() {
        var settings = new AzureOpenAiCompletionTaskSettings(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "user")),
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.user(), is("user"));
    }

    public void testFromMap_WithRequestContext_WhenUserIsEmpty_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> new AzureOpenAiCompletionTaskSettings(
                new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "")),
                ConfigurationParseContext.REQUEST
            )
        );
        assertThat(exception.getMessage(), containsString("[user] must be a non-empty string"));
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = new AzureOpenAiCompletionTaskSettings(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "user")),
            ConfigurationParseContext.PERSISTENT
        );

        var overriddenTaskSettings = AzureOpenAiCompletionTaskSettings.of(taskSettings, AzureOpenAiCompletionTaskSettings.EMPTY);
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var user = "user";
        var userOverride = "user override";

        var taskSettings = new AzureOpenAiCompletionTaskSettings(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, user)),
            ConfigurationParseContext.PERSISTENT
        );

        var requestTaskSettings = new AzureOpenAiCompletionTaskSettings(
            new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, userOverride)),
            ConfigurationParseContext.REQUEST
        );

        var overriddenTaskSettings = AzureOpenAiCompletionTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new AzureOpenAiCompletionTaskSettings(userOverride, null)));
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
        String user = randomValueOtherThan(instance.user(), () -> randomAlphaOfLengthOrNull(15));
        return new AzureOpenAiCompletionTaskSettings(user, instance.headers());
    }
}
