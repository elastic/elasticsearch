/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.USER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<AzureOpenAiEmbeddingsTaskSettings> {

    public static AzureOpenAiEmbeddingsTaskSettings createRandomWithUser() {
        return new AzureOpenAiEmbeddingsTaskSettings(randomAlphaOfLength(15), null);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    /**
     * The created settings can have the user set to null.
     */
    public static AzureOpenAiEmbeddingsTaskSettings createRandom() {
        return new AzureOpenAiEmbeddingsTaskSettings(randomAlphaOfLengthOrNull(15), null);
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        var updatedSettings = initialSettings.updatedTaskSettings(
            newSettings.user() == null ? Map.of() : Map.of(USER, newSettings.user())
        );

        if (newSettings.user() == null) {
            assertEquals(initialSettings.user(), updatedSettings.user());
        } else {
            assertEquals(newSettings.user(), updatedSettings.user());
        }
    }

    public void testFromMap_WithUser() {
        assertEquals(
            new AzureOpenAiEmbeddingsTaskSettings("user", null),
            new AzureOpenAiEmbeddingsTaskSettings(new HashMap<>(Map.of(USER, "user")), ConfigurationParseContext.PERSISTENT)
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> new AzureOpenAiEmbeddingsTaskSettings(new HashMap<>(Map.of(USER, "")), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = new AzureOpenAiEmbeddingsTaskSettings(new HashMap<>(Map.of()), ConfigurationParseContext.PERSISTENT);
        assertNull(taskSettings.user());
    }

    public void testFromMap_WithRequestContext_ReturnsEmptySettings_WhenMapIsEmpty() {
        var settings = new AzureOpenAiEmbeddingsTaskSettings(new HashMap<>(Map.of()), ConfigurationParseContext.REQUEST);
        assertTrue(settings.isEmpty());
        assertNull(settings.user());
        assertNull(settings.headers());
        MatcherAssert.assertThat(settings, is(AzureOpenAiEmbeddingsTaskSettings.EMPTY));
    }

    public void testFromMap_WithRequestContext_ReturnsEmptySettings_WhenMapDoesNotContainKnownFields() {
        var settings = new AzureOpenAiEmbeddingsTaskSettings(
            new HashMap<>(Map.of("key", "model")),
            ConfigurationParseContext.REQUEST
        );
        assertTrue(settings.isEmpty());
        assertNull(settings.user());
        assertNull(settings.headers());
        MatcherAssert.assertThat(settings, is(AzureOpenAiEmbeddingsTaskSettings.EMPTY));
    }

    public void testFromMap_WithRequestContext_ReturnsUser() {
        var settings = new AzureOpenAiEmbeddingsTaskSettings(
            new HashMap<>(Map.of(USER, "user")),
            ConfigurationParseContext.REQUEST
        );
        MatcherAssert.assertThat(settings.user(), is("user"));
    }

    public void testFromMap_WithRequestContext_WhenUserIsEmpty_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> new AzureOpenAiEmbeddingsTaskSettings(
                new HashMap<>(Map.of(USER, "")),
                ConfigurationParseContext.REQUEST
            )
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("[user] must be a non-empty string"));
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = new AzureOpenAiEmbeddingsTaskSettings(
            new HashMap<>(Map.of(USER, "user")),
            ConfigurationParseContext.PERSISTENT
        );

        var overriddenTaskSettings = AzureOpenAiEmbeddingsTaskSettings.of(taskSettings, AzureOpenAiEmbeddingsTaskSettings.EMPTY);
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = new AzureOpenAiEmbeddingsTaskSettings(
            new HashMap<>(Map.of(USER, "user")),
            ConfigurationParseContext.PERSISTENT
        );

        var requestTaskSettings = new AzureOpenAiEmbeddingsTaskSettings(
            new HashMap<>(Map.of(USER, "user2")),
            ConfigurationParseContext.REQUEST
        );

        var overriddenTaskSettings = AzureOpenAiEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureOpenAiEmbeddingsTaskSettings("user2", null)));
    }

    @Override
    protected Writeable.Reader<AzureOpenAiEmbeddingsTaskSettings> instanceReader() {
        return AzureOpenAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected AzureOpenAiEmbeddingsTaskSettings createTestInstance() {
        return createRandomWithUser();
    }

    @Override
    protected AzureOpenAiEmbeddingsTaskSettings mutateInstance(AzureOpenAiEmbeddingsTaskSettings instance) throws IOException {
        String user = randomValueOtherThan(instance.user(), () -> randomAlphaOfLengthOrNull(15));
        return new AzureOpenAiEmbeddingsTaskSettings(user, instance.headers());
    }

    public static Map<String, Object> getAzureOpenAiRequestTaskSettingsMap(@Nullable String user) {
        return createRequestTaskSettingsMap(user);
    }

    /** Helper for tests that need a task-settings map (e.g. request overrides). */
    public static Map<String, Object> createRequestTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(USER, user);
        }

        return map;
    }
}
