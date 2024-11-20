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
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields.USER;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<AzureOpenAiEmbeddingsTaskSettings> {

    public static AzureOpenAiEmbeddingsTaskSettings createRandomWithUser() {
        return new AzureOpenAiEmbeddingsTaskSettings(randomAlphaOfLength(15));
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
        var user = randomBoolean() ? randomAlphaOfLength(15) : null;
        return new AzureOpenAiEmbeddingsTaskSettings(user);
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        AzureOpenAiEmbeddingsTaskSettings updatedSettings = (AzureOpenAiEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
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
            new AzureOpenAiEmbeddingsTaskSettings("user"),
            AzureOpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(USER, "user")))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(USER, "")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = AzureOpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = AzureOpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(USER, "user")));

        var overriddenTaskSettings = AzureOpenAiEmbeddingsTaskSettings.of(
            taskSettings,
            AzureOpenAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS
        );
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = AzureOpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(USER, "user")));

        var requestTaskSettings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(USER, "user2")));

        var overriddenTaskSettings = AzureOpenAiEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureOpenAiEmbeddingsTaskSettings("user2")));
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
        return randomValueOtherThan(instance, AzureOpenAiEmbeddingsTaskSettingsTests::createRandomWithUser);
    }

    public static Map<String, Object> getAzureOpenAiRequestTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(USER, user);
        }

        return map;
    }
}
